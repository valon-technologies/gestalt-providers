package relationaldb

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

type relationalCursor struct {
	store       *Store
	storeName   string
	meta        *storeMeta
	index       *proto.IndexSchema
	indexCursor bool
	keysOnly    bool
	reverse     bool
	unique      bool
	entries     []cursorEntry
	pos         int
}

type cursorEntry struct {
	key             any
	primaryKey      string
	primaryKeyValue any
	record          *proto.Record
}

func (s *Store) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	openReq := first.GetOpen()
	if openReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be OpenCursorRequest")
	}

	cursor, err := s.openCursorSnapshot(stream.Context(), openReq)
	if err != nil {
		return err
	}

	if err := stream.Send(&proto.CursorResponse{
		Result: &proto.CursorResponse_Done{Done: false},
	}); err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		cmd := msg.GetCommand()
		if cmd == nil {
			return status.Error(codes.InvalidArgument, "expected CursorCommand after open")
		}

		switch v := cmd.GetCommand().(type) {
		case *proto.CursorCommand_Next:
			entry, ok, err := cursor.continueNext()
			if err != nil {
				return err
			}
			if !ok {
				if err := stream.Send(doneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(entryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_ContinueToKey:
			target, err := cursorTargetToAny(v.ContinueToKey.GetKey(), cursor.indexCursor)
			if err != nil {
				return err
			}
			entry, ok, err := cursor.continueToKey(target)
			if err != nil {
				return err
			}
			if !ok {
				if err := stream.Send(doneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(entryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Advance:
			if v.Advance <= 0 {
				return status.Error(codes.InvalidArgument, "advance count must be positive")
			}
			entry, ok, err := cursor.advance(int(v.Advance))
			if err != nil {
				return err
			}
			if !ok {
				if err := stream.Send(doneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(entryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Delete:
			if err := cursor.deleteCurrent(stream.Context()); err != nil {
				return err
			}
			if err := stream.Send(doneResponse(false)); err != nil {
				return err
			}

		case *proto.CursorCommand_Update:
			entry, err := cursor.updateCurrent(stream.Context(), v.Update)
			if err != nil {
				return err
			}
			if err := stream.Send(entryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Close:
			return nil

		default:
			return status.Error(codes.InvalidArgument, "unknown cursor command")
		}
	}
}

func (s *Store) openCursorSnapshot(ctx context.Context, req *proto.OpenCursorRequest) (*relationalCursor, error) {
	meta, err := s.getMeta(req.GetStore())
	if err != nil {
		return nil, err
	}

	cursor := &relationalCursor{
		store:       s,
		storeName:   req.GetStore(),
		meta:        meta,
		indexCursor: req.GetIndex() != "",
		keysOnly:    req.GetKeysOnly(),
		reverse: req.GetDirection() == proto.CursorDirection_CURSOR_PREV ||
			req.GetDirection() == proto.CursorDirection_CURSOR_PREV_UNIQUE,
		unique: req.GetDirection() == proto.CursorDirection_CURSOR_NEXT_UNIQUE ||
			req.GetDirection() == proto.CursorDirection_CURSOR_PREV_UNIQUE,
		pos: -1,
	}

	records, err := s.cursorRecords(ctx, req)
	if err != nil {
		return nil, err
	}
	if cursor.indexCursor {
		cursor.index = findIndex(meta, req.GetIndex())
		if cursor.index == nil {
			return nil, status.Errorf(codes.NotFound, "index not found: %s", req.GetIndex())
		}
	}

	entries := make([]cursorEntry, 0, len(records))
	for _, record := range records {
		entry, err := cursor.entryFromRecord(record)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := compareCursorValue(entries[i].key, entries[j].key)
		if cmp == 0 {
			cmp = compareCursorValue(entries[i].primaryKeyValue, entries[j].primaryKeyValue)
		}
		if cursor.reverse {
			return cmp > 0
		}
		return cmp < 0
	})

	entries, err = cursor.applyRange(entries, req.GetRange())
	if err != nil {
		return nil, err
	}
	cursor.entries = entries
	return cursor, nil
}

func (s *Store) cursorRecords(ctx context.Context, req *proto.OpenCursorRequest) ([]*proto.Record, error) {
	if req.GetIndex() == "" {
		resp, err := s.GetAll(ctx, &proto.ObjectStoreRangeRequest{Store: req.GetStore()})
		if err != nil {
			return nil, err
		}
		return resp.GetRecords(), nil
	}

	resp, err := s.IndexGetAll(ctx, &proto.IndexQueryRequest{
		Store:  req.GetStore(),
		Index:  req.GetIndex(),
		Values: req.GetValues(),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRecords(), nil
}

func (c *relationalCursor) entryFromRecord(record *proto.Record) (cursorEntry, error) {
	primaryKeyValue, err := recordFieldAny(record, c.meta.pkCol)
	if err != nil {
		return cursorEntry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}
	primaryKey := fmt.Sprint(primaryKeyValue)

	key := primaryKeyValue
	if c.indexCursor {
		parts := make([]any, len(c.index.GetKeyPath()))
		for i, field := range c.index.GetKeyPath() {
			value, err := recordFieldAny(record, field)
			if err != nil {
				return cursorEntry{}, status.Errorf(codes.InvalidArgument, "record index field %q: %v", field, err)
			}
			parts[i] = value
		}
		key = parts
	}

	return cursorEntry{
		key:             key,
		primaryKey:      primaryKey,
		primaryKeyValue: primaryKeyValue,
		record:          record,
	}, nil
}

func (c *relationalCursor) applyRange(entries []cursorEntry, kr *proto.KeyRange) ([]cursorEntry, error) {
	if kr == nil {
		return entries, nil
	}

	lower, upper, err := cursorRangeBounds(kr, c.indexCursor)
	if err != nil {
		return nil, err
	}

	filtered := make([]cursorEntry, 0, len(entries))
	for _, entry := range entries {
		if lower != nil {
			cmp := compareCursorValue(entry.key, lower)
			if kr.GetLowerOpen() && cmp <= 0 {
				continue
			}
			if !kr.GetLowerOpen() && cmp < 0 {
				continue
			}
		}
		if upper != nil {
			cmp := compareCursorValue(entry.key, upper)
			if kr.GetUpperOpen() && cmp >= 0 {
				continue
			}
			if !kr.GetUpperOpen() && cmp > 0 {
				continue
			}
		}
		filtered = append(filtered, entry)
	}
	return filtered, nil
}

func (c *relationalCursor) continueNext() (*proto.CursorEntry, bool, error) {
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev := c.entries[c.pos].key
		for c.pos++; c.pos < len(c.entries); c.pos++ {
			if compareCursorValue(c.entries[c.pos].key, prev) != 0 {
				entry, err := c.currentEntry()
				return entry, err == nil, err
			}
		}
		return nil, false, nil
	}

	c.pos++
	if c.pos >= len(c.entries) {
		return nil, false, nil
	}
	entry, err := c.currentEntry()
	return entry, err == nil, err
}

func (c *relationalCursor) continueToKey(target any) (*proto.CursorEntry, bool, error) {
	var prev any
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev = c.entries[c.pos].key
	}
	for c.pos++; c.pos < len(c.entries); c.pos++ {
		cur := c.entries[c.pos].key
		if prev != nil && c.unique && c.indexCursor && compareCursorValue(cur, prev) == 0 {
			continue
		}
		cmp := compareCursorValue(cur, target)
		if c.reverse {
			if cmp <= 0 {
				entry, err := c.currentEntry()
				return entry, err == nil, err
			}
			continue
		}
		if cmp >= 0 {
			entry, err := c.currentEntry()
			return entry, err == nil, err
		}
	}
	return nil, false, nil
}

func (c *relationalCursor) advance(count int) (*proto.CursorEntry, bool, error) {
	if count <= 0 {
		return nil, false, status.Error(codes.InvalidArgument, "advance count must be positive")
	}
	for i := 0; i <= count; i++ {
		entry, ok, err := c.continueNext()
		if !ok || err != nil {
			return entry, ok, err
		}
		if i == count {
			return entry, true, nil
		}
	}
	return nil, false, nil
}

func (c *relationalCursor) current() (*cursorEntry, error) {
	if c.pos < 0 || c.pos >= len(c.entries) {
		return nil, status.Error(codes.NotFound, "cursor is exhausted")
	}
	return &c.entries[c.pos], nil
}

func (c *relationalCursor) currentEntry() (*proto.CursorEntry, error) {
	entry, err := c.current()
	if err != nil {
		return nil, err
	}
	out := &proto.CursorEntry{PrimaryKey: entry.primaryKey}
	switch key := entry.key.(type) {
	case []any:
		out.Key = make([]*proto.KeyValue, len(key))
		for i, part := range key {
			kv, err := gestalt.AnyToKeyValue(part)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "marshal cursor key[%d]: %v", i, err)
			}
			out.Key[i] = kv
		}
	default:
		kv, err := gestalt.AnyToKeyValue(key)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "marshal cursor key: %v", err)
		}
		out.Key = []*proto.KeyValue{kv}
	}

	if !c.keysOnly {
		out.Record = entry.record
	}
	return out, nil
}

func (c *relationalCursor) deleteCurrent(ctx context.Context) error {
	entry, err := c.current()
	if err != nil {
		return err
	}
	return c.store.deleteByPrimaryKeyValue(ctx, c.meta, entry.primaryKeyValue)
}

func (c *relationalCursor) updateCurrent(ctx context.Context, record *proto.Record) (*proto.CursorEntry, error) {
	entry, err := c.current()
	if err != nil {
		return nil, err
	}
	cloned, err := c.prepareUpdatedRecord(record, entry.primaryKeyValue)
	if err != nil {
		return nil, err
	}

	if _, err := c.store.Put(ctx, &proto.RecordRequest{
		Store:  c.storeName,
		Record: cloned,
	}); err != nil {
		return nil, err
	}

	// Preserve the cursor's existing key/range ordering after in-place updates.
	c.entries[c.pos].record = cloned
	return c.currentEntry()
}

func (c *relationalCursor) prepareUpdatedRecord(record *proto.Record, primaryKeyValue any) (*proto.Record, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "update record is required")
	}

	cloned := gproto.Clone(record).(*proto.Record)
	if cloned.Fields == nil {
		cloned.Fields = map[string]*proto.TypedValue{}
	}
	keyValue, err := gestalt.TypedValueFromAny(primaryKeyValue)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal primary key: %v", err)
	}
	cloned.Fields[c.meta.pkCol] = keyValue
	return cloned, nil
}

func cursorTargetToAny(kvs []*proto.KeyValue, indexCursor bool) (any, error) {
	if len(kvs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "continue key is required")
	}
	parts, err := gestalt.KeyValuesToAny(kvs)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal continue key: %v", err)
	}
	if indexCursor {
		return parts, nil
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return parts, nil
}

func cursorRangeBounds(kr *proto.KeyRange, indexCursor bool) (any, any, error) {
	var lower any
	if kr.GetLower() != nil {
		value, err := gestalt.AnyFromTypedValue(kr.GetLower())
		if err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "key range lower: %v", err)
		}
		if value != nil {
			if indexCursor {
				if parts, ok := value.([]any); ok {
					lower = parts
				} else {
					lower = []any{value}
				}
			} else {
				lower = value
			}
		}
	}

	var upper any
	if kr.GetUpper() != nil {
		value, err := gestalt.AnyFromTypedValue(kr.GetUpper())
		if err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "key range upper: %v", err)
		}
		if value != nil {
			if indexCursor {
				if parts, ok := value.([]any); ok {
					upper = parts
				} else {
					upper = []any{value}
				}
			} else {
				upper = value
			}
		}
	}
	return lower, upper, nil
}

func recordFieldAny(record *proto.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	value, ok := record.Fields[field]
	if !ok {
		return nil, fmt.Errorf("field %q not found", field)
	}
	return gestalt.AnyFromTypedValue(value)
}

func entryResponse(entry *proto.CursorEntry) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Entry{Entry: entry}}
}

func doneResponse(done bool) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Done{Done: done}}
}

func compareCursorValue(a, b any) int {
	switch av := a.(type) {
	case []any:
		if bv, ok := b.([]any); ok {
			for i := range av {
				if i >= len(bv) {
					return 1
				}
				if cmp := compareCursorValue(av[i], bv[i]); cmp != 0 {
					return cmp
				}
			}
			if len(av) < len(bv) {
				return -1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			default:
				return 0
			}
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			switch {
			case av.Before(bv):
				return -1
			case av.After(bv):
				return 1
			default:
				return 0
			}
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Compare(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			switch {
			case !av && bv:
				return -1
			case av && !bv:
				return 1
			default:
				return 0
			}
		}
	}

	if af, ok := cursorNumber(a); ok {
		if bf, ok := cursorNumber(b); ok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}

	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func cursorNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
