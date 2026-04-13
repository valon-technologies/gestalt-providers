package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

type mongoCursor struct {
	provider    *Provider
	storeName   string
	index       *indexMeta
	indexCursor bool
	keysOnly    bool
	reverse     bool
	unique      bool
	entries     []mongoCursorEntry
	pos         int
}

type mongoCursorEntry struct {
	key             any
	primaryKey      string
	primaryKeyValue any
	record          *proto.Record
}

var errMongoCursorFieldMissing = errors.New("mongodb cursor field missing")

func (p *Provider) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
	if _, err := p.configured(); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	openReq := first.GetOpen()
	if openReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be OpenCursorRequest")
	}

	cursor, err := p.openCursorSnapshot(stream.Context(), openReq)
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
				if err := stream.Send(mongoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(mongoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_ContinueToKey:
			target, err := mongoCursorTargetToAny(v.ContinueToKey.GetKey(), cursor.indexCursor)
			if err != nil {
				return err
			}
			entry, ok, err := cursor.continueToKey(target)
			if err != nil {
				return err
			}
			if !ok {
				if err := stream.Send(mongoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(mongoEntryResponse(entry)); err != nil {
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
				if err := stream.Send(mongoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(mongoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Delete:
			if err := cursor.deleteCurrent(stream.Context()); err != nil {
				return err
			}
			if err := stream.Send(mongoDoneResponse(false)); err != nil {
				return err
			}

		case *proto.CursorCommand_Update:
			entry, err := cursor.updateCurrent(stream.Context(), v.Update)
			if err != nil {
				return err
			}
			if err := stream.Send(mongoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Close:
			return nil

		default:
			return status.Error(codes.InvalidArgument, "unknown cursor command")
		}
	}
}

func (p *Provider) openCursorSnapshot(ctx context.Context, req *proto.OpenCursorRequest) (*mongoCursor, error) {
	cursor := &mongoCursor{
		provider:    p,
		storeName:   req.GetStore(),
		indexCursor: req.GetIndex() != "",
		keysOnly:    req.GetKeysOnly(),
		reverse: req.GetDirection() == proto.CursorDirection_CURSOR_PREV ||
			req.GetDirection() == proto.CursorDirection_CURSOR_PREV_UNIQUE,
		unique: req.GetDirection() == proto.CursorDirection_CURSOR_NEXT_UNIQUE ||
			req.GetDirection() == proto.CursorDirection_CURSOR_PREV_UNIQUE,
		pos: -1,
	}

	if cursor.indexCursor {
		meta, err := p.lookupIndexMeta(req.GetStore(), req.GetIndex())
		if err != nil {
			return nil, err
		}
		cursor.index = meta
	}

	records, err := p.cursorRecords(ctx, cursor, req)
	if err != nil {
		return nil, err
	}

	entries := make([]mongoCursorEntry, 0, len(records))
	for _, record := range records {
		entry, err := cursor.entryFromRecord(record)
		if err != nil {
			if cursor.indexCursor && errors.Is(err, errMongoCursorFieldMissing) {
				continue
			}
			return nil, err
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := mongoCompareCursorValue(entries[i].key, entries[j].key)
		if cmp == 0 {
			cmp = mongoCompareCursorValue(entries[i].primaryKeyValue, entries[j].primaryKeyValue)
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

func (p *Provider) lookupIndexMeta(storeName, indexName string) (*indexMeta, error) {
	store, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	store.mu.RLock()
	defer store.mu.RUnlock()

	indexes, ok := store.schemas[storeName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "object store %q has no registered schema", storeName)
	}
	meta, ok := indexes[indexName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %q not found on store %q", indexName, storeName)
	}
	metaCopy := meta
	return &metaCopy, nil
}

func (p *Provider) cursorRecords(ctx context.Context, cursor *mongoCursor, req *proto.OpenCursorRequest) ([]*proto.Record, error) {
	docs, err := p.cursorDocuments(ctx, cursor, req)
	if err != nil {
		return nil, err
	}

	records := make([]*proto.Record, 0, len(docs))
	for _, doc := range docs {
		record, err := docToProto(doc)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal cursor record: %v", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (p *Provider) cursorDocuments(ctx context.Context, cursor *mongoCursor, req *proto.OpenCursorRequest) ([]bson.M, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var filter bson.M
	if cursor.indexCursor {
		filter, err = s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
		if err != nil {
			return nil, err
		}
	} else {
		filter, err = keyRangeFilter(req.GetRange())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
		}
	}

	var mongoCursorDocs *mongo.Cursor
	projection := mongoCursorProjection(cursor)
	if projection == nil {
		mongoCursorDocs, err = s.db.Collection(req.GetStore()).Find(ctx, filter)
	} else {
		mongoCursorDocs, err = s.db.Collection(req.GetStore()).Find(ctx, filter, options.Find().SetProjection(projection))
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open cursor snapshot: %v", err)
	}
	defer mongoCursorDocs.Close(ctx)

	var docs []bson.M
	for mongoCursorDocs.Next(ctx) {
		var doc bson.M
		if err := mongoCursorDocs.Decode(&doc); err != nil {
			return nil, status.Errorf(codes.Internal, "decode cursor snapshot: %v", err)
		}
		docs = append(docs, doc)
	}
	if err := mongoCursorDocs.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "cursor snapshot: %v", err)
	}
	return docs, nil
}

func mongoCursorProjection(cursor *mongoCursor) bson.M {
	if !cursor.keysOnly {
		return nil
	}

	projection := bson.M{"_id": 1}
	if cursor.indexCursor && cursor.index != nil {
		for _, field := range cursor.index.keyPath {
			projection[field] = 1
		}
	}
	return projection
}

func (c *mongoCursor) entryFromRecord(record *proto.Record) (mongoCursorEntry, error) {
	primaryKeyValue, primaryKey, err := mongoExtractID(record)
	if err != nil {
		return mongoCursorEntry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}

	key := primaryKeyValue
	if c.indexCursor {
		parts := make([]any, len(c.index.keyPath))
		for i, field := range c.index.keyPath {
			value, err := mongoRecordFieldAny(record, field)
			if err != nil {
				if errors.Is(err, errMongoCursorFieldMissing) {
					return mongoCursorEntry{}, err
				}
				return mongoCursorEntry{}, status.Errorf(codes.InvalidArgument, "record index field %q: %v", field, err)
			}
			parts[i] = value
		}
		key = parts
	}

	return mongoCursorEntry{
		key:             key,
		primaryKey:      primaryKey,
		primaryKeyValue: primaryKeyValue,
		record:          record,
	}, nil
}

func (c *mongoCursor) applyRange(entries []mongoCursorEntry, kr *proto.KeyRange) ([]mongoCursorEntry, error) {
	if kr == nil {
		return entries, nil
	}

	lower, upper, err := mongoCursorRangeBounds(kr, c.indexCursor)
	if err != nil {
		return nil, err
	}

	filtered := make([]mongoCursorEntry, 0, len(entries))
	for _, entry := range entries {
		if lower != nil {
			cmp := mongoCompareCursorValue(entry.key, lower)
			if kr.GetLowerOpen() && cmp <= 0 {
				continue
			}
			if !kr.GetLowerOpen() && cmp < 0 {
				continue
			}
		}
		if upper != nil {
			cmp := mongoCompareCursorValue(entry.key, upper)
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

func (c *mongoCursor) continueNext() (*proto.CursorEntry, bool, error) {
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev := c.entries[c.pos].key
		for c.pos++; c.pos < len(c.entries); c.pos++ {
			if mongoCompareCursorValue(c.entries[c.pos].key, prev) != 0 {
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

func (c *mongoCursor) continueToKey(target any) (*proto.CursorEntry, bool, error) {
	var prev any
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev = c.entries[c.pos].key
	}
	for c.pos++; c.pos < len(c.entries); c.pos++ {
		cur := c.entries[c.pos].key
		if prev != nil && c.unique && c.indexCursor && mongoCompareCursorValue(cur, prev) == 0 {
			continue
		}
		cmp := mongoCompareCursorValue(cur, target)
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

func (c *mongoCursor) advance(count int) (*proto.CursorEntry, bool, error) {
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

func (c *mongoCursor) current() (*mongoCursorEntry, error) {
	if c.pos < 0 || c.pos >= len(c.entries) {
		return nil, status.Error(codes.NotFound, "cursor is exhausted")
	}
	return &c.entries[c.pos], nil
}

func (c *mongoCursor) currentEntry() (*proto.CursorEntry, error) {
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

func (c *mongoCursor) deleteCurrent(ctx context.Context) error {
	entry, err := c.current()
	if err != nil {
		return err
	}
	return c.provider.deleteByIDValue(ctx, c.storeName, entry.primaryKeyValue)
}

func (c *mongoCursor) updateCurrent(ctx context.Context, record *proto.Record) (*proto.CursorEntry, error) {
	entry, err := c.current()
	if err != nil {
		return nil, err
	}
	cloned, err := c.prepareUpdatedRecord(record, entry.primaryKeyValue)
	if err != nil {
		return nil, err
	}

	if _, err := c.provider.Put(ctx, &proto.RecordRequest{
		Store:  c.storeName,
		Record: cloned,
	}); err != nil {
		return nil, err
	}

	// Preserve the cursor's existing key/range ordering after in-place updates.
	c.entries[c.pos].record = cloned
	return c.currentEntry()
}

func (c *mongoCursor) prepareUpdatedRecord(record *proto.Record, primaryKey any) (*proto.Record, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "update record is required")
	}

	cloned := gproto.Clone(record).(*proto.Record)
	if cloned.Fields == nil {
		cloned.Fields = map[string]*proto.TypedValue{}
	}
	idValue, err := gestalt.TypedValueFromAny(primaryKey)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal primary key: %v", err)
	}
	cloned.Fields["id"] = idValue
	return cloned, nil
}

func mongoCursorTargetToAny(kvs []*proto.KeyValue, indexCursor bool) (any, error) {
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

func mongoCursorRangeBounds(kr *proto.KeyRange, indexCursor bool) (any, any, error) {
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

func mongoExtractID(record *proto.Record) (any, string, error) {
	if record == nil {
		return nil, "", status.Error(codes.InvalidArgument, "record is required")
	}
	value, ok := record.Fields["id"]
	if !ok || value == nil {
		return nil, "", status.Error(codes.InvalidArgument, "record must contain an \"id\" field")
	}
	goValue, err := gestalt.AnyFromTypedValue(value)
	if err != nil {
		return nil, "", err
	}
	if goValue == nil {
		return nil, "", status.Error(codes.InvalidArgument, "record \"id\" must be non-null")
	}
	id := fmt.Sprint(goValue)
	if id == "" {
		return nil, "", status.Error(codes.InvalidArgument, "record \"id\" must be non-empty")
	}
	return goValue, id, nil
}

func mongoRecordFieldAny(record *proto.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}

	fields, err := gestalt.RecordFromProto(record)
	if err != nil {
		return nil, err
	}

	value, ok := mongoLookupField(fields, strings.Split(field, "."))
	if !ok {
		return nil, fmt.Errorf("%w: field %q", errMongoCursorFieldMissing, field)
	}
	return value, nil
}

func mongoLookupField(value any, path []string) (any, bool) {
	current := value
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func mongoEntryResponse(entry *proto.CursorEntry) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Entry{Entry: entry}}
}

func mongoDoneResponse(done bool) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Done{Done: done}}
}

func mongoCompareCursorValue(a, b any) int {
	switch av := a.(type) {
	case []any:
		if bv, ok := b.([]any); ok {
			for i := range av {
				if i >= len(bv) {
					return 1
				}
				if cmp := mongoCompareCursorValue(av[i], bv[i]); cmp != 0 {
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

	if af, ok := mongoCursorNumber(a); ok {
		if bf, ok := mongoCursorNumber(b); ok {
			return af.Cmp(bf)
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

func mongoCursorNumber(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case int:
		return big.NewRat(int64(n), 1), true
	case int8:
		return big.NewRat(int64(n), 1), true
	case int16:
		return big.NewRat(int64(n), 1), true
	case int32:
		return big.NewRat(int64(n), 1), true
	case int64:
		return big.NewRat(n, 1), true
	case float32:
		return mongoCursorFloatRat(float64(n))
	case float64:
		return mongoCursorFloatRat(n)
	default:
		return nil, false
	}
}

func mongoCursorFloatRat(v float64) (*big.Rat, bool) {
	r := new(big.Rat).SetFloat64(v)
	if r == nil {
		return nil, false
	}
	return r, true
}

func mongoMapCursorWriteErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if err == mongo.ErrNoDocuments {
		return status.Error(codes.NotFound, "not found")
	}
	if mongo.IsDuplicateKeyError(err) {
		return status.Error(codes.AlreadyExists, "already exists")
	}
	return status.Errorf(codes.Internal, "%s: %v", op, err)
}
