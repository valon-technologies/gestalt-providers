package dynamodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

type dynamoCursor struct {
	provider    *Provider
	storeName   string
	index       *indexDef
	indexCursor bool
	keysOnly    bool
	reverse     bool
	unique      bool
	entries     []dynamoCursorEntry
	pos         int
}

type dynamoCursorEntry struct {
	key        any
	primaryKey string
	record     *proto.Record
}

var errDynamoCursorFieldMissing = errors.New("dynamodb cursor field missing")

func (p *Provider) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
	if p.store == nil {
		return status.Error(codes.FailedPrecondition, "dynamodb: not configured")
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
				if err := stream.Send(dynamoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(dynamoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_ContinueToKey:
			target, err := dynamoCursorTargetToAny(v.ContinueToKey.GetKey(), cursor.indexCursor)
			if err != nil {
				return err
			}
			entry, ok, err := cursor.continueToKey(target)
			if err != nil {
				return err
			}
			if !ok {
				if err := stream.Send(dynamoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(dynamoEntryResponse(entry)); err != nil {
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
				if err := stream.Send(dynamoDoneResponse(true)); err != nil {
					return err
				}
				continue
			}
			if err := stream.Send(dynamoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Delete:
			if err := cursor.deleteCurrent(stream.Context()); err != nil {
				return err
			}
			if err := stream.Send(dynamoDoneResponse(false)); err != nil {
				return err
			}

		case *proto.CursorCommand_Update:
			entry, err := cursor.updateCurrent(stream.Context(), v.Update)
			if err != nil {
				return err
			}
			if err := stream.Send(dynamoEntryResponse(entry)); err != nil {
				return err
			}

		case *proto.CursorCommand_Close:
			return nil

		default:
			return status.Error(codes.InvalidArgument, "unknown cursor command")
		}
	}
}

func (p *Provider) openCursorSnapshot(ctx context.Context, req *proto.OpenCursorRequest) (*dynamoCursor, error) {
	cursor := &dynamoCursor{
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

	var entries []dynamoCursorEntry
	var err error
	if cursor.keysOnly {
		entries, err = p.cursorKeys(ctx, cursor, req)
		if err != nil {
			return nil, err
		}
	} else {
		records, err := p.cursorRecords(ctx, req)
		if err != nil {
			return nil, err
		}

		entries = make([]dynamoCursorEntry, 0, len(records))
		for _, record := range records {
			entry, err := cursor.entryFromRecord(record)
			if err != nil {
				if cursor.indexCursor && errors.Is(err, errDynamoCursorFieldMissing) {
					continue
				}
				return nil, err
			}
			entries = append(entries, entry)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := dynamoCompareCursorValue(entries[i].key, entries[j].key)
		if cmp == 0 {
			cmp = dynamoCompareCursorValue(entries[i].primaryKey, entries[j].primaryKey)
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

func (p *Provider) cursorKeys(ctx context.Context, cursor *dynamoCursor, req *proto.OpenCursorRequest) ([]dynamoCursorEntry, error) {
	if cursor.indexCursor {
		return p.cursorIndexKeys(ctx, cursor, req)
	}
	return p.cursorObjectStoreKeys(ctx, req)
}

func (p *Provider) cursorObjectStoreKeys(ctx context.Context, req *proto.OpenCursorRequest) ([]dynamoCursorEntry, error) {
	cond, vals := buildKeyCondition(req.GetStore(), req.GetRange())
	var entries []dynamoCursorEntry
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := p.store.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &p.store.table,
			KeyConditionExpression:    &cond,
			ExpressionAttributeValues: vals,
			ExclusiveStartKey:         startKey,
			ProjectionExpression:      aws.String(attrSK),
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			primaryKey := getS(item, attrSK)
			entries = append(entries, dynamoCursorEntry{
				key:        primaryKey,
				primaryKey: primaryKey,
			})
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return entries, nil
}

func (p *Provider) cursorIndexKeys(ctx context.Context, cursor *dynamoCursor, req *proto.OpenCursorRequest) ([]dynamoCursorEntry, error) {
	cond, exprVals := buildIndexCondition(req.GetStore(), req.GetIndex(), req.GetValues())
	var entries []dynamoCursorEntry
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := p.store.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &p.store.table,
			KeyConditionExpression:    aws.String(cond),
			ExpressionAttributeValues: exprVals,
			ExpressionAttributeNames: map[string]string{
				"#idx_key": attrKey,
			},
			ExclusiveStartKey:    startKey,
			ProjectionExpression: aws.String(attrSK + ",#idx_key," + attrRefID),
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			key, err := dynamoIndexKeyFromItem(item, len(cursor.index.KeyPath))
			if err != nil {
				return nil, err
			}
			entries = append(entries, dynamoCursorEntry{
				key:        key,
				primaryKey: getS(item, attrRefID),
			})
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return entries, nil
}

func dynamoIndexKeyFromItem(item map[string]ddbtypes.AttributeValue, keyParts int) ([]any, error) {
	if raw, ok := item[attrKey].(*ddbtypes.AttributeValueMemberB); ok && len(raw.Value) > 0 {
		return unmarshalIndexKey(raw.Value, keyParts)
	}

	sk := getS(item, attrSK)
	parts := strings.Split(sk, sep)
	if len(parts) < keyParts+1 {
		return nil, status.Errorf(codes.Internal, "invalid index sort key %q", sk)
	}

	key := make([]any, keyParts)
	for i := 0; i < keyParts; i++ {
		key[i] = parts[i]
	}
	return key, nil
}

func (p *Provider) lookupIndexMeta(storeName, indexName string) (*indexDef, error) {
	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "dynamodb: not configured")
	}
	schema, ok := p.store.getSchema(storeName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "object store %q has no registered schema", storeName)
	}
	for _, idx := range schema.Indexes {
		if idx.Name == indexName {
			idxCopy := idx
			return &idxCopy, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "index %q not found on store %q", indexName, storeName)
}

func (p *Provider) cursorRecords(ctx context.Context, req *proto.OpenCursorRequest) ([]*proto.Record, error) {
	if req.GetIndex() == "" {
		resp, err := p.GetAll(ctx, &proto.ObjectStoreRangeRequest{Store: req.GetStore()})
		if err != nil {
			return nil, err
		}
		return resp.GetRecords(), nil
	}

	resp, err := p.IndexGetAll(ctx, &proto.IndexQueryRequest{
		Store:  req.GetStore(),
		Index:  req.GetIndex(),
		Values: req.GetValues(),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRecords(), nil
}

func (c *dynamoCursor) entryFromRecord(record *proto.Record) (dynamoCursorEntry, error) {
	primaryKey, err := extractID(record)
	if err != nil {
		return dynamoCursorEntry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}

	key := any(primaryKey)
	if c.indexCursor {
		parts := make([]any, len(c.index.KeyPath))
		for i, field := range c.index.KeyPath {
			value, err := dynamoRecordFieldAny(record, field)
			if err != nil {
				if errors.Is(err, errDynamoCursorFieldMissing) {
					return dynamoCursorEntry{}, err
				}
				return dynamoCursorEntry{}, status.Errorf(codes.InvalidArgument, "record index field %q: %v", field, err)
			}
			parts[i] = value
		}
		key = parts
	}

	return dynamoCursorEntry{
		key:        key,
		primaryKey: primaryKey,
		record:     record,
	}, nil
}

func (c *dynamoCursor) applyRange(entries []dynamoCursorEntry, kr *proto.KeyRange) ([]dynamoCursorEntry, error) {
	if kr == nil {
		return entries, nil
	}

	lower, upper, err := dynamoCursorRangeBounds(kr, c.indexCursor)
	if err != nil {
		return nil, err
	}

	filtered := make([]dynamoCursorEntry, 0, len(entries))
	for _, entry := range entries {
		if lower != nil {
			cmp := dynamoCompareCursorValue(entry.key, lower)
			if kr.GetLowerOpen() && cmp <= 0 {
				continue
			}
			if !kr.GetLowerOpen() && cmp < 0 {
				continue
			}
		}
		if upper != nil {
			cmp := dynamoCompareCursorValue(entry.key, upper)
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

func (c *dynamoCursor) continueNext() (*proto.CursorEntry, bool, error) {
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev := c.entries[c.pos].key
		for c.pos++; c.pos < len(c.entries); c.pos++ {
			if dynamoCompareCursorValue(c.entries[c.pos].key, prev) != 0 {
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

func (c *dynamoCursor) continueToKey(target any) (*proto.CursorEntry, bool, error) {
	var prev any
	if c.unique && c.indexCursor && c.pos >= 0 && c.pos < len(c.entries) {
		prev = c.entries[c.pos].key
	}
	for c.pos++; c.pos < len(c.entries); c.pos++ {
		cur := c.entries[c.pos].key
		if prev != nil && c.unique && c.indexCursor && dynamoCompareCursorValue(cur, prev) == 0 {
			continue
		}
		cmp := dynamoCompareCursorValue(cur, target)
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

func (c *dynamoCursor) advance(count int) (*proto.CursorEntry, bool, error) {
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

func (c *dynamoCursor) current() (*dynamoCursorEntry, error) {
	if c.pos < 0 || c.pos >= len(c.entries) {
		return nil, status.Error(codes.NotFound, "cursor is exhausted")
	}
	return &c.entries[c.pos], nil
}

func (c *dynamoCursor) currentEntry() (*proto.CursorEntry, error) {
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

func (c *dynamoCursor) deleteCurrent(ctx context.Context) error {
	entry, err := c.current()
	if err != nil {
		return err
	}
	_, err = c.provider.Delete(ctx, &proto.ObjectStoreRequest{
		Store: c.storeName,
		Id:    entry.primaryKey,
	})
	return err
}

func (c *dynamoCursor) updateCurrent(ctx context.Context, record *proto.Record) (*proto.CursorEntry, error) {
	entry, err := c.current()
	if err != nil {
		return nil, err
	}
	cloned, err := c.prepareUpdatedRecord(record, entry.primaryKey)
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

func (c *dynamoCursor) prepareUpdatedRecord(record *proto.Record, primaryKey string) (*proto.Record, error) {
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

func dynamoCursorTargetToAny(kvs []*proto.KeyValue, indexCursor bool) (any, error) {
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

func dynamoCursorRangeBounds(kr *proto.KeyRange, indexCursor bool) (any, any, error) {
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

func dynamoRecordFieldAny(record *proto.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	value, ok := record.Fields[field]
	if !ok {
		return nil, fmt.Errorf("%w: field %q", errDynamoCursorFieldMissing, field)
	}
	return gestalt.AnyFromTypedValue(value)
}

func dynamoEntryResponse(entry *proto.CursorEntry) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Entry{Entry: entry}}
}

func dynamoDoneResponse(done bool) *proto.CursorResponse {
	return &proto.CursorResponse{Result: &proto.CursorResponse_Done{Done: done}}
}

func dynamoCompareCursorValue(a, b any) int {
	switch av := a.(type) {
	case []any:
		if bv, ok := b.([]any); ok {
			for i := range av {
				if i >= len(bv) {
					return 1
				}
				if cmp := dynamoCompareCursorValue(av[i], bv[i]); cmp != 0 {
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

	if af, ok := dynamoCursorNumber(a); ok {
		if bf, ok := dynamoCursorNumber(b); ok {
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

func dynamoCursorNumber(v any) (*big.Rat, bool) {
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
		return dynamoCursorFloatRat(float64(n))
	case float64:
		return dynamoCursorFloatRat(n)
	default:
		return nil, false
	}
}

func dynamoCursorFloatRat(v float64) (*big.Rat, bool) {
	r := new(big.Rat).SetFloat64(v)
	if r == nil {
		return nil, false
	}
	return r, true
}
