package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dynamoCursor struct {
	cursorutil.Snapshot
	provider  *providerCore
	storeName string
	index     *indexDef
	req       gestalt.IndexedDBOpenCursorRequest
	items     []map[string]ddbtypes.AttributeValue
	startKey  map[string]ddbtypes.AttributeValue
	lastKey   any
	lazy      bool
}

var errDynamoCursorFieldMissing = errors.New("dynamodb cursor field missing")

func (c *dynamoCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.Snapshot
}

func (p *providerCore) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "dynamodb: not configured")
	}
	return p.openCursorSnapshot(ctx, req)
}

func (p *providerCore) openCursorSnapshot(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*dynamoCursor, error) {
	cursor := &dynamoCursor{
		Snapshot:  cursorutil.NewSnapshot(req),
		provider:  p,
		storeName: req.Store,
	}
	if cursor.IndexCursor {
		meta, err := p.lookupIndexMeta(req.Store, req.Index)
		if err != nil {
			return nil, err
		}
		cursor.index = meta
	}
	if canUseLazyDynamoObjectCursor(req) {
		cursor.req = req
		cursor.lazy = true
		cursor.startKey = dynamoCursorExclusiveStartKey(req)
		return cursor, nil
	}

	var (
		entries []cursorutil.Entry
		err     error
	)
	if cursor.KeysOnly {
		entries, err = p.cursorKeys(ctx, cursor, req)
	} else {
		records, err := p.cursorRecords(ctx, req)
		if err != nil {
			return nil, err
		}
		entries, err = cursorutil.EntriesFromRecords(records, cursor.entryFromRecord, func(err error) bool {
			return cursor.IndexCursor && errors.Is(err, errDynamoCursorFieldMissing)
		})
	}
	if err != nil {
		return nil, err
	}
	if err := cursor.Load(entries, req.Range); err != nil {
		return nil, err
	}
	return cursor, nil
}

func canUseLazyDynamoObjectCursor(req gestalt.IndexedDBOpenCursorRequest) bool {
	return req.Index == "" && req.Direction != gestalt.CursorPrev && req.Direction != gestalt.CursorPrevUnique
}

func (p *providerCore) cursorKeys(ctx context.Context, cursor *dynamoCursor, req gestalt.IndexedDBOpenCursorRequest) ([]cursorutil.Entry, error) {
	if cursor.IndexCursor {
		return p.cursorIndexKeys(ctx, cursor, req)
	}
	return p.cursorObjectStoreKeys(ctx, req)
}

func (p *providerCore) cursorObjectStoreKeys(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) ([]cursorutil.Entry, error) {
	cond, vals := buildKeyCondition(req.Store, req.Range)
	var entries []cursorutil.Entry
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
			entries = append(entries, cursorutil.Entry{
				Key:             primaryKey,
				PrimaryKey:      primaryKey,
				PrimaryKeyValue: primaryKey,
			})
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return entries, nil
}

func (p *providerCore) cursorIndexKeys(ctx context.Context, cursor *dynamoCursor, req gestalt.IndexedDBOpenCursorRequest) ([]cursorutil.Entry, error) {
	cond, exprVals := buildIndexCondition(req.Store, req.Index, req.Values)
	var entries []cursorutil.Entry
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
			primaryKey := getS(item, attrRefID)
			entries = append(entries, cursorutil.Entry{
				Key:             key,
				PrimaryKey:      primaryKey,
				PrimaryKeyValue: primaryKey,
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

func (p *providerCore) lookupIndexMeta(storeName, indexName string) (*indexDef, error) {
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

func (p *providerCore) cursorRecords(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) ([]gestalt.Record, error) {
	if req.Index == "" {
		return p.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: req.Store})
	}

	return p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  req.Store,
		Index:  req.Index,
		Values: req.Values,
	})
}

func (c *dynamoCursor) entryFromRecord(record gestalt.Record) (cursorutil.Entry, error) {
	primaryKey, err := extractID(record)
	if err != nil {
		return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}

	key := any(primaryKey)
	if c.IndexCursor {
		parts := make([]any, len(c.index.KeyPath))
		for i, field := range c.index.KeyPath {
			value, err := dynamoRecordFieldAny(record, field)
			if err != nil {
				if errors.Is(err, errDynamoCursorFieldMissing) {
					return cursorutil.Entry{}, err
				}
				return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record index field %q: %v", field, err)
			}
			parts[i] = value
		}
		key = parts
	}

	return cursorutil.Entry{
		Key:             key,
		PrimaryKey:      primaryKey,
		PrimaryKeyValue: primaryKey,
		Record:          record,
	}, nil
}

func (c *dynamoCursor) Next(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		return c.nextLazyObject(ctx)
	}
	entry, _, err := c.ContinueNext()
	return entry, err
}

func (c *dynamoCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		c.req.Range = mergeDynamoCursorSeekRange(c.req.Range, c.lastKey, key)
		c.startKey = dynamoCursorExclusiveStartKey(c.req)
		c.items = nil
		return c.nextLazyObject(ctx)
	}
	entry, _, err := c.Snapshot.ContinueToKey(key)
	return entry, err
}

func (c *dynamoCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		if count <= 0 {
			return nil, status.Error(codes.InvalidArgument, "advance count must be positive")
		}
		var entry *gestalt.IndexedDBCursorEntry
		var err error
		for i := 0; i < count; i++ {
			entry, err = c.Next(ctx)
			if entry == nil || err != nil {
				return entry, err
			}
		}
		return entry, nil
	}
	entry, _, err := c.Snapshot.Advance(count)
	return entry, err
}

func (c *dynamoCursor) Delete(ctx context.Context) error {
	return c.DeleteCurrent(ctx)
}

func (c *dynamoCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	return c.UpdateCurrent(ctx, record)
}

func (c *dynamoCursor) Close() error {
	return nil
}

func (c *dynamoCursor) nextLazyObject(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	for len(c.items) == 0 {
		if c.startKey == nil && c.items != nil {
			return nil, nil
		}
		if err := c.loadLazyObjectPage(ctx); err != nil {
			return nil, err
		}
		if len(c.items) == 0 && c.startKey == nil {
			return nil, nil
		}
	}
	item := c.items[0]
	c.items = c.items[1:]
	primaryKey := getS(item, attrSK)
	entry := cursorutil.Entry{
		Key:             primaryKey,
		PrimaryKey:      primaryKey,
		PrimaryKeyValue: primaryKey,
	}
	if !c.KeysOnly {
		record, err := parseData(item)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode cursor stream: %v", err)
		}
		entry.Record = record
	}
	c.lastKey = entry.Key
	c.Entries = []cursorutil.Entry{entry}
	c.Pos = 0
	return c.CurrentEntry()
}

func (c *dynamoCursor) loadLazyObjectPage(ctx context.Context) error {
	cond, vals := buildKeyCondition(c.req.Store, c.req.Range)
	input := &dynamodb.QueryInput{
		TableName:                 &c.provider.store.table,
		KeyConditionExpression:    &cond,
		ExpressionAttributeValues: vals,
		ExclusiveStartKey:         c.startKey,
		Limit:                     aws.Int32(100),
	}
	if c.KeysOnly {
		input.ProjectionExpression = aws.String(attrSK)
	}
	resp, err := c.provider.store.client.Query(ctx, input)
	if err != nil {
		return wrapErr(err)
	}
	c.items = resp.Items
	c.startKey = resp.LastEvaluatedKey
	return nil
}

func dynamoCursorExclusiveStartKey(req gestalt.IndexedDBOpenCursorRequest) map[string]ddbtypes.AttributeValue {
	if req.Range == nil || req.Range.Lower == nil || !req.Range.LowerOpen {
		return nil
	}
	return map[string]ddbtypes.AttributeValue{
		attrPK: &ddbtypes.AttributeValueMemberS{Value: req.Store},
		attrSK: &ddbtypes.AttributeValueMemberS{Value: valueToString(req.Range.Lower)},
	}
}

func mergeDynamoCursorSeekRange(r *gestalt.KeyRange, lastKey, key any) *gestalt.KeyRange {
	lower := key
	lowerOpen := false
	if lastKey != nil && (lower == nil || cursorutil.CompareValues(lower, lastKey) <= 0) {
		lower = lastKey
		lowerOpen = true
	}
	out := &gestalt.KeyRange{Lower: lower, LowerOpen: lowerOpen}
	if r != nil {
		*out = *r
		if lower != nil && (r.Lower == nil || cursorutil.CompareValues(lower, r.Lower) > 0 || (cursorutil.CompareValues(lower, r.Lower) == 0 && lowerOpen && !r.LowerOpen)) {
			out.Lower = lower
			out.LowerOpen = lowerOpen
		}
	}
	return out
}

func (c *dynamoCursor) DeleteCurrent(ctx context.Context) error {
	entry, err := c.Current()
	if err != nil {
		return err
	}
	return c.provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{
		Store: c.storeName,
		ID:    entry.PrimaryKey,
	})
}

func (c *dynamoCursor) UpdateCurrent(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	entry, err := c.Current()
	if err != nil {
		return nil, err
	}
	cloned, err := cursorutil.CloneRecordWithField(record, "id", entry.PrimaryKeyValue)
	if err != nil {
		return nil, err
	}

	if err := c.provider.Put(ctx, gestalt.IndexedDBRecordRequest{
		Store:  c.storeName,
		Record: cloned,
	}); err != nil {
		return nil, err
	}

	c.Entries[c.Pos].Record = cloned
	return c.CurrentEntry()
}

func dynamoRecordFieldAny(record gestalt.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	if _, ok := record[field]; !ok {
		return nil, fmt.Errorf("%w: field %q", errDynamoCursorFieldMissing, field)
	}
	return cursorutil.DirectRecordField(record, field)
}
