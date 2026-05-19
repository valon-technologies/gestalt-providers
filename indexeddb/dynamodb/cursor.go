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
	cursorutil.LazyCursor
	provider  *providerCore
	storeName string
	index     *indexDef
	req       gestalt.IndexedDBOpenCursorRequest
	items     []map[string]ddbtypes.AttributeValue
	startKey  map[string]ddbtypes.AttributeValue
	closed    bool
}

var errDynamoCursorFieldMissing = errors.New("dynamodb cursor field missing")

func (c *dynamoCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.LazyCursor.Snapshot
}

func (p *providerCore) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "dynamodb: not configured")
	}
	return p.openCursor(ctx, req)
}

func (p *providerCore) openCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*dynamoCursor, error) {
	cursor := &dynamoCursor{
		LazyCursor: cursorutil.NewLazyCursor(req),
		provider:   p,
		storeName:  req.Store,
		req:        req,
	}
	if cursor.IndexCursor {
		meta, err := p.lookupIndexMeta(req.Store, req.Index)
		if err != nil {
			return nil, err
		}
		cursor.index = meta
	}
	return cursor, nil
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
	return c.LazyCursor.Next(ctx, c.nextEntry)
}

func (c *dynamoCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.ContinueToKey(ctx, key, c.nextEntry)
}

func (c *dynamoCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.Advance(ctx, count, c.nextEntry)
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

func (c *dynamoCursor) nextEntry(ctx context.Context) (*cursorutil.Entry, error) {
	for len(c.items) == 0 {
		if c.closed {
			return nil, nil
		}
		if err := c.loadPage(ctx); err != nil {
			return nil, err
		}
	}
	item := c.items[0]
	c.items = c.items[1:]
	return c.entryFromItem(item)
}

func (c *dynamoCursor) loadPage(ctx context.Context) error {
	input := c.queryInput()
	resp, err := c.provider.store.client.Query(ctx, input)
	if err != nil {
		return wrapErr(err)
	}
	c.items = resp.Items
	c.startKey = resp.LastEvaluatedKey
	c.closed = resp.LastEvaluatedKey == nil
	return nil
}

func (c *dynamoCursor) queryInput() *dynamodb.QueryInput {
	input := &dynamodb.QueryInput{
		TableName:         &c.provider.store.table,
		ExclusiveStartKey: c.startKey,
		Limit:             aws.Int32(100),
		ScanIndexForward:  aws.Bool(!c.Reverse),
	}
	if c.IndexCursor {
		cond, vals := buildIndexCondition(c.req.Store, c.req.Index, c.req.Values)
		input.KeyConditionExpression = aws.String(cond)
		input.ExpressionAttributeValues = vals
		input.ExpressionAttributeNames = map[string]string{
			"#idx_key": attrKey,
		}
		input.ProjectionExpression = aws.String(attrSK + ",#idx_key," + attrRefID)
		if !c.KeysOnly {
			input.ExpressionAttributeNames["#data"] = attrData
			input.ProjectionExpression = aws.String(attrSK + ",#idx_key," + attrRefID + ",#data")
		}
		return input
	}

	cond, vals := buildKeyCondition(c.req.Store, c.req.Range)
	input.KeyConditionExpression = &cond
	input.ExpressionAttributeValues = vals
	if c.KeysOnly {
		input.ProjectionExpression = aws.String(attrSK)
	}
	return input
}

func (c *dynamoCursor) entryFromItem(item map[string]ddbtypes.AttributeValue) (*cursorutil.Entry, error) {
	if c.IndexCursor {
		key, err := dynamoIndexKeyFromItem(item, len(c.index.KeyPath))
		if err != nil {
			return nil, err
		}
		primaryKey := getS(item, attrRefID)
		entry := &cursorutil.Entry{
			Key:             key,
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
		return entry, nil
	}

	primaryKey := getS(item, attrSK)
	entry := &cursorutil.Entry{
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
	return entry, nil
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
