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
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dynamoCursor struct {
	cursorutil.Snapshot
	provider  *Provider
	storeName string
	index     *indexDef
}

var errDynamoCursorFieldMissing = errors.New("dynamodb cursor field missing")

func (c *dynamoCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.Snapshot
}

func (p *Provider) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
	if p.store == nil {
		return status.Error(codes.FailedPrecondition, "dynamodb: not configured")
	}

	return cursorutil.Serve(stream, func(ctx context.Context, req *proto.OpenCursorRequest) (cursorutil.Runtime, error) {
		return p.openCursorSnapshot(ctx, req)
	})
}

func (p *Provider) openCursorSnapshot(ctx context.Context, req *proto.OpenCursorRequest) (*dynamoCursor, error) {
	cursor := &dynamoCursor{
		Snapshot:  cursorutil.NewSnapshot(req),
		provider:  p,
		storeName: req.GetStore(),
	}
	if cursor.IndexCursor {
		meta, err := p.lookupIndexMeta(req.GetStore(), req.GetIndex())
		if err != nil {
			return nil, err
		}
		cursor.index = meta
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
	if err := cursor.Load(entries, req.GetRange()); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (p *Provider) cursorKeys(ctx context.Context, cursor *dynamoCursor, req *proto.OpenCursorRequest) ([]cursorutil.Entry, error) {
	if cursor.IndexCursor {
		return p.cursorIndexKeys(ctx, cursor, req)
	}
	return p.cursorObjectStoreKeys(ctx, req)
}

func (p *Provider) cursorObjectStoreKeys(ctx context.Context, req *proto.OpenCursorRequest) ([]cursorutil.Entry, error) {
	cond, vals := buildKeyCondition(req.GetStore(), req.GetRange())
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

func (p *Provider) cursorIndexKeys(ctx context.Context, cursor *dynamoCursor, req *proto.OpenCursorRequest) ([]cursorutil.Entry, error) {
	cond, exprVals := buildIndexCondition(req.GetStore(), req.GetIndex(), req.GetValues())
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

func (c *dynamoCursor) entryFromRecord(record *proto.Record) (cursorutil.Entry, error) {
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

func (c *dynamoCursor) DeleteCurrent(ctx context.Context) error {
	entry, err := c.Current()
	if err != nil {
		return err
	}
	_, err = c.provider.Delete(ctx, &proto.ObjectStoreRequest{
		Store: c.storeName,
		Id:    entry.PrimaryKey,
	})
	return err
}

func (c *dynamoCursor) UpdateCurrent(ctx context.Context, record *proto.Record) (*proto.CursorEntry, error) {
	entry, err := c.Current()
	if err != nil {
		return nil, err
	}
	cloned, err := cursorutil.CloneRecordWithField(record, "id", entry.PrimaryKeyValue)
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
	c.Entries[c.Pos].Record = cloned
	return c.CurrentEntry()
}

func dynamoRecordFieldAny(record *proto.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	if _, ok := record.Fields[field]; !ok {
		return nil, fmt.Errorf("%w: field %q", errDynamoCursorFieldMissing, field)
	}
	return cursorutil.DirectRecordField(record, field)
}
