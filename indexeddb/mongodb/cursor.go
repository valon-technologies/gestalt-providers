package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mongoCursor struct {
	cursorutil.Snapshot
	provider  *providerCore
	storeName string
	index     *indexMeta
}

var errMongoCursorFieldMissing = errors.New("mongodb cursor field missing")

func (c *mongoCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.Snapshot
}

func (p *providerCore) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	if _, err := p.configured(); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return p.openCursorSnapshot(ctx, req)
}

func (p *providerCore) openCursorSnapshot(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*mongoCursor, error) {
	cursor := &mongoCursor{
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

	records, err := p.cursorRecords(ctx, cursor, req)
	if err != nil {
		return nil, err
	}
	entries, err := cursorutil.EntriesFromRecords(records, cursor.entryFromRecord, func(err error) bool {
		return cursor.IndexCursor && errors.Is(err, errMongoCursorFieldMissing)
	})
	if err != nil {
		return nil, err
	}
	if err := cursor.Load(entries, req.Range); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (p *providerCore) lookupIndexMeta(storeName, indexName string) (*indexMeta, error) {
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

func (p *providerCore) cursorRecords(ctx context.Context, cursor *mongoCursor, req gestalt.IndexedDBOpenCursorRequest) ([]gestalt.Record, error) {
	docs, err := p.cursorDocuments(ctx, cursor, req)
	if err != nil {
		return nil, err
	}

	records := make([]gestalt.Record, 0, len(docs))
	for _, doc := range docs {
		record, err := docToRecord(doc)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal cursor record: %v", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (p *providerCore) cursorDocuments(ctx context.Context, cursor *mongoCursor, req gestalt.IndexedDBOpenCursorRequest) ([]bson.M, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var filter bson.M
	if cursor.IndexCursor {
		filter, err = s.indexFilter(ctx, req.Store, req.Index, req.Values)
		if err != nil {
			return nil, err
		}
	} else {
		filter, err = keyRangeFilter(req.Range)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
		}
	}

	var mongoCursorDocs *mongo.Cursor
	projection := mongoCursorProjection(cursor)
	if projection == nil {
		mongoCursorDocs, err = s.db.Collection(req.Store).Find(ctx, filter)
	} else {
		mongoCursorDocs, err = s.db.Collection(req.Store).Find(ctx, filter, options.Find().SetProjection(projection))
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
	if !cursor.KeysOnly {
		return nil
	}

	projection := bson.M{"_id": 1}
	if cursor.IndexCursor && cursor.index != nil {
		for _, field := range cursor.index.keyPath {
			projection[field] = 1
		}
	}
	return projection
}

func (c *mongoCursor) entryFromRecord(record gestalt.Record) (cursorutil.Entry, error) {
	primaryKeyValue, primaryKey, err := mongoExtractID(record)
	if err != nil {
		return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}

	key := primaryKeyValue
	if c.IndexCursor {
		parts := make([]any, len(c.index.keyPath))
		for i, field := range c.index.keyPath {
			value, err := mongoRecordFieldAny(record, field)
			if err != nil {
				if errors.Is(err, errMongoCursorFieldMissing) {
					return cursorutil.Entry{}, err
				}
				return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record index field %q: %v", field, err)
			}
			parts[i] = value
		}
		key = parts
	}

	entry := cursorutil.Entry{
		Key:             key,
		PrimaryKey:      primaryKey,
		PrimaryKeyValue: primaryKeyValue,
	}
	if !c.KeysOnly {
		entry.Record = record
	}
	return entry, nil
}

func (c *mongoCursor) Next(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	entry, _, err := c.ContinueNext()
	return entry, err
}

func (c *mongoCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	entry, _, err := c.Snapshot.ContinueToKey(key)
	return entry, err
}

func (c *mongoCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	entry, _, err := c.Snapshot.Advance(count)
	return entry, err
}

func (c *mongoCursor) Delete(ctx context.Context) error {
	return c.DeleteCurrent(ctx)
}

func (c *mongoCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	return c.UpdateCurrent(ctx, record)
}

func (c *mongoCursor) Close() error {
	return nil
}

func (c *mongoCursor) DeleteCurrent(ctx context.Context) error {
	entry, err := c.Current()
	if err != nil {
		return err
	}
	return c.provider.deleteByIDValue(ctx, c.storeName, entry.PrimaryKeyValue)
}

func (c *mongoCursor) UpdateCurrent(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
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

func mongoExtractID(record gestalt.Record) (any, string, error) {
	if record == nil {
		return nil, "", status.Error(codes.InvalidArgument, "record is required")
	}
	value, ok := record["id"]
	if !ok || value == nil {
		return nil, "", status.Error(codes.InvalidArgument, "record must contain an \"id\" field")
	}
	id := fmt.Sprint(value)
	if id == "" {
		return nil, "", status.Error(codes.InvalidArgument, "record \"id\" must be non-empty")
	}
	return value, id, nil
}

func mongoRecordFieldAny(record gestalt.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	value, ok := mongoLookupField(record, strings.Split(field, "."))
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
