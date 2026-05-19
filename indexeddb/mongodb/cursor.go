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
	cursorutil.LazyCursor
	provider  *providerCore
	storeName string
	index     *indexMeta
	req       gestalt.IndexedDBOpenCursorRequest
	docs      *mongo.Cursor
}

var errMongoCursorFieldMissing = errors.New("mongodb cursor field missing")

func (c *mongoCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.LazyCursor.Snapshot
}

func (p *providerCore) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	if _, err := p.configured(); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return p.openCursor(ctx, req)
}

func (p *providerCore) openCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*mongoCursor, error) {
	cursor := &mongoCursor{
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
	return c.LazyCursor.Next(ctx, c.nextEntry)
}

func (c *mongoCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.ContinueToKey(ctx, key, c.nextEntry)
}

func (c *mongoCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.Advance(ctx, count, c.nextEntry)
}

func (c *mongoCursor) Delete(ctx context.Context) error {
	return c.DeleteCurrent(ctx)
}

func (c *mongoCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	return c.UpdateCurrent(ctx, record)
}

func (c *mongoCursor) Close() error {
	if c.docs != nil {
		err := c.docs.Close(context.Background())
		c.docs = nil
		return err
	}
	return nil
}

func (c *mongoCursor) nextEntry(ctx context.Context) (*cursorutil.Entry, error) {
	if c.docs == nil {
		if err := c.openDocs(ctx); err != nil {
			return nil, err
		}
	}
	if !c.docs.Next(ctx) {
		if err := c.docs.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "cursor stream: %v", err)
		}
		return nil, nil
	}
	var doc bson.M
	if err := c.docs.Decode(&doc); err != nil {
		return nil, status.Errorf(codes.Internal, "decode cursor stream: %v", err)
	}
	record, err := docToRecord(doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal cursor record: %v", err)
	}
	entry, err := c.entryFromRecord(record)
	if err != nil {
		if c.IndexCursor && errors.Is(err, errMongoCursorFieldMissing) {
			return c.nextEntry(ctx)
		}
		return nil, err
	}
	return &entry, nil
}

func (c *mongoCursor) openDocs(ctx context.Context) error {
	s, err := c.provider.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := c.cursorFilter(ctx, s)
	if err != nil {
		return err
	}
	opts := options.Find().SetSort(c.cursorSort())
	if projection := mongoCursorProjection(c); projection != nil {
		opts.SetProjection(projection)
	}
	docs, err := s.db.Collection(c.req.Store).Find(ctx, filter, opts)
	if err != nil {
		return status.Errorf(codes.Internal, "open cursor stream: %v", err)
	}
	c.docs = docs
	return nil
}

func (c *mongoCursor) cursorFilter(ctx context.Context, s *Store) (bson.M, error) {
	if c.IndexCursor {
		filter, err := s.indexFilter(ctx, c.req.Store, c.req.Index, c.req.Values)
		if err != nil {
			return nil, err
		}
		return filter, nil
	}
	filter, err := keyRangeFilter(c.req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	return filter, nil
}

func (c *mongoCursor) cursorSort() bson.D {
	direction := 1
	if c.Reverse {
		direction = -1
	}
	if !c.IndexCursor || c.index == nil {
		return bson.D{{Key: "_id", Value: direction}}
	}
	sort := make(bson.D, 0, len(c.index.keyPath)+1)
	for _, field := range c.index.keyPath {
		sort = append(sort, bson.E{Key: field, Value: direction})
	}
	sort = append(sort, bson.E{Key: "_id", Value: direction})
	return sort
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
