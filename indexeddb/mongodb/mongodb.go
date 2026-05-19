package mongodb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type indexMeta struct {
	keyPath []string
	unique  bool
}

type Store struct {
	client *mongo.Client
	db     *mongo.Database

	mu      sync.RWMutex
	schemas map[string]map[string]indexMeta // store -> index name -> meta
}

func NewStore(client *mongo.Client, db *mongo.Database) *Store {
	return &Store{
		client:  client,
		db:      db,
		schemas: make(map[string]map[string]indexMeta),
	}
}

func (s *Store) loadSchemas(ctx context.Context) error {
	names, err := s.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return err
	}

	schemas := make(map[string]map[string]indexMeta, len(names))
	for _, name := range names {
		specs, err := s.db.Collection(name).Indexes().ListSpecifications(ctx)
		if err != nil {
			return err
		}

		indexes := make(map[string]indexMeta)
		for _, spec := range specs {
			if spec.Name == "_id_" {
				continue
			}

			var keys bson.D
			if err := bson.Unmarshal(spec.KeysDocument, &keys); err != nil {
				return err
			}

			keyPath := make([]string, 0, len(keys))
			for _, key := range keys {
				keyPath = append(keyPath, key.Key)
			}

			indexes[spec.Name] = indexMeta{
				keyPath: keyPath,
				unique:  spec.Unique != nil && *spec.Unique,
			}
		}
		schemas[name] = indexes
	}

	s.mu.Lock()
	s.schemas = schemas
	s.mu.Unlock()
	return nil
}

func (s *Store) Close() error {
	return s.client.Disconnect(context.Background())
}

// ---------------------------------------------------------------------------
// IndexedDBServer implementation
// ---------------------------------------------------------------------------

func (p *providerCore) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.CreateCollection(ctx, name); err != nil {
		if !mongo.IsDuplicateKeyError(err) && !isNamespaceExistsError(err) {
			return status.Errorf(codes.Internal, "create collection %s: %v", name, err)
		}
	}

	coll := s.db.Collection(name)
	indexes := make(map[string]indexMeta)
	for _, idx := range schema.Indexes {
		keys := bson.D{}
		for _, field := range idx.KeyPath {
			keys = append(keys, bson.E{Key: field, Value: 1})
		}
		model := mongo.IndexModel{
			Keys:    keys,
			Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
		}
		if _, err := coll.Indexes().CreateOne(ctx, model); err != nil {
			return status.Errorf(codes.Internal, "create index %s: %v", idx.Name, err)
		}
		indexes[idx.Name] = indexMeta{
			keyPath: append([]string(nil), idx.KeyPath...),
			unique:  idx.Unique,
		}
	}

	s.schemas[name] = indexes

	return nil
}

func (p *providerCore) DeleteObjectStore(ctx context.Context, name string) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Collection(name).Drop(ctx); err != nil {
		return status.Errorf(codes.Internal, "drop collection %s: %v", name, err)
	}
	delete(s.schemas, name)
	return nil
}

func (p *providerCore) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	var doc bson.M
	err = s.db.Collection(req.Store).FindOne(ctx, idFilter(req.ID)).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Error(codes.NotFound, "record not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	rec, err := docToRecord(doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal record: %v", err)
	}
	return rec, nil
}

func (p *providerCore) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	s, err := p.configured()
	if err != nil {
		return "", status.Error(codes.FailedPrecondition, err.Error())
	}
	opts := options.FindOne().SetProjection(bson.M{"_id": 1})
	var doc bson.M
	err = s.db.Collection(req.Store).FindOne(ctx, idFilter(req.ID), opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", status.Error(codes.NotFound, "key not found")
		}
		return "", status.Errorf(codes.Internal, "get key: %v", err)
	}
	return fmt.Sprint(doc["_id"]), nil
}

func (p *providerCore) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	doc, err := recordToDoc(req.Record)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "decode record: %v", err)
	}
	_, err = s.db.Collection(req.Store).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return status.Error(codes.AlreadyExists, "record already exists")
		}
		return status.Errorf(codes.Internal, "add: %v", err)
	}
	return nil
}

func (p *providerCore) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	doc, err := recordToDoc(req.Record)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "decode record: %v", err)
	}
	id, ok := doc["_id"]
	if !ok {
		return status.Error(codes.InvalidArgument, "record must have an id field")
	}
	opts := options.Replace().SetUpsert(true)
	_, err = s.db.Collection(req.Store).ReplaceOne(ctx, bson.M{"_id": id}, doc, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return status.Error(codes.AlreadyExists, "record already exists")
		}
		return status.Errorf(codes.Internal, "put: %v", err)
	}
	return nil
}

func (p *providerCore) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	if err := p.deleteByIDValue(ctx, req.Store, req.ID); err != nil {
		return err
	}
	return nil
}

func (p *providerCore) deleteByIDValue(ctx context.Context, storeName string, id any) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	_, err = s.db.Collection(storeName).DeleteOne(ctx, idFilter(id))
	if err != nil {
		return status.Errorf(codes.Internal, "delete: %v", err)
	}
	return nil
}

func (p *providerCore) Clear(ctx context.Context, storeName string) error {
	s, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	_, err = s.db.Collection(storeName).DeleteMany(ctx, bson.M{})
	if err != nil {
		return status.Errorf(codes.Internal, "clear: %v", err)
	}
	return nil
}

func (p *providerCore) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	cursor, err := s.db.Collection(req.Store).Find(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get all: %v", err)
	}
	defer cursor.Close(ctx)
	records, err := cursorToRecords(ctx, cursor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get all cursor: %v", err)
	}
	return records, nil
}

func (p *providerCore) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.Range)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := s.db.Collection(req.Store).Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get all keys: %v", err)
	}
	defer cursor.Close(ctx)
	var keys []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, status.Errorf(codes.Internal, "decode key: %v", err)
		}
		keys = append(keys, fmt.Sprint(doc["_id"]))
	}
	return keys, nil
}

func (p *providerCore) Query(ctx context.Context, req gestalt.IndexedDBObjectStoreQueryRequest) (*gestalt.IndexedDBQueryResponse, error) {
	if len(req.Filters) > 0 {
		return nil, status.Error(codes.InvalidArgument, "query filters are not supported by mongodb object-store scans")
	}
	for _, order := range req.OrderBy {
		if strings.TrimSpace(order.Column) != "" && strings.TrimSpace(order.Column) != "id" {
			return nil, status.Error(codes.InvalidArgument, "query order_by only supports id")
		}
		if order.Descending {
			return nil, status.Error(codes.InvalidArgument, "query order_by only supports ascending order")
		}
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	filter := bson.M{}
	if token := strings.TrimSpace(req.PageToken); token != "" {
		after, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "page_token is invalid")
		}
		filter["_id"] = bson.M{"$gt": string(after)}
	}
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	projection := bson.M{"_id": 1}
	if !req.KeysOnly {
		projection = nil
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(pageSize + 1)).
		SetProjection(projection)
	cursor, err := s.db.Collection(req.Store).Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query: %v", err)
	}
	defer cursor.Close(ctx)
	out := &gestalt.IndexedDBQueryResponse{}
	count := 0
	lastReturnedToken := ""
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, status.Errorf(codes.Internal, "query decode: %v", err)
		}
		count++
		if count > pageSize {
			out.NextPageToken = lastReturnedToken
			break
		}
		key := fmt.Sprint(doc["_id"])
		lastReturnedToken = base64.RawURLEncoding.EncodeToString([]byte(key))
		out.Keys = append(out.Keys, key)
		if !req.KeysOnly {
			record, err := docToRecord(doc)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "query record: %v", err)
			}
			out.Records = append(out.Records, record)
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "query cursor: %v", err)
	}
	return out, nil
}

func (p *providerCore) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	s, err := p.configured()
	if err != nil {
		return 0, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.Range)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	count, err := s.db.Collection(req.Store).CountDocuments(ctx, filter)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "count: %v", err)
	}
	return count, nil
}

func (p *providerCore) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	s, err := p.configured()
	if err != nil {
		return 0, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.Range)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	result, err := s.db.Collection(req.Store).DeleteMany(ctx, filter)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "delete range: %v", err)
	}
	return result.DeletedCount, nil
}

func (p *providerCore) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	entries, err := p.queryIndexEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return entries[0].Record, nil
}

func (p *providerCore) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", status.Error(codes.NotFound, "key not found")
	}
	return entries[0].PrimaryKey, nil
}

func (p *providerCore) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	entries, err := p.queryIndexEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	records := make([]gestalt.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return records, nil
}

func (p *providerCore) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return keys, nil
}

func (p *providerCore) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return 0, err
	}
	return int64(len(entries)), nil
}

func (p *providerCore) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, entry := range entries {
		if err := p.deleteByIDValue(ctx, req.Store, entry.PrimaryKeyValue); err != nil {
			return 0, mongoMapCursorWriteErr("index_delete", err)
		}
		deleted++
	}
	return deleted, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (p *providerCore) queryIndexEntries(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]cursorutil.Entry, error) {
	return p.queryIndexEntriesWithProjection(ctx, req, nil)
}

func (p *providerCore) queryIndexKeyEntries(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]cursorutil.Entry, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	meta, err := getMongoIndexMetaForContext(ctx, s, req.Store, req.Index)
	if err != nil {
		return nil, err
	}
	return p.queryIndexEntriesWithProjection(ctx, req, indexProjection(meta.keyPath))
}

func (p *providerCore) queryIndexEntriesWithProjection(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest, projection bson.M) ([]cursorutil.Entry, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	meta, err := getMongoIndexMetaForContext(ctx, s, req.Store, req.Index)
	if err != nil {
		return nil, err
	}

	filter, err := s.indexFilter(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	findOpts := options.Find()
	if len(projection) > 0 {
		findOpts.SetProjection(projection)
	}
	cursor, err := s.db.Collection(req.Store).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index query: %v", err)
	}
	defer cursor.Close(ctx)

	records, err := cursorToRecords(ctx, cursor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index query cursor: %v", err)
	}

	rangeCursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}},
		index:    &meta,
	}
	entries := make([]cursorutil.Entry, 0, len(records))
	for _, record := range records {
		entry, err := rangeCursor.entryFromRecord(record)
		if err != nil {
			if errors.Is(err, errMongoCursorFieldMissing) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "index query decode: %v", err)
		}
		entries = append(entries, entry)
	}

	entries, err = rangeCursor.ApplyRange(entries, req.Range)
	if err != nil {
		return nil, err
	}
	sortMongoIndexEntries(entries)
	return entries, nil
}

func sortMongoIndexEntries(entries []cursorutil.Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := cursorutil.CompareValues(entries[i].Key, entries[j].Key); cmp != 0 {
			return cmp < 0
		}
		return cursorutil.CompareValues(entries[i].PrimaryKeyValue, entries[j].PrimaryKeyValue) < 0
	})
}

func indexProjection(keyPath []string) bson.M {
	projection := bson.M{"_id": 1}
	for _, field := range keyPath {
		projection[field] = 1
	}
	return projection
}

func (s *Store) getIndexMeta(store, index string) (indexMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeSchemas, ok := s.schemas[store]
	if !ok {
		return indexMeta{}, status.Errorf(codes.NotFound, "object store %q has no registered schema", store)
	}
	meta, ok := storeSchemas[index]
	if !ok {
		return indexMeta{}, status.Errorf(codes.NotFound, "index %q not found on store %q", index, store)
	}
	return meta, nil
}

func (s *Store) indexFilter(ctx context.Context, store, index string, values []any) (bson.M, error) {
	meta, err := getMongoIndexMetaForContext(ctx, s, store, index)
	if err != nil {
		return nil, err
	}

	filter := bson.M{}
	for i, field := range meta.keyPath {
		if i < len(values) {
			filter[field] = values[i]
		}
	}
	return filter, nil
}

func keyRangeFilter(r *gestalt.KeyRange) (bson.M, error) {
	if r == nil {
		return bson.M{}, nil
	}
	idFilter := bson.M{}
	if r.Lower != nil {
		if r.LowerOpen {
			idFilter["$gt"] = r.Lower
		} else {
			idFilter["$gte"] = r.Lower
		}
	}
	if r.Upper != nil {
		if r.UpperOpen {
			idFilter["$lt"] = r.Upper
		} else {
			idFilter["$lte"] = r.Upper
		}
	}
	if len(idFilter) == 0 {
		return bson.M{}, nil
	}
	return bson.M{"_id": idFilter}, nil
}

func recordToDoc(record gestalt.Record) (bson.M, error) {
	if record == nil {
		return bson.M{}, nil
	}
	doc := bson.M{}
	for k, v := range record {
		if k == "id" {
			doc["_id"] = v
		} else {
			doc[k] = v
		}
	}
	return doc, nil
}

func docToRecord(doc bson.M) (gestalt.Record, error) {
	m := make(gestalt.Record, len(doc))
	for k, v := range doc {
		if k == "_id" {
			m["id"] = toGestaltCompatible(v)
		} else {
			m[k] = toGestaltCompatible(v)
		}
	}
	return m, nil
}

func cursorToRecords(ctx context.Context, cursor *mongo.Cursor) ([]gestalt.Record, error) {
	var records []gestalt.Record
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		rec, err := docToRecord(doc)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func toGestaltCompatible(v any) any {
	switch val := v.(type) {
	case bson.M:
		m := make(map[string]any, len(val))
		for k, v2 := range val {
			m[k] = toGestaltCompatible(v2)
		}
		return m
	case bson.D:
		m := make(map[string]any, len(val))
		for _, elem := range val {
			m[elem.Key] = toGestaltCompatible(elem.Value)
		}
		return m
	case bson.A:
		a := make([]any, len(val))
		for i, v2 := range val {
			a[i] = toGestaltCompatible(v2)
		}
		return a
	case int32:
		return int64(val)
	case int64:
		return val
	case float32:
		return float64(val)
	case bson.DateTime:
		return val.Time()
	case bson.Binary:
		return val.Data
	case bson.ObjectID:
		return val.Hex()
	default:
		return val
	}
}

func isNamespaceExistsError(err error) bool {
	ce, ok := err.(mongo.CommandError)
	if !ok {
		return false
	}
	return ce.Code == 48 // NamespaceExists
}
