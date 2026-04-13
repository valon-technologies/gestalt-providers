package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (p *Provider) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	name := req.GetName()
	if err := s.db.CreateCollection(ctx, name); err != nil {
		if !mongo.IsDuplicateKeyError(err) && !isNamespaceExistsError(err) {
			return nil, status.Errorf(codes.Internal, "create collection %s: %v", name, err)
		}
	}

	coll := s.db.Collection(name)
	indexes := make(map[string]indexMeta)
	for _, idx := range req.GetSchema().GetIndexes() {
		keys := bson.D{}
		for _, field := range idx.GetKeyPath() {
			keys = append(keys, bson.E{Key: field, Value: 1})
		}
		model := mongo.IndexModel{
			Keys:    keys,
			Options: options.Index().SetName(idx.GetName()).SetUnique(idx.GetUnique()),
		}
		if _, err := coll.Indexes().CreateOne(ctx, model); err != nil {
			return nil, status.Errorf(codes.Internal, "create index %s: %v", idx.GetName(), err)
		}
		indexes[idx.GetName()] = indexMeta{
			keyPath: idx.GetKeyPath(),
			unique:  idx.GetUnique(),
		}
	}

	s.mu.Lock()
	s.schemas[name] = indexes
	s.mu.Unlock()

	return &emptypb.Empty{}, nil
}

func (p *Provider) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	name := req.GetName()
	if err := s.db.Collection(name).Drop(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "drop collection %s: %v", name, err)
	}
	s.mu.Lock()
	delete(s.schemas, name)
	s.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	var doc bson.M
	err = s.db.Collection(req.GetStore()).FindOne(ctx, idFilter(req.GetId())).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Error(codes.NotFound, "record not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	rec, err := docToProto(doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal record: %v", err)
	}
	return &proto.RecordResponse{Record: rec}, nil
}

func (p *Provider) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	opts := options.FindOne().SetProjection(bson.M{"_id": 1})
	var doc bson.M
	err = s.db.Collection(req.GetStore()).FindOne(ctx, idFilter(req.GetId()), opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, "get key: %v", err)
	}
	return &proto.KeyResponse{Key: fmt.Sprint(doc["_id"])}, nil
}

func (p *Provider) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	doc, err := protoToDoc(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode record: %v", err)
	}
	_, err = s.db.Collection(req.GetStore()).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, status.Error(codes.AlreadyExists, "record already exists")
		}
		return nil, status.Errorf(codes.Internal, "add: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	doc, err := protoToDoc(req.GetRecord())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decode record: %v", err)
	}
	id, ok := doc["_id"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "record must have an id field")
	}
	opts := options.Replace().SetUpsert(true)
	_, err = s.db.Collection(req.GetStore()).ReplaceOne(ctx, bson.M{"_id": id}, doc, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, status.Error(codes.AlreadyExists, "record already exists")
		}
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	if err := p.deleteByIDValue(ctx, req.GetStore(), req.GetId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) deleteByIDValue(ctx context.Context, storeName string, id any) error {
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

func (p *Provider) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	_, err = s.db.Collection(req.GetStore()).DeleteMany(ctx, bson.M{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "clear: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.GetRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	cursor, err := s.db.Collection(req.GetStore()).Find(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get all: %v", err)
	}
	defer cursor.Close(ctx)
	records, err := cursorToProtos(ctx, cursor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get all cursor: %v", err)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (p *Provider) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.GetRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := s.db.Collection(req.GetStore()).Find(ctx, filter, opts)
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
	return &proto.KeysResponse{Keys: keys}, nil
}

func (p *Provider) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.GetRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	count, err := s.db.Collection(req.GetStore()).CountDocuments(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count: %v", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (p *Provider) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := keyRangeFilter(req.GetRange())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid key range: %v", err)
	}
	result, err := s.db.Collection(req.GetStore()).DeleteMany(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete range: %v", err)
	}
	return &proto.DeleteResponse{Deleted: result.DeletedCount}, nil
}

func (p *Provider) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	entries, err := p.queryIndexEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: entries[0].record}, nil
}

func (p *Provider) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &proto.KeyResponse{Key: entries[0].primaryKey}, nil
}

func (p *Provider) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	entries, err := p.queryIndexEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.record)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.primaryKey)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (p *Provider) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: int64(len(entries))}, nil
}

func (p *Provider) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	var deleted int64
	for _, entry := range entries {
		if err := p.deleteByIDValue(ctx, req.GetStore(), entry.primaryKeyValue); err != nil {
			return nil, mongoMapCursorWriteErr("index_delete", err)
		}
		deleted++
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (p *Provider) queryIndexEntries(ctx context.Context, req *proto.IndexQueryRequest) ([]mongoCursorEntry, error) {
	return p.queryIndexEntriesWithProjection(ctx, req, nil)
}

func (p *Provider) queryIndexKeyEntries(ctx context.Context, req *proto.IndexQueryRequest) ([]mongoCursorEntry, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	meta, err := s.getIndexMeta(req.GetStore(), req.GetIndex())
	if err != nil {
		return nil, err
	}
	return p.queryIndexEntriesWithProjection(ctx, req, indexProjection(meta.keyPath))
}

func (p *Provider) queryIndexEntriesWithProjection(ctx context.Context, req *proto.IndexQueryRequest, projection bson.M) ([]mongoCursorEntry, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	meta, err := s.getIndexMeta(req.GetStore(), req.GetIndex())
	if err != nil {
		return nil, err
	}

	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	findOpts := options.Find()
	if len(projection) > 0 {
		findOpts.SetProjection(projection)
	}
	cursor, err := s.db.Collection(req.GetStore()).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index query: %v", err)
	}
	defer cursor.Close(ctx)

	records, err := cursorToProtos(ctx, cursor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index query cursor: %v", err)
	}

	rangeCursor := &mongoCursor{indexCursor: true, index: &meta}
	entries := make([]mongoCursorEntry, 0, len(records))
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

	entries, err = rangeCursor.applyRange(entries, req.GetRange())
	if err != nil {
		return nil, err
	}
	sortMongoIndexEntries(entries)
	return entries, nil
}

func sortMongoIndexEntries(entries []mongoCursorEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := mongoCompareCursorValue(entries[i].key, entries[j].key); cmp != 0 {
			return cmp < 0
		}
		return mongoCompareCursorValue(entries[i].primaryKeyValue, entries[j].primaryKeyValue) < 0
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

func (s *Store) indexFilter(store, index string, values []*proto.TypedValue) (bson.M, error) {
	meta, err := s.getIndexMeta(store, index)
	if err != nil {
		return nil, err
	}

	filter := bson.M{}
	goValues, err := gestalt.AnyFromTypedValues(values)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid index values: %v", err)
	}
	for i, field := range meta.keyPath {
		if i < len(goValues) {
			filter[field] = goValues[i]
		}
	}
	return filter, nil
}

func keyRangeFilter(r *proto.KeyRange) (bson.M, error) {
	if r == nil {
		return bson.M{}, nil
	}
	idFilter := bson.M{}
	if r.GetLower() != nil {
		lower, err := gestalt.AnyFromTypedValue(r.GetLower())
		if err != nil {
			return nil, err
		}
		if r.GetLowerOpen() {
			idFilter["$gt"] = lower
		} else {
			idFilter["$gte"] = lower
		}
	}
	if r.GetUpper() != nil {
		upper, err := gestalt.AnyFromTypedValue(r.GetUpper())
		if err != nil {
			return nil, err
		}
		if r.GetUpperOpen() {
			idFilter["$lt"] = upper
		} else {
			idFilter["$lte"] = upper
		}
	}
	if len(idFilter) == 0 {
		return bson.M{}, nil
	}
	return bson.M{"_id": idFilter}, nil
}

func protoToDoc(record *proto.Record) (bson.M, error) {
	if record == nil {
		return bson.M{}, nil
	}
	m, err := gestalt.RecordFromProto(record)
	if err != nil {
		return nil, err
	}
	doc := bson.M{}
	for k, v := range m {
		if k == "id" {
			doc["_id"] = v
		} else {
			doc[k] = v
		}
	}
	return doc, nil
}

func docToProto(doc bson.M) (*proto.Record, error) {
	m := make(map[string]any, len(doc))
	for k, v := range doc {
		if k == "_id" {
			m["id"] = toGestaltCompatible(v)
		} else {
			m[k] = toGestaltCompatible(v)
		}
	}
	return gestalt.RecordToProto(m)
}

func cursorToProtos(ctx context.Context, cursor *mongo.Cursor) ([]*proto.Record, error) {
	var records []*proto.Record
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		rec, err := docToProto(doc)
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
