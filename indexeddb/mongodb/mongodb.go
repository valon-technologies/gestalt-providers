package mongodb

import (
	"context"
	"fmt"
	"sync"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	doc := protoToDoc(req.GetRecord())
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
	doc := protoToDoc(req.GetRecord())
	id, ok := doc["_id"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "record must have an id field")
	}
	opts := options.Replace().SetUpsert(true)
	_, err = s.db.Collection(req.GetStore()).ReplaceOne(ctx, bson.M{"_id": id}, doc, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	_, err = s.db.Collection(req.GetStore()).DeleteOne(ctx, idFilter(req.GetId()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &emptypb.Empty{}, nil
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
	filter := keyRangeFilter(req.GetRange())
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
	filter := keyRangeFilter(req.GetRange())
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
	filter := keyRangeFilter(req.GetRange())
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
	filter := keyRangeFilter(req.GetRange())
	result, err := s.db.Collection(req.GetStore()).DeleteMany(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete range: %v", err)
	}
	return &proto.DeleteResponse{Deleted: result.DeletedCount}, nil
}

func (p *Provider) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	var doc bson.M
	err = s.db.Collection(req.GetStore()).FindOne(ctx, filter).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Error(codes.NotFound, "record not found")
		}
		return nil, status.Errorf(codes.Internal, "index get: %v", err)
	}
	rec, err := docToProto(doc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal record: %v", err)
	}
	return &proto.RecordResponse{Record: rec}, nil
}

func (p *Provider) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	opts := options.FindOne().SetProjection(bson.M{"_id": 1})
	var doc bson.M
	err = s.db.Collection(req.GetStore()).FindOne(ctx, filter, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Errorf(codes.Internal, "index get key: %v", err)
	}
	return &proto.KeyResponse{Key: fmt.Sprint(doc["_id"])}, nil
}

func (p *Provider) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	cursor, err := s.db.Collection(req.GetStore()).Find(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index get all: %v", err)
	}
	defer cursor.Close(ctx)
	records, err := cursorToProtos(ctx, cursor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index get all cursor: %v", err)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cursor, err := s.db.Collection(req.GetStore()).Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index get all keys: %v", err)
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

func (p *Provider) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	count, err := s.db.Collection(req.GetStore()).CountDocuments(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index count: %v", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (p *Provider) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	filter, err := s.indexFilter(req.GetStore(), req.GetIndex(), req.GetValues())
	if err != nil {
		return nil, err
	}
	result, err := s.db.Collection(req.GetStore()).DeleteMany(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index delete: %v", err)
	}
	return &proto.DeleteResponse{Deleted: result.DeletedCount}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Store) indexFilter(store, index string, values []*structpb.Value) (bson.M, error) {
	s.mu.RLock()
	storeSchemas, ok := s.schemas[store]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "object store %q has no registered schema", store)
	}
	meta, ok := storeSchemas[index]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %q not found on store %q", index, store)
	}

	filter := bson.M{}
	for i, field := range meta.keyPath {
		if i < len(values) {
			filter[field] = values[i].AsInterface()
		}
	}
	return filter, nil
}

func keyRangeFilter(r *proto.KeyRange) bson.M {
	if r == nil {
		return bson.M{}
	}
	idFilter := bson.M{}
	if r.GetLower() != nil {
		if r.GetLowerOpen() {
			idFilter["$gt"] = r.GetLower().AsInterface()
		} else {
			idFilter["$gte"] = r.GetLower().AsInterface()
		}
	}
	if r.GetUpper() != nil {
		if r.GetUpperOpen() {
			idFilter["$lt"] = r.GetUpper().AsInterface()
		} else {
			idFilter["$lte"] = r.GetUpper().AsInterface()
		}
	}
	if len(idFilter) == 0 {
		return bson.M{}
	}
	return bson.M{"_id": idFilter}
}

func protoToDoc(s *structpb.Struct) bson.M {
	if s == nil {
		return bson.M{}
	}
	doc := bson.M{}
	for k, v := range s.AsMap() {
		if k == "id" {
			doc["_id"] = v
		} else {
			doc[k] = v
		}
	}
	return doc
}

func docToProto(doc bson.M) (*structpb.Struct, error) {
	m := make(map[string]any, len(doc))
	for k, v := range doc {
		if k == "_id" {
			m["id"] = fmt.Sprint(v)
		} else {
			m[k] = toStructpbCompatible(v)
		}
	}
	return structpb.NewStruct(m)
}

func cursorToProtos(ctx context.Context, cursor *mongo.Cursor) ([]*structpb.Struct, error) {
	var records []*structpb.Struct
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

// toStructpbCompatible converts BSON-specific types (like bson.A, int32, etc.)
// into types that structpb.NewStruct can handle (string, float64, bool, nil,
// []any, map[string]any).
func toStructpbCompatible(v any) any {
	switch val := v.(type) {
	case bson.M:
		m := make(map[string]any, len(val))
		for k, v2 := range val {
			m[k] = toStructpbCompatible(v2)
		}
		return m
	case bson.A:
		a := make([]any, len(val))
		for i, v2 := range val {
			a[i] = toStructpbCompatible(v2)
		}
		return a
	case int32:
		return float64(val)
	case int64:
		return float64(val)
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
