package dynamodb

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/txstream"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const maxDynamoTransactWriteItems = 100

func (p *providerCore) Transaction(stream proto.IndexedDB_TransactionServer) error {
	return txstream.Serve(stream, p.beginTransaction)
}

func (p *providerCore) beginTransaction(ctx context.Context, req *proto.BeginTransactionRequest) (txstream.Transaction, error) {
	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "dynamodb: not configured")
	}

	st := p.store
	st.mu.RLock()

	stores := make(map[string]*dynamoTransactionStore, len(req.GetStores()))
	schemas := make(map[string]*storedSchema, len(req.GetStores()))
	for _, storeName := range req.GetStores() {
		schema, ok := st.schemas[storeName]
		if !ok {
			st.mu.RUnlock()
			return nil, status.Errorf(codes.NotFound, "object store not found: %s", storeName)
		}
		schemas[storeName] = copyDynamoSchema(schema)
		txStore, err := st.loadTransactionStore(ctx, storeName)
		if err != nil {
			st.mu.RUnlock()
			return nil, err
		}
		stores[storeName] = txStore
	}

	return &dynamoTransaction{
		store:   st,
		stores:  stores,
		schemas: schemas,
		unlock: func() {
			st.mu.RUnlock()
		},
	}, nil
}

func copyDynamoSchema(in *storedSchema) *storedSchema {
	if in == nil {
		return nil
	}
	out := &storedSchema{Indexes: make([]indexDef, len(in.Indexes))}
	for i, idx := range in.Indexes {
		out.Indexes[i] = indexDef{
			Name:    idx.Name,
			KeyPath: append([]string(nil), idx.KeyPath...),
			Unique:  idx.Unique,
		}
	}
	return out
}

type dynamoTransaction struct {
	store   *store
	stores  map[string]*dynamoTransactionStore
	schemas map[string]*storedSchema
	mu      sync.Mutex
	done    bool
	unlock  func()
}

type dynamoTransactionStore struct {
	original map[string]*dynamoTransactionRecord
	records  map[string]*proto.Record
}

type dynamoTransactionRecord struct {
	record *proto.Record
	data   ddbtypes.AttributeValue
}

func (s *store) loadTransactionStore(ctx context.Context, storeName string) (*dynamoTransactionStore, error) {
	original := map[string]*dynamoTransactionRecord{}
	records := map[string]*proto.Record{}
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk"),
			ConsistentRead:         aws.Bool(true),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": &ddbtypes.AttributeValueMemberS{Value: storeName},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			id := getS(item, attrSK)
			record, err := parseData(item)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "load transaction record: %v", err)
			}
			if id == "" || record == nil {
				continue
			}
			cloned := cloneDynamoRecord(record)
			original[id] = &dynamoTransactionRecord{
				record: cloned,
				data:   cloneDynamoAttribute(item[attrData]),
			}
			records[id] = cloneDynamoRecord(record)
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return &dynamoTransactionStore{original: original, records: records}, nil
}

func (t *dynamoTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()

	items, legacyItems, err := t.commitItems()
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	if len(items) > maxDynamoTransactWriteItems {
		return status.Errorf(codes.ResourceExhausted, "dynamodb transaction uses %d write items; maximum is %d", len(items), maxDynamoTransactWriteItems)
	}
	if _, err := t.store.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items}); err != nil {
		if isConditionFailed(err) {
			return status.Error(codes.Aborted, "dynamodb transaction conflict")
		}
		return wrapErr(err)
	}
	_ = t.store.deleteLegacyUniqueIndexItems(ctx, legacyItems)
	return nil
}

func (t *dynamoTransaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()
	return nil
}

func (t *dynamoTransaction) transactionStore(name string) (*dynamoTransactionStore, error) {
	txStore, ok := t.stores[name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid transaction: object store not in scope: %s", name)
	}
	return txStore, nil
}

func (t *dynamoTransaction) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	record := txStore.records[req.GetId()]
	if record == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: cloneDynamoRecord(record)}, nil
}

func (t *dynamoTransaction) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	if txStore.records[req.GetId()] == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.KeyResponse{Key: req.GetId()}, nil
}

func (t *dynamoTransaction) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	id, err := extractID(req.GetRecord())
	if err != nil {
		return nil, err
	}
	if txStore.records[id] != nil {
		return nil, status.Errorf(codes.AlreadyExists, "record %s already exists", id)
	}
	txStore.records[id] = cloneDynamoRecord(req.GetRecord())
	if err := t.validateUniqueIndexes(req.GetStore()); err != nil {
		delete(txStore.records, id)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (t *dynamoTransaction) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	id, err := extractID(req.GetRecord())
	if err != nil {
		return nil, err
	}
	previous := txStore.records[id]
	txStore.records[id] = cloneDynamoRecord(req.GetRecord())
	if err := t.validateUniqueIndexes(req.GetStore()); err != nil {
		if previous == nil {
			delete(txStore.records, id)
		} else {
			txStore.records[id] = previous
		}
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (t *dynamoTransaction) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	delete(txStore.records, req.GetId())
	return &emptypb.Empty{}, nil
}

func (t *dynamoTransaction) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	txStore.records = map[string]*proto.Record{}
	return &emptypb.Empty{}, nil
}

func (t *dynamoTransaction) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	entries, err := t.objectStoreEntries(req.GetStore(), req.GetRange(), false)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, cloneDynamoRecord(entry.Record))
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (t *dynamoTransaction) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	entries, err := t.objectStoreEntries(req.GetStore(), req.GetRange(), true)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (t *dynamoTransaction) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	entries, err := t.objectStoreEntries(req.GetStore(), req.GetRange(), true)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: int64(len(entries))}, nil
}

func (t *dynamoTransaction) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	entries, err := t.objectStoreEntries(req.GetStore(), req.GetRange(), true)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		delete(txStore.records, entry.PrimaryKey)
	}
	return &proto.DeleteResponse{Deleted: int64(len(entries))}, nil
}

func (t *dynamoTransaction) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	entries, err := t.indexEntries(req, false)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: cloneDynamoRecord(entries[0].Record)}, nil
}

func (t *dynamoTransaction) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	entries, err := t.indexEntries(req, true)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.KeyResponse{Key: entries[0].PrimaryKey}, nil
}

func (t *dynamoTransaction) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	entries, err := t.indexEntries(req, false)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, cloneDynamoRecord(entry.Record))
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (t *dynamoTransaction) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	entries, err := t.indexEntries(req, true)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (t *dynamoTransaction) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	entries, err := t.indexEntries(req, true)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: int64(len(entries))}, nil
}

func (t *dynamoTransaction) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	entries, err := t.indexEntries(req, true)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		delete(txStore.records, entry.PrimaryKey)
	}
	return &proto.DeleteResponse{Deleted: int64(len(entries))}, nil
}

func (t *dynamoTransaction) objectStoreEntries(storeName string, keyRange *proto.KeyRange, keysOnly bool) ([]cursorutil.Entry, error) {
	txStore, err := t.transactionStore(storeName)
	if err != nil {
		return nil, err
	}
	entries := make([]cursorutil.Entry, 0, len(txStore.records))
	for id, record := range txStore.records {
		entry := cursorutil.Entry{
			Key:             id,
			PrimaryKey:      id,
			PrimaryKeyValue: id,
		}
		if !keysOnly {
			entry.Record = cloneDynamoRecord(record)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return cursorutil.CompareValues(entries[i].Key, entries[j].Key) < 0
	})
	return (&cursorutil.Snapshot{}).ApplyRange(entries, keyRange)
}

func (t *dynamoTransaction) indexEntries(req *proto.IndexQueryRequest, keysOnly bool) ([]cursorutil.Entry, error) {
	txStore, err := t.transactionStore(req.GetStore())
	if err != nil {
		return nil, err
	}
	index, err := t.indexDef(req.GetStore(), req.GetIndex())
	if err != nil {
		return nil, err
	}
	rangeCursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexCursor: true},
		index:    index,
	}
	entries := make([]cursorutil.Entry, 0, len(txStore.records))
	for _, record := range txStore.records {
		entry, err := rangeCursor.entryFromRecord(record)
		if err != nil {
			if errors.Is(err, errDynamoCursorFieldMissing) {
				continue
			}
			return nil, err
		}
		matches, err := indexEntryMatchesValues(entry, req.GetValues())
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		if keysOnly {
			entry.Record = nil
		} else {
			entry.Record = cloneDynamoRecord(entry.Record)
		}
		entries = append(entries, entry)
	}
	entries, err = rangeCursor.ApplyRange(entries, req.GetRange())
	if err != nil {
		return nil, err
	}
	sortDynamoIndexEntries(entries)
	return entries, nil
}

func indexEntryMatchesValues(entry cursorutil.Entry, values []*proto.TypedValue) (bool, error) {
	if len(values) == 0 {
		return true, nil
	}
	expected, err := gestalt.AnyFromTypedValues(values)
	if err != nil {
		return false, status.Errorf(codes.InvalidArgument, "invalid index values: %v", err)
	}
	var actual []any
	switch key := entry.Key.(type) {
	case []any:
		actual = key
	default:
		actual = []any{key}
	}
	if len(expected) > len(actual) {
		return false, nil
	}
	for i := range expected {
		if cursorutil.CompareValues(actual[i], expected[i]) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (t *dynamoTransaction) validateUniqueIndexes(storeName string) error {
	txStore, err := t.transactionStore(storeName)
	if err != nil {
		return err
	}
	seen := map[string]string{}
	ids := make([]string, 0, len(txStore.records))
	for id := range txStore.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		for _, idx := range buildIndexItemsForSchema(storeName, id, txStore.records[id], t.schemas[storeName]) {
			if !idx.unique {
				continue
			}
			if prev, ok := seen[idx.key()]; ok && prev != id {
				return status.Errorf(codes.AlreadyExists, "record %s violates a unique index", id)
			}
			seen[idx.key()] = id
		}
	}
	return nil
}

func (t *dynamoTransaction) commitItems() ([]ddbtypes.TransactWriteItem, []indexItem, error) {
	var items []ddbtypes.TransactWriteItem
	var legacyItems []indexItem
	storeNames := make([]string, 0, len(t.stores))
	for name := range t.stores {
		storeNames = append(storeNames, name)
	}
	sort.Strings(storeNames)

	for _, storeName := range storeNames {
		txStore := t.stores[storeName]
		ids := transactionRecordIDs(txStore)
		for _, id := range ids {
			original := txStore.original[id]
			current := txStore.records[id]
			if original != nil && current != nil && gproto.Equal(original.record, current) {
				continue
			}

			oldIndexItems := buildIndexItemsForSchema(storeName, id, originalRecord(original), t.schemas[storeName])
			newIndexItems := buildIndexItemsForSchema(storeName, id, current, t.schemas[storeName])
			legacyItems = append(legacyItems, oldIndexItems...)
			legacyItems = append(legacyItems, newIndexItems...)

			if current == nil {
				if original == nil {
					continue
				}
				items = append(items, t.deleteRecordItem(storeName, id, original))
				for _, idx := range oldIndexItems {
					items = append(items, deleteIndexItem(t.store.table, idx))
				}
				continue
			}

			data, err := marshalRecord(current)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, t.putRecordItem(storeName, id, data, original))

			newKeys := make(map[string]struct{}, len(newIndexItems))
			for _, idx := range newIndexItems {
				newKeys[idx.key()] = struct{}{}
				items = append(items, ddbtypes.TransactWriteItem{Put: t.store.indexPutItem(idx, data, original != nil)})
			}
			for _, idx := range oldIndexItems {
				if _, ok := newKeys[idx.key()]; ok {
					continue
				}
				items = append(items, deleteIndexItem(t.store.table, idx))
			}
		}
	}
	return items, legacyItems, nil
}

func (t *dynamoTransaction) indexDef(storeName, indexName string) (*indexDef, error) {
	schema, ok := t.schemas[storeName]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "store %q not found", storeName)
	}
	for i := range schema.Indexes {
		if schema.Indexes[i].Name == indexName {
			return &schema.Indexes[i], nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "index %q not found on store %q", indexName, storeName)
}

func transactionRecordIDs(txStore *dynamoTransactionStore) []string {
	seen := map[string]struct{}{}
	for id := range txStore.original {
		seen[id] = struct{}{}
	}
	for id := range txStore.records {
		seen[id] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func originalRecord(record *dynamoTransactionRecord) *proto.Record {
	if record == nil {
		return nil
	}
	return record.record
}

func (t *dynamoTransaction) putRecordItem(storeName, id string, data []byte, original *dynamoTransactionRecord) ddbtypes.TransactWriteItem {
	put := &ddbtypes.Put{
		TableName: &t.store.table,
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:   &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK:   &ddbtypes.AttributeValueMemberS{Value: id},
			attrData: &ddbtypes.AttributeValueMemberB{Value: data},
		},
	}
	if original == nil {
		put.ConditionExpression = aws.String("attribute_not_exists(PK)")
	} else if original.data != nil {
		put.ConditionExpression = aws.String("#data = :old_data")
		put.ExpressionAttributeNames = map[string]string{"#data": attrData}
		put.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{":old_data": cloneDynamoAttribute(original.data)}
	}
	return ddbtypes.TransactWriteItem{Put: put}
}

func (t *dynamoTransaction) deleteRecordItem(storeName, id string, original *dynamoTransactionRecord) ddbtypes.TransactWriteItem {
	del := &ddbtypes.Delete{
		TableName: &t.store.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
		},
	}
	if original != nil && original.data != nil {
		del.ConditionExpression = aws.String("#data = :old_data")
		del.ExpressionAttributeNames = map[string]string{"#data": attrData}
		del.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{":old_data": cloneDynamoAttribute(original.data)}
	}
	return ddbtypes.TransactWriteItem{Delete: del}
}

func deleteIndexItem(table string, idx indexItem) ddbtypes.TransactWriteItem {
	return ddbtypes.TransactWriteItem{
		Delete: &ddbtypes.Delete{
			TableName: &table,
			Key: map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: idx.pk},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: idx.sk},
			},
		},
	}
}

func cloneDynamoRecord(record *proto.Record) *proto.Record {
	if record == nil {
		return nil
	}
	return gproto.Clone(record).(*proto.Record)
}

func cloneDynamoAttribute(value ddbtypes.AttributeValue) ddbtypes.AttributeValue {
	switch v := value.(type) {
	case *ddbtypes.AttributeValueMemberB:
		return &ddbtypes.AttributeValueMemberB{Value: append([]byte(nil), v.Value...)}
	case *ddbtypes.AttributeValueMemberS:
		return &ddbtypes.AttributeValueMemberS{Value: v.Value}
	default:
		return value
	}
}
