package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	attrPK     = "PK"
	attrSK     = "SK"
	attrData   = "data"
	attrKey    = "key"
	attrRefID  = "ref_id"
	attrSchema = "schema"

	sep    = "\x1f"
	metaPK = "\x1fmeta"
)

type indexDef struct {
	Name    string   `json:"name"`
	KeyPath []string `json:"key_path"`
	Unique  bool     `json:"unique"`
}

type sentinelStatusError struct {
	sentinel error
	code     codes.Code
	message  string
}

func (e *sentinelStatusError) Error() string {
	return e.GRPCStatus().Err().Error()
}

func (e *sentinelStatusError) Unwrap() error {
	return e.sentinel
}

func (e *sentinelStatusError) GRPCStatus() *status.Status {
	return status.New(e.code, e.message)
}

func alreadyExistsErrorf(format string, args ...any) error {
	return &sentinelStatusError{
		sentinel: gestalt.ErrAlreadyExists,
		code:     codes.AlreadyExists,
		message:  fmt.Sprintf(format, args...),
	}
}

type storedSchema struct {
	Indexes []indexDef `json:"indexes"`
}

type store struct {
	client  *dynamodb.Client
	table   string
	mu      sync.RWMutex
	schemas map[string]*storedSchema
}

func (s *store) ensureTable(ctx context.Context) error {
	_, err := s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &s.table,
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		var inUse *ddbtypes.ResourceInUseException
		if errors.As(err, &inUse) {
			return nil
		}
		return err
	}
	return s.waitForTable(ctx)
}

func (s *store) waitForTable(ctx context.Context) error {
	for range 60 {
		resp, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &s.table})
		if err != nil {
			return err
		}
		if resp.Table != nil && resp.Table.TableStatus == ddbtypes.TableStatusActive {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("dynamodb: table did not become active within 60s")
}

func (s *store) healthCheck(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &s.table})
	return err
}

func (s *store) loadSchemas(ctx context.Context) error {
	schemas := make(map[string]*storedSchema)
	var startKey map[string]ddbtypes.AttributeValue

	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": &ddbtypes.AttributeValueMemberS{Value: metaPK},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return err
		}
		for _, item := range resp.Items {
			name := getS(item, attrSK)
			raw := getS(item, attrSchema)
			var sc storedSchema
			if err := json.Unmarshal([]byte(raw), &sc); err != nil {
				continue
			}
			schemas[name] = &sc
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}

	s.mu.Lock()
	s.schemas = schemas
	s.mu.Unlock()
	return nil
}

func (s *store) getSchema(name string) (*storedSchema, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sc, ok := s.schemas[name]
	return sc, ok
}

func (s *store) getIndexDef(storeName, indexName string) (*indexDef, error) {
	sc, ok := s.getSchema(storeName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "store %q not found", storeName)
	}
	for i := range sc.Indexes {
		if sc.Indexes[i].Name == indexName {
			return &sc.Indexes[i], nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "index %q not found on store %q", indexName, storeName)
}

func (p *providerCore) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	st := p.store
	st.mu.Lock()
	defer st.mu.Unlock()

	sc := &storedSchema{}
	for _, idx := range schema.Indexes {
		sc.Indexes = append(sc.Indexes, indexDef{
			Name:    idx.Name,
			KeyPath: append([]string(nil), idx.KeyPath...),
			Unique:  idx.Unique,
		})
	}

	raw, _ := json.Marshal(sc)
	_, err := st.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &st.table,
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:     &ddbtypes.AttributeValueMemberS{Value: metaPK},
			attrSK:     &ddbtypes.AttributeValueMemberS{Value: name},
			attrSchema: &ddbtypes.AttributeValueMemberS{Value: string(raw)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		if isConditionFailed(err) {
			return alreadyExistsErrorf("object store %s already exists", name)
		}
		return wrapErr(err)
	}

	st.schemas[name] = sc
	return nil
}

func (p *providerCore) DeleteObjectStore(ctx context.Context, name string) error {
	st := p.store
	st.mu.Lock()
	defer st.mu.Unlock()

	sc := st.schemas[name]
	delete(st.schemas, name)

	_, _ = st.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &st.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: metaPK},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: name},
		},
	})

	prefixes := []string{name}
	if sc != nil {
		for _, idx := range sc.Indexes {
			prefixes = append(prefixes, indexPK(name, idx.Name))
		}
	}
	for _, prefix := range prefixes {
		if err := st.deleteAllInPartition(ctx, prefix); err != nil {
			return wrapErr(err)
		}
	}
	return nil
}

func (p *providerCore) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	record, err := p.store.getRecord(ctx, req.Store, req.ID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return record, nil
}

func (p *providerCore) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	record, err := p.store.getRecord(ctx, req.Store, req.ID)
	if err != nil {
		return "", err
	}
	if record == nil {
		return "", status.Error(codes.NotFound, "record not found")
	}
	return req.ID, nil
}

func (p *providerCore) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	st := p.store
	id, err := extractID(req.Record)
	if err != nil {
		return err
	}
	data, err := marshalRecord(req.Record)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	idxItems := st.buildIndexItems(req.Store, id, req.Record)
	if conflict, err := st.hasUniqueIndexConflict(ctx, idxItems); err != nil {
		return err
	} else if conflict {
		return alreadyExistsErrorf("record %s violates a unique index", id)
	}

	items := []ddbtypes.TransactWriteItem{
		{Put: &ddbtypes.Put{
			TableName: &st.table,
			Item: map[string]ddbtypes.AttributeValue{
				attrPK:   &ddbtypes.AttributeValueMemberS{Value: req.Store},
				attrSK:   &ddbtypes.AttributeValueMemberS{Value: id},
				attrData: &ddbtypes.AttributeValueMemberB{Value: data},
			},
			ConditionExpression: aws.String("attribute_not_exists(PK)"),
		}},
	}
	for _, idx := range idxItems {
		items = append(items, ddbtypes.TransactWriteItem{Put: st.indexPutItem(idx, data, false)})
	}

	_, err = st.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err != nil {
		if isConditionFailed(err) {
			return alreadyExistsErrorf("record %s already exists", id)
		}
		return wrapErr(err)
	}
	if err := st.deleteLegacyUniqueIndexItems(ctx, idxItems); err != nil {
		return err
	}
	return nil
}

func (p *providerCore) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	st := p.store
	id, err := extractID(req.Record)
	if err != nil {
		return err
	}
	data, err := marshalRecord(req.Record)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	old, _ := st.getRecord(ctx, req.Store, id)
	oldIdxItems := st.buildIndexItems(req.Store, id, old)
	idxItems := st.buildIndexItems(req.Store, id, req.Record)
	if conflict, err := st.hasUniqueIndexConflict(ctx, idxItems); err != nil {
		return err
	} else if conflict {
		return alreadyExistsErrorf("record %s violates a unique index", id)
	}

	items := []ddbtypes.TransactWriteItem{
		{Put: &ddbtypes.Put{
			TableName: &st.table,
			Item: map[string]ddbtypes.AttributeValue{
				attrPK:   &ddbtypes.AttributeValueMemberS{Value: req.Store},
				attrSK:   &ddbtypes.AttributeValueMemberS{Value: id},
				attrData: &ddbtypes.AttributeValueMemberB{Value: data},
			},
		}},
	}

	newKeys := make(map[string]struct{}, len(idxItems))
	for _, idx := range idxItems {
		newKeys[idx.key()] = struct{}{}
		items = append(items, ddbtypes.TransactWriteItem{Put: st.indexPutItem(idx, data, true)})
	}
	for _, idx := range oldIdxItems {
		if _, ok := newKeys[idx.key()]; ok {
			continue
		}
		items = append(items, ddbtypes.TransactWriteItem{
			Delete: &ddbtypes.Delete{
				TableName: &st.table,
				Key: map[string]ddbtypes.AttributeValue{
					attrPK: &ddbtypes.AttributeValueMemberS{Value: idx.pk},
					attrSK: &ddbtypes.AttributeValueMemberS{Value: idx.sk},
				},
			},
		})
	}

	_, err = st.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err != nil {
		if isConditionFailed(err) {
			return alreadyExistsErrorf("record %s violates a unique index", id)
		}
		return wrapErr(err)
	}
	if err := st.deleteLegacyUniqueIndexItems(ctx, append(oldIdxItems, idxItems...)); err != nil {
		return err
	}
	return nil
}

func (p *providerCore) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	if err := p.store.deleteRecordByID(ctx, req.Store, req.ID); err != nil {
		return err
	}
	return nil
}

func (p *providerCore) Clear(ctx context.Context, storeName string) error {
	st := p.store
	prefixes := []string{storeName}
	if sc, ok := st.getSchema(storeName); ok {
		for _, idx := range sc.Indexes {
			prefixes = append(prefixes, indexPK(storeName, idx.Name))
		}
	}
	for _, prefix := range prefixes {
		if err := st.deleteAllInPartition(ctx, prefix); err != nil {
			return wrapErr(err)
		}
	}
	return nil
}

func (p *providerCore) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	records, err := p.store.queryRecords(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	return records, nil
}

func (p *providerCore) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	keys, err := p.store.queryKeys(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	return keys, nil
}

func (p *providerCore) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	count, err := p.store.queryCount(ctx, req.Store, req.Range)
	if err != nil {
		return 0, wrapErr(err)
	}
	return count, nil
}

func (p *providerCore) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	st := p.store
	keys, err := st.queryKeys(ctx, req.Store, req.Range)
	if err != nil {
		return 0, wrapErr(err)
	}
	var deleted int64
	for _, id := range keys {
		if err := st.deleteRecordByID(ctx, req.Store, id); err != nil {
			return 0, err
		}
		deleted++
	}
	return deleted, nil
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
		return "", status.Error(codes.NotFound, "record not found")
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
	st := p.store
	entries, err := p.queryIndexKeyEntries(ctx, req)
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, entry := range entries {
		if err := st.deleteRecordByID(ctx, req.Store, entry.PrimaryKey); err != nil {
			return 0, err
		}
		deleted++
	}
	return deleted, nil
}

func (p *providerCore) queryIndexEntries(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]cursorutil.Entry, error) {
	st := p.store
	index, err := st.getIndexDef(req.Store, req.Index)
	if err != nil {
		return nil, err
	}

	records, err := st.queryIndex(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}

	rangeCursor := &dynamoCursor{
		LazyCursor: cursorutil.LazyCursor{Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}}},
		index:      index,
	}
	entries := make([]cursorutil.Entry, 0, len(records))
	for _, record := range records {
		entry, err := rangeCursor.entryFromRecord(record)
		if err != nil {
			if errors.Is(err, errDynamoCursorFieldMissing) {
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
	sortDynamoIndexEntries(entries)
	return entries, nil
}

func (p *providerCore) queryIndexKeyEntries(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]cursorutil.Entry, error) {
	st := p.store
	index, err := st.getIndexDef(req.Store, req.Index)
	if err != nil {
		return nil, err
	}

	entries, err := st.queryIndexKeyEntries(ctx, req.Store, req.Index, req.Values, index)
	if err != nil {
		return nil, err
	}

	rangeCursor := &dynamoCursor{
		LazyCursor: cursorutil.LazyCursor{Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}}},
		index:      index,
	}
	entries, err = rangeCursor.ApplyRange(entries, req.Range)
	if err != nil {
		return nil, err
	}
	sortDynamoIndexEntries(entries)
	return entries, nil
}

func sortDynamoIndexEntries(entries []cursorutil.Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := cursorutil.CompareValues(entries[i].Key, entries[j].Key); cmp != 0 {
			return cmp < 0
		}
		return entries[i].PrimaryKey < entries[j].PrimaryKey
	})
}

type indexItem struct {
	pk     string
	sk     string
	refID  string
	keyRaw []byte
	unique bool
}

func (i indexItem) key() string {
	return i.pk + sep + i.sk
}

func (s *store) indexPutItem(idx indexItem, data []byte, allowSameRefID bool) *ddbtypes.Put {
	put := &ddbtypes.Put{
		TableName: &s.table,
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:    &ddbtypes.AttributeValueMemberS{Value: idx.pk},
			attrSK:    &ddbtypes.AttributeValueMemberS{Value: idx.sk},
			attrData:  &ddbtypes.AttributeValueMemberB{Value: data},
			attrKey:   &ddbtypes.AttributeValueMemberB{Value: idx.keyRaw},
			attrRefID: &ddbtypes.AttributeValueMemberS{Value: idx.refID},
		},
	}
	if idx.unique {
		if allowSameRefID {
			put.ConditionExpression = aws.String("attribute_not_exists(PK) OR ref_id = :ref_id")
			put.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{
				":ref_id": &ddbtypes.AttributeValueMemberS{Value: idx.refID},
			}
		} else {
			put.ConditionExpression = aws.String("attribute_not_exists(PK)")
		}
	}
	return put
}

func (s *store) getRecord(ctx context.Context, storeName, id string) (gestalt.Record, error) {
	_, record, err := s.getRecordItem(ctx, storeName, id)
	return record, err
}

func (s *store) getRecordItem(ctx context.Context, storeName, id string) (map[string]ddbtypes.AttributeValue, gestalt.Record, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return nil, nil, wrapErr(err)
	}
	if resp.Item == nil {
		return nil, nil, nil
	}
	record, err := parseData(resp.Item)
	return resp.Item, record, err
}

func (s *store) deleteRecord(ctx context.Context, storeName, id string, idxItems []indexItem) error {
	if len(idxItems) == 0 {
		_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &s.table,
			Key: map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
			},
		})
		if err != nil {
			return wrapErr(err)
		}
		return nil
	}
	if len(idxItems)+1 > 25 {
		ddbItems := make([]map[string]ddbtypes.AttributeValue, len(idxItems))
		for i, idx := range idxItems {
			ddbItems[i] = map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: idx.pk},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: idx.sk},
			}
		}
		if err := s.batchDelete(ctx, ddbItems); err != nil {
			return err
		}
		_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &s.table,
			Key: map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
			},
		})
		if err != nil {
			return wrapErr(err)
		}
		return nil
	}
	items := []ddbtypes.TransactWriteItem{
		{Delete: &ddbtypes.Delete{
			TableName: &s.table,
			Key: map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
			},
		}},
	}
	for _, idx := range idxItems {
		items = append(items, ddbtypes.TransactWriteItem{
			Delete: &ddbtypes.Delete{
				TableName: &s.table,
				Key: map[string]ddbtypes.AttributeValue{
					attrPK: &ddbtypes.AttributeValueMemberS{Value: idx.pk},
					attrSK: &ddbtypes.AttributeValueMemberS{Value: idx.sk},
				},
			},
		})
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err != nil {
		return wrapErr(err)
	}
	return nil
}

func (s *store) deleteIndexItems(ctx context.Context, items []indexItem) {
	if len(items) == 0 {
		return
	}
	ddbItems := make([]map[string]ddbtypes.AttributeValue, len(items))
	for i, idx := range items {
		ddbItems[i] = map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: idx.pk},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: idx.sk},
		}
	}
	s.batchDelete(ctx, ddbItems)
}

func (s *store) buildIndexItems(storeName, id string, record gestalt.Record) []indexItem {
	if record == nil {
		return nil
	}
	sc, ok := s.getSchema(storeName)
	if !ok {
		return nil
	}
	return buildIndexItemsForSchema(storeName, id, record, sc)
}

func buildIndexItemsForSchema(storeName, id string, record gestalt.Record, sc *storedSchema) []indexItem {
	if record == nil || sc == nil {
		return nil
	}
	var items []indexItem
	for _, idx := range sc.Indexes {
		vals := make([]string, len(idx.KeyPath))
		keyValues := make([]any, len(idx.KeyPath))
		missingField := false
		for i, field := range idx.KeyPath {
			value, ok := record[field]
			if !ok || value == nil {
				missingField = true
				break
			}
			vals[i] = keyStringFromAny(value)
			keyValues[i] = value
		}
		if missingField {
			continue
		}
		keyRaw, err := marshalIndexKey(keyValues)
		if err != nil {
			continue
		}
		sk := indexSK(vals, id)
		if idx.Unique {
			sk = indexUniqueSK(vals)
		}
		items = append(items, indexItem{
			pk:     indexPK(storeName, idx.Name),
			sk:     sk,
			refID:  id,
			keyRaw: keyRaw,
			unique: idx.Unique,
		})
	}
	return items
}

func (s *store) queryRecords(ctx context.Context, storeName string, kr *gestalt.KeyRange) ([]gestalt.Record, error) {
	cond, vals := buildKeyCondition(storeName, kr)
	var records []gestalt.Record
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    &cond,
			ExpressionAttributeValues: vals,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			if rec, err := parseData(item); err == nil && rec != nil {
				records = append(records, rec)
			}
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return records, nil
}

func (s *store) queryKeys(ctx context.Context, storeName string, kr *gestalt.KeyRange) ([]string, error) {
	cond, vals := buildKeyCondition(storeName, kr)
	var keys []string
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    &cond,
			ExpressionAttributeValues: vals,
			ExclusiveStartKey:         startKey,
			ProjectionExpression:      aws.String(attrSK),
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			keys = append(keys, getS(item, attrSK))
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return keys, nil
}

func (s *store) queryCount(ctx context.Context, storeName string, kr *gestalt.KeyRange) (int64, error) {
	cond, vals := buildKeyCondition(storeName, kr)
	var total int64
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    &cond,
			ExpressionAttributeValues: vals,
			ExclusiveStartKey:         startKey,
			Select:                    ddbtypes.SelectCount,
		})
		if err != nil {
			return 0, wrapErr(err)
		}
		total += int64(resp.Count)
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return total, nil
}

func (s *store) queryIndex(ctx context.Context, storeName, indexName string, values []any) ([]gestalt.Record, error) {
	cond, exprVals := buildIndexCondition(storeName, indexName, values)
	var records []gestalt.Record
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    aws.String(cond),
			ExpressionAttributeValues: exprVals,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			if rec, err := parseData(item); err == nil && rec != nil {
				records = append(records, rec)
			}
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return records, nil
}

func (s *store) queryIndexKeyEntries(ctx context.Context, storeName, indexName string, values []any, index *indexDef) ([]cursorutil.Entry, error) {
	cond, exprVals := buildIndexCondition(storeName, indexName, values)
	var entries []cursorutil.Entry
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
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
			key, err := dynamoIndexKeyFromItem(item, len(index.KeyPath))
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

func (s *store) queryIndexKeys(ctx context.Context, storeName, indexName string, values []any) ([]string, error) {
	cond, exprVals := buildIndexCondition(storeName, indexName, values)
	var keys []string
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    aws.String(cond),
			ExpressionAttributeValues: exprVals,
			ExclusiveStartKey:         startKey,
			ProjectionExpression:      aws.String(attrRefID),
		})
		if err != nil {
			return nil, wrapErr(err)
		}
		for _, item := range resp.Items {
			keys = append(keys, getS(item, attrRefID))
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return keys, nil
}

func (s *store) queryIndexCount(ctx context.Context, storeName, indexName string, values []any) (int64, error) {
	cond, exprVals := buildIndexCondition(storeName, indexName, values)
	var total int64
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.table,
			KeyConditionExpression:    aws.String(cond),
			ExpressionAttributeValues: exprVals,
			ExclusiveStartKey:         startKey,
			Select:                    ddbtypes.SelectCount,
		})
		if err != nil {
			return 0, wrapErr(err)
		}
		total += int64(resp.Count)
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return total, nil
}

func (s *store) deleteRecordByID(ctx context.Context, storeName, id string) error {
	item, record, err := s.getRecordItem(ctx, storeName, id)
	if err == nil {
		if item != nil && shouldFallbackDeleteByRefID(item, record) {
			return s.deleteRecordByRefID(ctx, storeName, id)
		}
		return s.deleteRecord(ctx, storeName, id, s.buildIndexItems(storeName, id, record))
	}
	if item == nil {
		return err
	}
	return s.deleteRecordByRefID(ctx, storeName, id)
}

func shouldFallbackDeleteByRefID(item map[string]ddbtypes.AttributeValue, record gestalt.Record) bool {
	if item == nil || record == nil {
		return true
	}
	if raw, ok := item[attrData].(*ddbtypes.AttributeValueMemberB); ok {
		return len(raw.Value) == 0
	}
	if raw, ok := item[attrData].(*ddbtypes.AttributeValueMemberS); ok {
		return raw.Value == ""
	}
	return false
}

func (s *store) deleteRecordByRefID(ctx context.Context, storeName, id string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return wrapErr(err)
	}

	sc, ok := s.getSchema(storeName)
	if !ok {
		return nil
	}

	var items []map[string]ddbtypes.AttributeValue
	for _, idx := range sc.Indexes {
		var startKey map[string]ddbtypes.AttributeValue
		for {
			resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
				TableName:              &s.table,
				KeyConditionExpression: aws.String("PK = :pk"),
				ConsistentRead:         aws.Bool(true),
				FilterExpression:       aws.String("ref_id = :ref_id"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":pk":     &ddbtypes.AttributeValueMemberS{Value: indexPK(storeName, idx.Name)},
					":ref_id": &ddbtypes.AttributeValueMemberS{Value: id},
				},
				ProjectionExpression: aws.String("PK, SK"),
				ExclusiveStartKey:    startKey,
			})
			if err != nil {
				return wrapErr(err)
			}
			items = append(items, resp.Items...)
			if resp.LastEvaluatedKey == nil {
				break
			}
			startKey = resp.LastEvaluatedKey
		}
	}
	return s.batchDelete(ctx, items)
}

func (s *store) deleteAllInPartition(ctx context.Context, pk string) error {
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk": &ddbtypes.AttributeValueMemberS{Value: pk},
			},
			ProjectionExpression: aws.String("PK, SK"),
			ExclusiveStartKey:    startKey,
		})
		if err != nil {
			return err
		}
		if err := s.batchDelete(ctx, resp.Items); err != nil {
			return err
		}
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return nil
}

func (s *store) batchDelete(ctx context.Context, items []map[string]ddbtypes.AttributeValue) error {
	for i := 0; i < len(items); i += 25 {
		end := min(i+25, len(items))
		var reqs []ddbtypes.WriteRequest
		for _, item := range items[i:end] {
			reqs = append(reqs, ddbtypes.WriteRequest{
				DeleteRequest: &ddbtypes.DeleteRequest{
					Key: map[string]ddbtypes.AttributeValue{attrPK: item[attrPK], attrSK: item[attrSK]},
				},
			})
		}
		requestItems := map[string][]ddbtypes.WriteRequest{s.table: reqs}
		for attempt := 0; attempt < 5; attempt++ {
			resp, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: requestItems,
			})
			if err != nil {
				return wrapErr(err)
			}
			unprocessed := resp.UnprocessedItems[s.table]
			if len(unprocessed) == 0 {
				requestItems = nil
				break
			}
			requestItems = map[string][]ddbtypes.WriteRequest{s.table: unprocessed}
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
		if len(requestItems[s.table]) > 0 {
			return status.Errorf(codes.Internal, "dynamodb: unprocessed delete items: %d", len(requestItems[s.table]))
		}
	}
	return nil
}

func indexPK(storeName, indexName string) string {
	return storeName + sep + "idx" + sep + indexName
}

func indexSK(values []string, id string) string {
	parts := make([]string, len(values)+1)
	copy(parts, values)
	parts[len(values)] = id
	return strings.Join(parts, sep)
}

func indexUniqueSK(values []string) string {
	return indexSKPrefix(values)
}

func indexSKPrefix(values []string) string {
	return strings.Join(values, sep) + sep
}

func (s *store) hasUniqueIndexConflict(ctx context.Context, items []indexItem) (bool, error) {
	for _, idx := range items {
		if !idx.unique {
			continue
		}

		var startKey map[string]ddbtypes.AttributeValue
		for {
			resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
				TableName:              &s.table,
				KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skp)"),
				ConsistentRead:         aws.Bool(true),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":pk":  &ddbtypes.AttributeValueMemberS{Value: idx.pk},
					":skp": &ddbtypes.AttributeValueMemberS{Value: idx.sk},
				},
				ProjectionExpression: aws.String(attrRefID),
				ExclusiveStartKey:    startKey,
			})
			if err != nil {
				return false, wrapErr(err)
			}
			for _, item := range resp.Items {
				if refID := getS(item, attrRefID); refID != "" && refID != idx.refID {
					return true, nil
				}
			}
			if resp.LastEvaluatedKey == nil {
				break
			}
			startKey = resp.LastEvaluatedKey
		}
	}
	return false, nil
}

func (s *store) deleteLegacyUniqueIndexItems(ctx context.Context, items []indexItem) error {
	seen := make(map[string]map[string]ddbtypes.AttributeValue)
	for _, idx := range items {
		if !idx.unique {
			continue
		}

		var startKey map[string]ddbtypes.AttributeValue
		for {
			resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
				TableName:              &s.table,
				KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skp)"),
				ConsistentRead:         aws.Bool(true),
				FilterExpression:       aws.String("ref_id = :ref_id"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":pk":     &ddbtypes.AttributeValueMemberS{Value: idx.pk},
					":skp":    &ddbtypes.AttributeValueMemberS{Value: idx.sk},
					":ref_id": &ddbtypes.AttributeValueMemberS{Value: idx.refID},
				},
				ProjectionExpression: aws.String("PK, SK"),
				ExclusiveStartKey:    startKey,
			})
			if err != nil {
				return wrapErr(err)
			}
			for _, item := range resp.Items {
				if getS(item, attrSK) == idx.sk {
					continue
				}
				seen[getS(item, attrPK)+sep+getS(item, attrSK)] = item
			}
			if resp.LastEvaluatedKey == nil {
				break
			}
			startKey = resp.LastEvaluatedKey
		}
	}

	toDelete := make([]map[string]ddbtypes.AttributeValue, 0, len(seen))
	for _, item := range seen {
		toDelete = append(toDelete, item)
	}
	return s.batchDelete(ctx, toDelete)
}

func marshalIndexKey(values []any) ([]byte, error) {
	return gestalt.EncodeIndexedDBIndexValues(values)
}

func unmarshalIndexKey(raw []byte, keyParts int) ([]any, error) {
	return gestalt.DecodeIndexedDBIndexValues(raw, keyParts)
}

func buildIndexCondition(storeName, indexName string, values []any) (string, map[string]ddbtypes.AttributeValue) {
	exprVals := map[string]ddbtypes.AttributeValue{
		":pk": &ddbtypes.AttributeValueMemberS{Value: indexPK(storeName, indexName)},
	}
	if len(values) == 0 {
		return "PK = :pk", exprVals
	}
	exprVals[":skp"] = &ddbtypes.AttributeValueMemberS{Value: indexSKPrefix(valuesToStrings(values))}
	return "PK = :pk AND begins_with(SK, :skp)", exprVals
}

func buildKeyCondition(storeName string, kr *gestalt.KeyRange) (string, map[string]ddbtypes.AttributeValue) {
	vals := map[string]ddbtypes.AttributeValue{
		":pk": &ddbtypes.AttributeValueMemberS{Value: storeName},
	}
	if kr == nil || (kr.Lower == nil && kr.Upper == nil) {
		return "PK = :pk", vals
	}
	lower := valueToString(kr.Lower)
	upper := valueToString(kr.Upper)

	switch {
	case lower != "" && upper != "" && lower == upper && !kr.LowerOpen && !kr.UpperOpen:
		vals[":sk"] = &ddbtypes.AttributeValueMemberS{Value: lower}
		return "PK = :pk AND SK = :sk", vals
	case lower != "" && upper != "":
		vals[":sk_lo"] = &ddbtypes.AttributeValueMemberS{Value: lower}
		vals[":sk_hi"] = &ddbtypes.AttributeValueMemberS{Value: upper}
		return "PK = :pk AND SK BETWEEN :sk_lo AND :sk_hi", vals
	case lower != "":
		vals[":sk"] = &ddbtypes.AttributeValueMemberS{Value: lower}
		if kr.LowerOpen {
			return "PK = :pk AND SK > :sk", vals
		}
		return "PK = :pk AND SK >= :sk", vals
	default:
		vals[":sk"] = &ddbtypes.AttributeValueMemberS{Value: upper}
		if kr.UpperOpen {
			return "PK = :pk AND SK < :sk", vals
		}
		return "PK = :pk AND SK <= :sk", vals
	}
}

func getS(item map[string]ddbtypes.AttributeValue, key string) string {
	if v, ok := item[key]; ok {
		if sv, ok := v.(*ddbtypes.AttributeValueMemberS); ok {
			return sv.Value
		}
	}
	return ""
}

func parseData(item map[string]ddbtypes.AttributeValue) (gestalt.Record, error) {
	if raw, ok := item[attrData].(*ddbtypes.AttributeValueMemberB); ok {
		return gestalt.DecodeIndexedDBRecord(raw.Value)
	}
	raw := getS(item, attrData)
	if raw == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return gestalt.Record(m), nil
}

func extractID(record gestalt.Record) (string, error) {
	if record == nil {
		return "", status.Error(codes.InvalidArgument, "record is required")
	}
	value, ok := record["id"]
	if !ok {
		return "", status.Error(codes.InvalidArgument, "record must contain an \"id\" field")
	}
	id, ok := value.(string)
	if !ok || id == "" {
		return "", status.Error(codes.InvalidArgument, "record \"id\" must be a non-empty string")
	}
	return id, nil
}

func valueToString(v any) string {
	if v == nil {
		return ""
	}
	return keyStringFromAny(v)
}

func valuesToStrings(values []any) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = valueToString(v)
	}
	return out
}

func wrapErr(err error) error {
	return status.Errorf(codes.Internal, "dynamodb: %v", err)
}

func isConditionFailed(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ConditionalCheckFailed") || strings.Contains(msg, "conditional request failed")
}

func marshalRecord(record gestalt.Record) ([]byte, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	return gestalt.EncodeIndexedDBRecord(record)
}

func keyStringFromAny(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		return fmt.Sprintf("%t", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(raw)
	}
}
