package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	attrPK     = "PK"
	attrSK     = "SK"
	attrData   = "data"
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

func (p *Provider) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	st := p.store
	sc := &storedSchema{}
	if req.Schema != nil {
		for _, idx := range req.Schema.Indexes {
			sc.Indexes = append(sc.Indexes, indexDef{
				Name:    idx.Name,
				KeyPath: idx.KeyPath,
				Unique:  idx.Unique,
			})
		}
	}

	raw, _ := json.Marshal(sc)
	_, err := st.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &st.table,
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:     &ddbtypes.AttributeValueMemberS{Value: metaPK},
			attrSK:     &ddbtypes.AttributeValueMemberS{Value: req.Name},
			attrSchema: &ddbtypes.AttributeValueMemberS{Value: string(raw)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		if isConditionFailed(err) {
			return nil, status.Errorf(codes.AlreadyExists, "object store %s already exists", req.Name)
		}
		return nil, wrapErr(err)
	}

	st.mu.Lock()
	st.schemas[req.Name] = sc
	st.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	st := p.store

	st.mu.Lock()
	sc := st.schemas[req.Name]
	delete(st.schemas, req.Name)
	st.mu.Unlock()

	_, _ = st.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &st.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: metaPK},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: req.Name},
		},
	})

	prefixes := []string{req.Name}
	if sc != nil {
		for _, idx := range sc.Indexes {
			prefixes = append(prefixes, indexPK(req.Name, idx.Name))
		}
	}
	for _, prefix := range prefixes {
		if err := st.deleteAllInPartition(ctx, prefix); err != nil {
			return nil, wrapErr(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	record, err := p.store.getRecord(ctx, req.Store, req.Id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: record}, nil
}

func (p *Provider) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	record, err := p.store.getRecord(ctx, req.Store, req.Id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.KeyResponse{Key: req.Id}, nil
}

func (p *Provider) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	st := p.store
	id, err := extractID(req.Record)
	if err != nil {
		return nil, err
	}
	data, err := marshalRecord(req.Record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	idxItems := st.buildIndexItems(req.Store, id, req.Record)

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
		items = append(items, ddbtypes.TransactWriteItem{
			Put: &ddbtypes.Put{
				TableName: &st.table,
				Item: map[string]ddbtypes.AttributeValue{
					attrPK:    &ddbtypes.AttributeValueMemberS{Value: idx.pk},
					attrSK:    &ddbtypes.AttributeValueMemberS{Value: idx.sk},
					attrData:  &ddbtypes.AttributeValueMemberB{Value: data},
					attrRefID: &ddbtypes.AttributeValueMemberS{Value: id},
				},
			},
		})
	}

	_, err = st.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err != nil {
		if isConditionFailed(err) {
			return nil, status.Errorf(codes.AlreadyExists, "record %s already exists", id)
		}
		return nil, wrapErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	st := p.store
	id, err := extractID(req.Record)
	if err != nil {
		return nil, err
	}
	data, err := marshalRecord(req.Record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}

	if old, _ := st.getRecord(ctx, req.Store, id); old != nil {
		st.deleteIndexItems(ctx, st.buildIndexItems(req.Store, id, old))
	}

	idxItems := st.buildIndexItems(req.Store, id, req.Record)
	if len(idxItems) == 0 {
		_, err = st.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &st.table,
			Item: map[string]ddbtypes.AttributeValue{
				attrPK:   &ddbtypes.AttributeValueMemberS{Value: req.Store},
				attrSK:   &ddbtypes.AttributeValueMemberS{Value: id},
				attrData: &ddbtypes.AttributeValueMemberB{Value: data},
			},
		})
	} else {
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
		for _, idx := range idxItems {
			items = append(items, ddbtypes.TransactWriteItem{
				Put: &ddbtypes.Put{
					TableName: &st.table,
					Item: map[string]ddbtypes.AttributeValue{
						attrPK:    &ddbtypes.AttributeValueMemberS{Value: idx.pk},
						attrSK:    &ddbtypes.AttributeValueMemberS{Value: idx.sk},
						attrData:  &ddbtypes.AttributeValueMemberB{Value: data},
						attrRefID: &ddbtypes.AttributeValueMemberS{Value: id},
					},
				},
			})
		}
		_, err = st.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	}
	if err != nil {
		return nil, wrapErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	st := p.store
	old, _ := st.getRecord(ctx, req.Store, req.Id)
	idxItems := st.buildIndexItems(req.Store, req.Id, old)
	st.deleteRecord(ctx, req.Store, req.Id, idxItems)
	return &emptypb.Empty{}, nil
}

func (p *Provider) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	st := p.store
	prefixes := []string{req.Store}
	if sc, ok := st.getSchema(req.Store); ok {
		for _, idx := range sc.Indexes {
			prefixes = append(prefixes, indexPK(req.Store, idx.Name))
		}
	}
	for _, prefix := range prefixes {
		if err := st.deleteAllInPartition(ctx, prefix); err != nil {
			return nil, wrapErr(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	records, err := p.store.queryRecords(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (p *Provider) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	keys, err := p.store.queryKeys(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (p *Provider) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	count, err := p.store.queryCount(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (p *Provider) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	st := p.store
	records, err := st.queryRecords(ctx, req.Store, req.Range)
	if err != nil {
		return nil, wrapErr(err)
	}
	var deleted int64
	for _, rec := range records {
		id, err := extractID(rec)
		if err != nil {
			continue
		}
		st.deleteRecord(ctx, req.Store, id, st.buildIndexItems(req.Store, id, rec))
		deleted++
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

func (p *Provider) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	records, err := p.store.queryIndex(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: records[0]}, nil
}

func (p *Provider) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	keys, err := p.store.queryIndexKeys(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.KeyResponse{Key: keys[0]}, nil
}

func (p *Provider) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	records, err := p.store.queryIndex(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	keys, err := p.store.queryIndexKeys(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (p *Provider) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	count, err := p.store.queryIndexCount(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: count}, nil
}

func (p *Provider) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	st := p.store
	records, err := st.queryIndex(ctx, req.Store, req.Index, req.Values)
	if err != nil {
		return nil, err
	}
	var deleted int64
	for _, rec := range records {
		id, err := extractID(rec)
		if err != nil {
			continue
		}
		st.deleteRecord(ctx, req.Store, id, st.buildIndexItems(req.Store, id, rec))
		deleted++
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

type indexItem struct{ pk, sk string }

func (s *store) getRecord(ctx context.Context, storeName, id string) (*proto.Record, error) {
	resp, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	if resp.Item == nil {
		return nil, nil
	}
	return parseData(resp.Item)
}

func (s *store) deleteRecord(ctx context.Context, storeName, id string, idxItems []indexItem) {
	if len(idxItems) == 0 {
		_, _ = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: &s.table,
			Key: map[string]ddbtypes.AttributeValue{
				attrPK: &ddbtypes.AttributeValueMemberS{Value: storeName},
				attrSK: &ddbtypes.AttributeValueMemberS{Value: id},
			},
		})
		return
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
	_, _ = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
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

func (s *store) buildIndexItems(storeName, id string, record *proto.Record) []indexItem {
	if record == nil {
		return nil
	}
	sc, ok := s.getSchema(storeName)
	if !ok {
		return nil
	}
	var items []indexItem
	for _, idx := range sc.Indexes {
		vals := make([]string, len(idx.KeyPath))
		for i, field := range idx.KeyPath {
			vals[i] = typedValueKeyString(record.GetFields()[field])
		}
		items = append(items, indexItem{
			pk: indexPK(storeName, idx.Name),
			sk: indexSK(vals, id),
		})
	}
	return items
}

func (s *store) queryRecords(ctx context.Context, storeName string, kr *proto.KeyRange) ([]*proto.Record, error) {
	cond, vals := buildKeyCondition(storeName, kr)
	var records []*proto.Record
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

func (s *store) queryKeys(ctx context.Context, storeName string, kr *proto.KeyRange) ([]string, error) {
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

func (s *store) queryCount(ctx context.Context, storeName string, kr *proto.KeyRange) (int64, error) {
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

func (s *store) queryIndex(ctx context.Context, storeName, indexName string, values []*proto.TypedValue) ([]*proto.Record, error) {
	pk := indexPK(storeName, indexName)
	skp := indexSKPrefix(protoValuesToStrings(values))
	var records []*proto.Record
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skp)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":  &ddbtypes.AttributeValueMemberS{Value: pk},
				":skp": &ddbtypes.AttributeValueMemberS{Value: skp},
			},
			ExclusiveStartKey: startKey,
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

func (s *store) queryIndexKeys(ctx context.Context, storeName, indexName string, values []*proto.TypedValue) ([]string, error) {
	pk := indexPK(storeName, indexName)
	skp := indexSKPrefix(protoValuesToStrings(values))
	var keys []string
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skp)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":  &ddbtypes.AttributeValueMemberS{Value: pk},
				":skp": &ddbtypes.AttributeValueMemberS{Value: skp},
			},
			ExclusiveStartKey:    startKey,
			ProjectionExpression: aws.String(attrRefID),
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

func (s *store) queryIndexCount(ctx context.Context, storeName, indexName string, values []*proto.TypedValue) (int64, error) {
	pk := indexPK(storeName, indexName)
	skp := indexSKPrefix(protoValuesToStrings(values))
	var total int64
	var startKey map[string]ddbtypes.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              &s.table,
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skp)"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":pk":  &ddbtypes.AttributeValueMemberS{Value: pk},
				":skp": &ddbtypes.AttributeValueMemberS{Value: skp},
			},
			ExclusiveStartKey: startKey,
			Select:            ddbtypes.SelectCount,
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
		s.batchDelete(ctx, resp.Items)
		if resp.LastEvaluatedKey == nil {
			break
		}
		startKey = resp.LastEvaluatedKey
	}
	return nil
}

func (s *store) batchDelete(ctx context.Context, items []map[string]ddbtypes.AttributeValue) {
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
		_, _ = s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]ddbtypes.WriteRequest{s.table: reqs},
		})
	}
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

func indexSKPrefix(values []string) string {
	return strings.Join(values, sep) + sep
}

func buildKeyCondition(storeName string, kr *proto.KeyRange) (string, map[string]ddbtypes.AttributeValue) {
	vals := map[string]ddbtypes.AttributeValue{
		":pk": &ddbtypes.AttributeValueMemberS{Value: storeName},
	}
	if kr == nil || (kr.Lower == nil && kr.Upper == nil) {
		return "PK = :pk", vals
	}
	lower := protoValueToString(kr.Lower)
	upper := protoValueToString(kr.Upper)

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

func parseData(item map[string]ddbtypes.AttributeValue) (*proto.Record, error) {
	if raw, ok := item[attrData].(*ddbtypes.AttributeValueMemberB); ok {
		record := &proto.Record{}
		if err := gproto.Unmarshal(raw.Value, record); err != nil {
			return nil, err
		}
		return record, nil
	}
	raw := getS(item, attrData)
	if raw == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return gestalt.RecordToProto(m)
}

func extractID(record *proto.Record) (string, error) {
	if record == nil {
		return "", status.Error(codes.InvalidArgument, "record is required")
	}
	v, ok := record.Fields["id"]
	if !ok {
		return "", status.Error(codes.InvalidArgument, "record must contain an \"id\" field")
	}
	value, err := gestalt.AnyFromTypedValue(v)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "record id: %v", err)
	}
	id, ok := value.(string)
	if id == "" {
		return "", status.Error(codes.InvalidArgument, "record \"id\" must be a non-empty string")
	}
	return id, nil
}

func protoValueToString(v *proto.TypedValue) string {
	if v == nil {
		return ""
	}
	return typedValueKeyString(v)
}

func protoValuesToStrings(values []*proto.TypedValue) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = protoValueToString(v)
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

func marshalRecord(record *proto.Record) ([]byte, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	return gproto.Marshal(record)
}

func typedValueKeyString(value *proto.TypedValue) string {
	goValue, err := gestalt.AnyFromTypedValue(value)
	if err != nil {
		return ""
	}
	return keyStringFromAny(goValue)
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
