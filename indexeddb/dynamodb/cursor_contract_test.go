package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	contracttest "github.com/valon-technologies/gestalt-providers/indexeddb/contracttest"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type dynamoContractHarness struct {
	endpoint string
	region   string
	table    string
	client   *dynamodb.Client
}

func TestCursorContract(t *testing.T) {
	endpoint := os.Getenv("GESTALT_TEST_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("GESTALT_TEST_DYNAMODB_ENDPOINT is not set")
	}

	region := os.Getenv("GESTALT_TEST_DYNAMODB_REGION")
	if region == "" {
		region = "us-east-1"
	}

	harness := newDynamoContractHarness(t, endpoint, region)
	contracttest.Run(t, harness)
}

func TestLegacyUniqueIndexCompatibility(t *testing.T) {
	endpoint := os.Getenv("GESTALT_TEST_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("GESTALT_TEST_DYNAMODB_ENDPOINT is not set")
	}

	region := os.Getenv("GESTALT_TEST_DYNAMODB_REGION")
	if region == "" {
		region = "us-east-1"
	}

	harness := newDynamoContractHarness(t, endpoint, region).(*dynamoContractHarness)
	provider := New()
	if err := provider.Configure(context.Background(), "", map[string]any{
		"endpoint": harness.endpoint,
		"region":   harness.region,
		"table":    harness.table,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	defer provider.Close()

	ctx := context.Background()
	if err := provider.CreateObjectStore(ctx, "users", uniqueEmailSchema()); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	alice, err := gestalt.RecordToProto(gestalt.Record{
		"id":    "a",
		"name":  "Alice",
		"email": "alice@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto(alice): %v", err)
	}
	rawAlice, err := marshalRecord(alice)
	if err != nil {
		t.Fatalf("marshalRecord(alice): %v", err)
	}

	if _, err := harness.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(harness.table),
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:   &ddbtypes.AttributeValueMemberS{Value: "users"},
			attrSK:   &ddbtypes.AttributeValueMemberS{Value: "a"},
			attrData: &ddbtypes.AttributeValueMemberB{Value: rawAlice},
		},
	}); err != nil {
		t.Fatalf("PutItem(store row): %v", err)
	}
	if _, err := harness.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(harness.table),
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:    &ddbtypes.AttributeValueMemberS{Value: indexPK("users", "by_email")},
			attrSK:    &ddbtypes.AttributeValueMemberS{Value: indexSK([]string{"alice@test.com"}, "a")},
			attrData:  &ddbtypes.AttributeValueMemberB{Value: rawAlice},
			attrRefID: &ddbtypes.AttributeValueMemberS{Value: "a"},
		},
	}); err != nil {
		t.Fatalf("PutItem(legacy unique row): %v", err)
	}

	keys, err := provider.IndexGetAllKeys(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  "users",
		Index:  "by_email",
		Values: []any{"alice@test.com"},
	})
	if err != nil {
		t.Fatalf("IndexGetAllKeys(legacy row): %v", err)
	}
	if len(keys) != 1 || keys[0] != "a" {
		t.Fatalf("IndexGetAllKeys legacy keys = %#v, want %#v", keys, []string{"a"})
	}

	bob := gestalt.Record{
		"id":    "b",
		"name":  "Bob",
		"email": "alice@test.com",
	}
	err = provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: bob})
	if !errors.Is(err, gestalt.ErrAlreadyExists) {
		t.Fatalf("Add duplicate error = %v, want ErrAlreadyExists", err)
	}

	aliceUpdated := gestalt.Record{
		"id":    "a",
		"name":  "Alice Updated",
		"email": "alice@test.com",
	}
	if err := provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: aliceUpdated}); err != nil {
		t.Fatalf("Put(aliceUpdated): %v", err)
	}

	indexRows, err := harness.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(harness.table),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: indexPK("users", "by_email")},
		},
		ProjectionExpression: aws.String("PK, SK, ref_id"),
	})
	if err != nil {
		t.Fatalf("Query(index rows): %v", err)
	}
	if len(indexRows.Items) != 1 {
		t.Fatalf("index row count = %d, want 1", len(indexRows.Items))
	}
	if got := getS(indexRows.Items[0], attrSK); got != indexUniqueSK([]string{"alice@test.com"}) {
		t.Fatalf("unique index SK = %q, want %q", got, indexUniqueSK([]string{"alice@test.com"}))
	}
	if got := getS(indexRows.Items[0], attrRefID); got != "a" {
		t.Fatalf("unique index ref_id = %q, want %q", got, "a")
	}

	if err := provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: "users", ID: "a"}); err != nil {
		t.Fatalf("Delete(alice): %v", err)
	}

	indexRows, err = harness.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(harness.table),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: indexPK("users", "by_email")},
		},
		ProjectionExpression: aws.String("PK, SK"),
	})
	if err != nil {
		t.Fatalf("Query(index rows after delete): %v", err)
	}
	if len(indexRows.Items) != 0 {
		t.Fatalf("index rows after delete = %d, want 0", len(indexRows.Items))
	}
}

func newDynamoContractHarness(t *testing.T, endpoint, region string) contracttest.Harness {
	t.Helper()

	client, err := newClient(config{
		Endpoint: endpoint,
		Region:   region,
	})
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}

	harness := &dynamoContractHarness{
		endpoint: endpoint,
		region:   region,
		table:    fmt.Sprintf("gestalt_contract_%d", time.Now().UnixNano()),
		client:   client,
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(harness.table),
		})
	})
	return harness
}

func (h *dynamoContractHarness) Name() string {
	return "DynamoDBLocal"
}

func (h *dynamoContractHarness) Capabilities() contracttest.Capabilities {
	return contracttest.Capabilities{UnreadablePayloadRow: true}
}

func (h *dynamoContractHarness) NewProvider(t *testing.T) (gestalt.IndexedDBProvider, func()) {
	t.Helper()

	provider := New()
	if err := provider.Configure(context.Background(), "", map[string]any{
		"endpoint": h.endpoint,
		"region":   h.region,
		"table":    h.table,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return provider, func() {
		_ = provider.Close()
	}
}

func (h *dynamoContractHarness) InsertUnreadablePayloadRow(t *testing.T, storeName, id, status string) {
	t.Helper()

	raw := []byte{0xff, 0x00, 0x7f}
	if _, err := h.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(h.table),
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:   &ddbtypes.AttributeValueMemberS{Value: storeName},
			attrSK:   &ddbtypes.AttributeValueMemberS{Value: id},
			attrData: &ddbtypes.AttributeValueMemberB{Value: raw},
		},
	}); err != nil {
		t.Fatalf("PutItem(store row): %v", err)
	}

	if _, err := h.client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(h.table),
		Item: map[string]ddbtypes.AttributeValue{
			attrPK:    &ddbtypes.AttributeValueMemberS{Value: indexPK(storeName, "by_status")},
			attrSK:    &ddbtypes.AttributeValueMemberS{Value: indexSK([]string{status}, id)},
			attrData:  &ddbtypes.AttributeValueMemberB{Value: raw},
			attrRefID: &ddbtypes.AttributeValueMemberS{Value: id},
		},
	}); err != nil {
		t.Fatalf("PutItem(index row): %v", err)
	}
}

func uniqueEmailSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: gestalt.TypeString},
			{Name: "email", Type: gestalt.TypeString},
		},
	}
}
