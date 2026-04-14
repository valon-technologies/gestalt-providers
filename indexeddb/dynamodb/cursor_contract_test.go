package dynamodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	contracttest "github.com/valon-technologies/gestalt-providers/indexeddb/contracttest"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if _, err := provider.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name:   "users",
		Schema: uniqueEmailSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	alice, err := gestalt.RecordToProto(map[string]any{
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

	keysResp, err := provider.IndexGetAllKeys(ctx, &proto.IndexQueryRequest{
		Store: "users",
		Index: "by_email",
		Values: []*proto.TypedValue{
			{Kind: &proto.TypedValue_StringValue{StringValue: "alice@test.com"}},
		},
	})
	if err != nil {
		t.Fatalf("IndexGetAllKeys(legacy row): %v", err)
	}
	if len(keysResp.Keys) != 1 || keysResp.Keys[0] != "a" {
		t.Fatalf("IndexGetAllKeys legacy keys = %#v, want %#v", keysResp.Keys, []string{"a"})
	}

	bob, err := gestalt.RecordToProto(map[string]any{
		"id":    "b",
		"name":  "Bob",
		"email": "alice@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto(bob): %v", err)
	}
	_, err = provider.Add(ctx, &proto.RecordRequest{Store: "users", Record: bob})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("Add duplicate error = %s, want %s", got, codes.AlreadyExists)
	}

	aliceUpdated, err := gestalt.RecordToProto(map[string]any{
		"id":    "a",
		"name":  "Alice Updated",
		"email": "alice@test.com",
	})
	if err != nil {
		t.Fatalf("RecordToProto(aliceUpdated): %v", err)
	}
	if _, err := provider.Put(ctx, &proto.RecordRequest{Store: "users", Record: aliceUpdated}); err != nil {
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

	if _, err := provider.Delete(ctx, &proto.ObjectStoreRequest{Store: "users", Id: "a"}); err != nil {
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

func (h *dynamoContractHarness) NewServer(t *testing.T) (proto.IndexedDBServer, func()) {
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

func uniqueEmailSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: 0},
			{Name: "email", Type: 0},
		},
	}
}
