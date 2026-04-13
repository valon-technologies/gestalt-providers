package mongodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	contracttest "github.com/valon-technologies/gestalt-providers/indexeddb/contracttest"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoContractHarness struct {
	uri      string
	database string
	client   *mongo.Client
}

func TestCursorContract(t *testing.T) {
	uri := os.Getenv("GESTALT_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("GESTALT_TEST_MONGODB_URI is not set")
	}

	harness := newMongoContractHarness(t, uri)
	contracttest.Run(t, harness)
}

func newMongoContractHarness(t *testing.T, uri string) contracttest.Harness {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("mongo.Ping: %v", err)
	}

	harness := &mongoContractHarness{
		uri:      uri,
		database: fmt.Sprintf("gestalt_contract_%d", time.Now().UnixNano()),
		client:   client,
	}
	t.Cleanup(func() {
		_ = client.Database(harness.database).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	return harness
}

func (h *mongoContractHarness) Name() string {
	return "MongoDB"
}

func (h *mongoContractHarness) Capabilities() contracttest.Capabilities {
	return contracttest.Capabilities{
		TypedPrimaryKeys:     true,
		NestedIndexPaths:     true,
		UnreadablePayloadRow: true,
	}
}

func (h *mongoContractHarness) NewServer(t *testing.T) (proto.IndexedDBServer, func()) {
	t.Helper()

	provider := New()
	if err := provider.Configure(context.Background(), "", map[string]any{
		"uri":      h.uri,
		"database": h.database,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}

	return provider, func() {
		_ = provider.Close()
	}
}

func (h *mongoContractHarness) InsertUnreadablePayloadRow(t *testing.T, storeName, id, status string) {
	t.Helper()

	if _, err := h.client.Database(h.database).Collection(storeName).InsertOne(context.Background(), bson.M{
		"_id":    id,
		"status": status,
		"payload": bson.Regex{
			Pattern: ".*",
			Options: "i",
		},
	}); err != nil {
		t.Fatalf("InsertOne(unreadable payload): %v", err)
	}
}
