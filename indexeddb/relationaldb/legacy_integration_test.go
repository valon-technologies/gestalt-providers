package relationaldb

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLegacyV1StoreRemainsReadableOverTransport(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy-v1.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(metadataTableSQL(dialectSQLite, metadataTableName)); err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	if _, err := db.Exec(createTableSQL(dialectSQLite, "widgets", widgetsSchema())); err != nil {
		t.Fatalf("create widgets table: %v", err)
	}
	for _, idx := range widgetsSchema().GetIndexes() {
		if _, err := db.Exec(createIndexSQL(dialectSQLite, "widgets", idx, widgetsSchema())); err != nil {
			t.Fatalf("create widgets index %q: %v", idx.GetName(), err)
		}
	}

	legacySchema := newStoredSchema("widgets", widgetsSchema(), storageVersionLegacy)
	legacySchema.StorageVersion = 0
	schemaJSON, err := json.Marshal(legacySchema)
	if err != nil {
		t.Fatalf("marshal legacy schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "_gestalt_stores" ("name", "schema_json") VALUES (?, ?)`, "widgets", string(schemaJSON)); err != nil {
		t.Fatalf("insert legacy metadata: %v", err)
	}

	record, err := gestalt.RecordToProto(map[string]any{
		"id":         "w1",
		"code":       "W-001",
		"title":      "Legacy Widget",
		"created_at": time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC),
		"updated_at": time.Date(2024, time.January, 2, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("RecordToProto: %v", err)
	}
	values, err := recordToArgs(record, widgetsSchema().GetColumns())
	if err != nil {
		t.Fatalf("recordToArgs: %v", err)
	}
	if _, err := db.Exec(insertSQL(dialectSQLite, "widgets", widgetsSchema().GetColumns()), values...); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	client := newIndexedDBTestClient(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name:   "widgets",
		Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore(existing legacy): %v", err)
	}

	meta, err := store.getMeta("widgets")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if usesGenericStorage(meta) {
		t.Fatal("legacy store unexpectedly switched to generic storage")
	}

	resp, err := client.Get(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if err != nil {
		t.Fatalf("Get legacy row: %v", err)
	}
	if got := resp.GetRecord().GetFields()["title"].GetStringValue(); got != "Legacy Widget" {
		t.Fatalf("legacy title = %q, want %q", got, "Legacy Widget")
	}

	indexResp, err := client.IndexGetAll(ctx, &proto.IndexQueryRequest{
		Store: "widgets",
		Index: "by_code",
		Values: []*proto.TypedValue{
			{Kind: &proto.TypedValue_StringValue{StringValue: "W-001"}},
		},
	})
	if err != nil {
		t.Fatalf("IndexGetAll legacy row: %v", err)
	}
	if len(indexResp.GetRecords()) != 1 {
		t.Fatalf("IndexGetAll legacy count = %d, want 1", len(indexResp.GetRecords()))
	}

	second, err := gestalt.RecordToProto(map[string]any{
		"id":         "w2",
		"code":       "W-002",
		"title":      "New Widget",
		"created_at": time.Date(2024, time.January, 3, 12, 0, 0, 0, time.UTC),
		"updated_at": time.Date(2024, time.January, 4, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("RecordToProto(second): %v", err)
	}
	if _, err := client.Add(ctx, &proto.RecordRequest{Store: "widgets", Record: second}); err != nil {
		t.Fatalf("Add second legacy row: %v", err)
	}

	allResp, err := client.GetAll(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if err != nil {
		t.Fatalf("GetAll legacy rows: %v", err)
	}
	if len(allResp.GetRecords()) != 2 {
		t.Fatalf("GetAll legacy count = %d, want 2", len(allResp.GetRecords()))
	}
}

func newIndexedDBTestClient(t *testing.T, serverImpl proto.IndexedDBServer) proto.IndexedDBClient {
	t.Helper()

	listener := bufconn.Listen(cursorTestBufSize)
	server := grpc.NewServer()
	proto.RegisterIndexedDBServer(server, serverImpl)
	go func() {
		_ = server.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	return proto.NewIndexedDBClient(conn)
}
