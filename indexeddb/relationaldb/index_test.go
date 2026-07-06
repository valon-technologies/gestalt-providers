package relationaldb

import (
	"context"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func indexTestProvider(t *testing.T) *Provider {
	t.Helper()
	return &Provider{Store: testStore(t)}
}

func indexTestStoreSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true, NotNull: true},
			{Name: "status", Type: gestalt.TypeString},
		},
	}
}

func seedIndexRecords(t *testing.T, p *Provider) {
	t.Helper()
	ctx := context.Background()
	if err := p.CreateObjectStore(ctx, "issues", indexTestStoreSchema()); err != nil {
		t.Fatalf("CreateObjectStore(issues): %v", err)
	}
	for _, record := range []gestalt.Record{
		{"id": "a", "status": "open"},
		{"id": "b", "status": "open"},
		{"id": "c", "status": "closed"},
	} {
		if err := p.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "issues", Record: record}); err != nil {
			t.Fatalf("Add(%q): %v", record["id"], err)
		}
	}
}

func TestCreateIndexBackfillsAndQueries(t *testing.T) {
	p := indexTestProvider(t)
	ctx := context.Background()
	seedIndexRecords(t, p)

	if err := p.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{
		Store:   "issues",
		Name:    "by_status",
		KeyPath: []string{"status"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	open, err := p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store: "issues",
		Index: "by_status",
		Query: indexeddb.ToQuery("open"),
	})
	if err != nil {
		t.Fatalf("IndexGetAll(open): %v", err)
	}
	if got := len(open); got != 2 {
		t.Fatalf("open record count = %d, want 2", got)
	}
}

func TestCreateIndexAlreadyExists(t *testing.T) {
	p := indexTestProvider(t)
	ctx := context.Background()
	seedIndexRecords(t, p)

	req := gestalt.IndexedDBCreateIndexRequest{Store: "issues", Name: "by_status", KeyPath: []string{"status"}}
	if err := p.CreateIndex(ctx, req); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	err := p.CreateIndex(ctx, req)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("re-create index code = %v, want AlreadyExists", status.Code(err))
	}
}

func TestDeleteIndexRemovesAndIsStrict(t *testing.T) {
	p := indexTestProvider(t)
	ctx := context.Background()
	seedIndexRecords(t, p)

	if err := p.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{
		Store:   "issues",
		Name:    "by_status",
		KeyPath: []string{"status"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := p.DeleteIndex(ctx, gestalt.IndexedDBDeleteIndexRequest{Store: "issues", Name: "by_status"}); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}

	if _, err := p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store: "issues",
		Index: "by_status",
		Query: indexeddb.ToQuery("open"),
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("IndexGetAll after delete code = %v, want NotFound", status.Code(err))
	}

	err := p.DeleteIndex(ctx, gestalt.IndexedDBDeleteIndexRequest{Store: "issues", Name: "by_status"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("re-delete index code = %v, want NotFound", status.Code(err))
	}
}
