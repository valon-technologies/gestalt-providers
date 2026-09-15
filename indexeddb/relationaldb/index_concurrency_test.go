package relationaldb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIndexManagementCoordinatesProviderConnections(t *testing.T) {
	targets := map[string]string{"SQLite": "file:" + filepath.Join(t.TempDir(), "index-lock.sqlite")}
	for name, variable := range map[string]string{"Postgres": "GESTALT_TEST_POSTGRES_DSN", "MySQL": "GESTALT_TEST_MYSQL_DSN"} {
		if dsn := os.Getenv(variable); dsn != "" {
			targets[name] = dsn
		}
	}
	for name, dsn := range targets {
		t.Run(name, func(t *testing.T) {
			opts := storeOptions{TablePrefix: fmt.Sprintf("index_lock_%d_", time.Now().UnixNano()), MetadataKeyPrefix: true}
			first := &Provider{Store: testStoreWithOptions(t, dsn, opts)}
			second := &Provider{Store: testStoreWithOptions(t, dsn, opts)}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			seedIndexRecords(t, first)
			writer, err := first.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{"issues"}})
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Abort(context.Background())
			if err := writer.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "issues", Record: gestalt.Record{"id": "d", "status": "open"}}); err != nil {
				t.Fatal(err)
			}
			created := make(chan error, 1)
			go func() {
				created <- second.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{Store: "issues", Name: "by_status", KeyPath: []string{"status"}})
			}()
			select {
			case err := <-created:
				t.Fatalf("index build did not wait for the writer transaction: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if err := writer.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-created; err != nil {
				t.Fatal(err)
			}
			rows, err := second.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "issues", Index: "by_status", Query: indexeddb.ToQuery("open")})
			if err != nil || len(rows) != 3 {
				t.Fatalf("backfill omitted committed writes: rows=%v err=%v", rows, err)
			}

			// Hold the same exclusive schema lock used by public CreateIndex, then issue
			// a write through another provider/connection while the schema is changing.
			locked, release := make(chan struct{}), make(chan struct{})
			go func() {
				created <- first.Store.changeIndex(ctx, "issues", func(ctx context.Context) error {
					close(locked)
					<-release
					return first.Store.createIndexStrict(ctx, "issues", "by_id", []string{"id"}, IndexParameters{})
				})
			}()
			<-locked
			written := make(chan error, 1)
			go func() {
				written <- second.Put(ctx, gestalt.IndexedDBRecordRequest{Store: "issues", Record: gestalt.Record{"id": "after", "status": "open"}})
			}()
			select {
			case err := <-written:
				close(release)
				<-created
				t.Fatalf("write did not wait for schema change: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			close(release)
			if err := <-created; err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			rows, err = first.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "issues", Index: "by_id", Query: indexeddb.ToQuery("after")})
			if err != nil || len(rows) != 1 {
				t.Fatalf("write used stale index metadata: rows=%v err=%v", rows, err)
			}
		})
	}
}

func TestSQLiteTransactionRefreshesSchemaBeforeFirstWrite(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "schema-refresh.sqlite")
	first := &Provider{Store: testStoreWithDSN(t, dsn)}
	second := &Provider{Store: testStoreWithDSN(t, dsn)}
	ctx := context.Background()
	seedIndexRecords(t, first)
	tx, err := first.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{"issues"}})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort(ctx)
	if err := second.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{Store: "issues", Name: "by_status", KeyPath: []string{"status"}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "issues", Record: gestalt.Record{"id": "new", "status": "new"}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := second.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "issues", Index: "by_status", Query: indexeddb.ToQuery("new")})
	if err != nil || len(rows) != 1 {
		t.Fatalf("stale schema after delayed first write: rows=%v err=%v", rows, err)
	}
}

func TestPublicUniqueIndexFailureRollsBack(t *testing.T) {
	p := indexTestProvider(t)
	ctx := context.Background()
	seedIndexRecords(t, p)
	err := p.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{Store: "issues", Name: "unique_status", KeyPath: []string{"status"}, Unique: true})
	if err == nil {
		t.Fatal("expected duplicate values to reject unique index")
	}
	if _, err = p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "issues", Index: "unique_status"}); status.Code(err) != codes.NotFound {
		t.Fatalf("failed index became visible: %v", err)
	}
	if count, err := p.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "issues"}); err != nil || count != 3 {
		t.Fatalf("rollback lost rows: count=%d err=%v", count, err)
	}
}
