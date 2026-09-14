package relationaldb_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func TestLargeStoreReadContract(t *testing.T) {
	s, err := relationaldb.NewStore("file:" + filepath.Join(t.TempDir(), "large.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "large", gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatal(err)
	}
	const size = 100005
	for start := 0; start < size; start += 1000 {
		tx, err := s.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{"large"}, Mode: gestalt.TransactionReadwrite})
		if err != nil {
			t.Fatal(err)
		}
		for i := start; i < min(start+1000, size); i++ {
			if err := tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "large", Record: gestalt.Record{"id": fmt.Sprintf("row-%06d", i)}}); err != nil {
				tx.Abort(ctx)
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	limit := uint32(2)
	first, err := s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound("row-100000", "row-100004", true, false)), Count: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0]["id"] != "row-100001" || first[1]["id"] != "row-100002" {
		t.Fatalf("first page: %v", first)
	}
	next, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound(first[1]["id"], "row-100004", true, false)), Count: &limit})
	if err != nil || !reflect.DeepEqual(next, []string{"row-100003", "row-100004"}) {
		t.Fatalf("continuation: %v %v", next, err)
	}
	count, err := s.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound("row-100000", "row-100004", true, true))})
	if err != nil || count != 3 {
		t.Fatalf("count: %d %v", count, err)
	}
	all, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large"})
	if err != nil || len(all) != size {
		t.Fatalf("complete scan: %d %v", len(all), err)
	}
	for i, id := range all {
		if id != fmt.Sprintf("row-%06d", i) {
			t.Fatalf("record %d: %s", i, id)
		}
	}
}

func TestLongKeysAndDuplicateIndexReadContract(t *testing.T) {
	dsns := map[string]string{"SQLite": "file:" + filepath.Join(t.TempDir(), "long.sqlite")}
	if dsn := os.Getenv("GESTALT_TEST_MYSQL_DSN"); dsn != "" {
		dsns["MySQL"] = dsn
	}
	for name, dsn := range dsns {
		t.Run(name, func(t *testing.T) {
			s, err := relationaldb.NewStore(dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ctx := context.Background()
			store := fmt.Sprintf("long_keys_%s", name)
			if err := s.CreateObjectStore(ctx, store, gestalt.ObjectStoreOptions{Indexes: []gestalt.IndexSchema{{Name: "by_group", KeyPath: []string{"group"}}}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Clear(ctx, store); err != nil {
				t.Fatal(err)
			}
			prefix := strings.Repeat("x", 600)
			tx, err := s.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{store}, Mode: gestalt.TransactionReadwrite})
			if err != nil {
				t.Fatal(err)
			}
			for i := 1204; i >= 0; i-- {
				if err := tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: store, Record: gestalt.Record{"id": fmt.Sprintf("%s-%04d", prefix, i), "group": "shared"}}); err != nil {
					tx.Abort(ctx)
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			limit := uint32(2)
			first, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store, Count: &limit})
			want := []string{prefix + "-0000", prefix + "-0001"}
			if err != nil || !reflect.DeepEqual(first, want) {
				t.Fatalf("long primary keys: %v", err)
			}
			records, err := s.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Query: indexeddb.ToQuery("shared"), Count: &limit})
			if err != nil || len(records) != 2 || records[0]["id"] != want[0] || records[1]["id"] != want[1] {
				t.Fatalf("duplicate index keys: count=%d error=%v", len(records), err)
			}
			count, err := s.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Query: indexeddb.ToQuery("shared")})
			if err != nil || count != 1205 {
				t.Fatalf("duplicate count: %d %v", count, err)
			}
		})
	}
}

func TestBackfillUpgradeContract(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "upgrade.sqlite")
	s, err := relationaldb.NewStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateObjectStore(ctx, "records", gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatal(err)
	}
	want := gestalt.Record{"id": "persisted-before-upgrade", "value": "keep me"}
	if err := s.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "records", Record: want}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Persisted fixture after adding the column, before the data migration.
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE _gestalt_records SET pk_ord = NULL"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	p := relationaldb.New()
	if err := p.Configure(ctx, "", map[string]any{"dsn": dsn}); err == nil {
		p.Close()
		t.Fatal("provider accepted an unfinished data migration")
	}
	if _, err := relationaldb.BackfillPrimaryKeyOrder(ctx, dsn, relationaldb.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := p.Configure(ctx, "", map[string]any{"dsn": dsn}); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	got, err := p.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "records"})
	if err != nil || !reflect.DeepEqual(got, []gestalt.Record{want}) {
		t.Fatalf("persisted records after upgrade: %v %v", got, err)
	}
}
