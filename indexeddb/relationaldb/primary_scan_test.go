package relationaldb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func seedPrimaryRows(t *testing.T, s *Store, store string, keys []any, legacy bool) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO " + s.genericRecordsTable() + " (store_name, pk_hash, pk_bytes, record_blob, pk_ord) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, value := range keys {
		key := mustEncodedKey(t, value)
		blob, err := marshalRecordBlob(gestalt.Record{"id": fmt.Sprint(value)})
		if err != nil {
			t.Fatal(err)
		}
		var ord any = key.ord
		if legacy {
			ord = nil
		}
		if _, err := stmt.Exec(store, key.hash, key.raw, blob, ord); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryScanOverVitessRowLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "large", gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatal(err)
	}
	keys := make([]any, 100005)
	for i := range keys {
		keys[i] = fmt.Sprintf("row-%06d", i)
	}
	seedPrimaryRows(t, s, "large", keys, false)
	count := uint32(3)
	query := indexeddb.ToQuery(indexeddb.Bound("row-100000", "row-100004", true, false))
	got, err := s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: query, Count: &count})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0]["id"] != "row-100001" || got[2]["id"] != "row-100003" {
		t.Fatalf("limited range: %#v", got)
	}
	n, err := s.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: query})
	if err != nil || n != 4 {
		t.Fatalf("count = %d, %v", n, err)
	}
	all, err := s.loadAllGenericRecords(ctx, "large")
	if err != nil || len(all) != len(keys) {
		t.Fatalf("unbounded read length = %d, %v", len(all), err)
	}
	zero := uint32(0)
	got, err = s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Count: &zero})
	if err != nil || len(got) != 0 {
		t.Fatalf("zero count: %v, %v", got, err)
	}
}

func TestPrimaryLegacyMergeAndResumableBackfill(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	legacy := []any{float64(-2), float64(10), "b", []byte{1, 2}, []any{"z", float64(1)}}
	current := []any{float64(2), time.Unix(100, 0).UTC(), "a", []byte{1}, []any{"a"}}
	seedPrimaryRows(t, s, "mixed", legacy, true)
	seedPrimaryRows(t, s, "mixed", current, false)
	count := uint32(4)
	check := func() {
		t.Helper()
		rows, err := s.loadPrimaryRows(ctx, "mixed", nil, true, &count, true)
		if err != nil {
			t.Fatal(err)
		}
		var got []any
		for _, row := range rows {
			value, err := decodeKeyValue(row.pkBytes)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, value)
		}
		want := []any{float64(-2), float64(2), float64(10), time.Unix(100, 0).UTC()}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mixed key order = %#v", got)
		}
	}
	check()
	n, err := s.countPrimaryRange(ctx, "mixed", indexeddb.ToQuery(indexeddb.Bound(float64(-2), "b", true, true)))
	if err != nil || n != 4 {
		t.Fatalf("mixed count: %d %v", n, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.backfillPrimaryKeyOrder(cancelled); err == nil {
		t.Fatal("cancelled backfill succeeded")
	}
	n, err = s.backfillPrimaryKeyOrder(ctx)
	if err != nil || n != int64(len(legacy)) {
		t.Fatalf("backfill: %d %v", n, err)
	}
	check()
	n, err = s.backfillPrimaryKeyOrder(ctx)
	if err != nil || n != 0 {
		t.Fatalf("replay: %d %v", n, err)
	}
	seedPrimaryRows(t, s, "mixed", []any{"old-writer-after-backfill"}, true)
	rows, err := s.loadPrimaryRows(ctx, "mixed", indexeddb.ToQuery("old-writer-after-backfill"), true, &count, true)
	if err != nil || len(rows) != 1 {
		t.Fatalf("old writer hidden: %d %v", len(rows), err)
	}
	if n, err := s.backfillPrimaryKeyOrder(ctx); err != nil || n != 1 {
		t.Fatalf("catchup: %d %v", n, err)
	}
}

func TestPrimarySQLPageCapsAndMySQLPrefixBoundary(t *testing.T) {
	for _, d := range []dialect{dialectSQLite, dialectPostgres, dialectSQLServer, dialectMySQL} {
		t.Run(fmt.Sprint(d), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := makeStoreWithDB(db, bindQuestion, d, storeOptions{}, false)
			key := mustEncodedKey(t, strings.Repeat("x", 300)+"a")
			columns := []string{"pk_hash", "pk_bytes", "record_blob", "pk_ord"}
			cap := "LIMIT 1$"
			if d == dialectSQLServer {
				cap = "OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY$"
			}
			mock.ExpectQuery(cap).WithArgs("store").WillReturnRows(sqlmock.NewRows(columns).AddRow(key.hash, key.raw, nil, key.ord)).RowsWillBeClosed()
			if d == dialectMySQL {
				prefix := make([]byte, 255)
				copy(prefix, key.ord)
				other := mustEncodedKey(t, strings.Repeat("x", 300)+"b")
				mock.ExpectQuery("CAST.* = \\? AND .*pk_hash.* > \\?.*LIMIT 1000$").WithArgs("store", prefix, key.hash).WillReturnRows(sqlmock.NewRows(columns).AddRow(other.hash, other.raw, nil, other.ord)).RowsWillBeClosed()
			}
			count := uint32(1)
			n := 0
			err = s.scanPrimaryRows(context.Background(), "store", "ordered", genericIndexRange{}, true, &count, func(genericRecordRow) error { n++; return nil })
			want := 1
			if d == dialectMySQL {
				want = 2
			}
			if err != nil || n != want {
				t.Fatalf("scan: %d %v", n, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIndexDuplicateKeysAcrossPagesLimitBeforePayload(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "items", gestalt.ObjectStoreOptions{Indexes: []gestalt.IndexSchema{{Name: "by_group", KeyPath: []string{"group"}}}}); err != nil {
		t.Fatal(err)
	}
	keys := make([]any, 2005)
	for i := range keys {
		keys[i] = fmt.Sprintf("item-%04d", i)
	}
	seedPrimaryRows(t, s, "items", keys, false)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	indexKey := mustEncodedKey(t, "shared")
	for _, value := range keys {
		key := mustEncodedKey(t, value)
		_, err := tx.Exec("INSERT INTO "+s.genericIndexTable()+" (store_name,index_name,index_key_hash,index_key_bytes,index_key_ord,pk_hash,pk_bytes) VALUES (?,?,?,?,?,?,?)", "items", "by_group", indexKey.hash, indexKey.raw, indexKey.ord, key.hash, key.raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Unrequested payloads must not be decoded (or even fetched).
	first := mustEncodedKey(t, keys[0])
	if _, err := s.db.Exec("UPDATE "+s.genericRecordsTable()+" SET record_blob = ? WHERE pk_hash <> ?", []byte("invalid"), first.hash); err != nil {
		t.Fatal(err)
	}
	count := uint32(1)
	got, err := s.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "items", Index: "by_group", Query: indexeddb.ToQuery("shared"), Count: &count})
	if err != nil || len(got) != 1 || got[0]["id"] != "item-0000" {
		t.Fatalf("limited duplicates: %#v %v", got, err)
	}
	n, err := s.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "items", Index: "by_group", Query: indexeddb.ToQuery("shared")})
	if err != nil || n != int64(len(keys)) {
		t.Fatalf("duplicate count: %d %v", n, err)
	}
}

func TestMigrateAddsNullablePrimaryOrderToExistingTable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.db.Exec("DROP TABLE " + s.genericRecordsTable()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE " + s.genericRecordsTable() + " (store_name TEXT NOT NULL,pk_hash BLOB NOT NULL,pk_bytes BLOB NOT NULL,record_blob BLOB NOT NULL,PRIMARY KEY(store_name,pk_hash))"); err != nil {
		t.Fatal(err)
	}
	key := mustEncodedKey(t, "legacy")
	if _, err := s.db.Exec("INSERT INTO "+s.genericRecordsTable()+" VALUES(?,?,?,?)", "store", key.hash, key.raw, []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureGenericTables(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureGenericTables(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.backfillPrimaryKeyOrder(ctx); err != nil || n != 1 {
		t.Fatalf("upgrade backfill: %d %v", n, err)
	}
}

func TestMySQLLongPrimaryKeys(t *testing.T) {
	dsn := os.Getenv("GESTALT_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("GESTALT_TEST_MYSQL_DSN not set")
	}
	s := testStoreWithOptions(t, dsn, storeOptions{TablePrefix: fmt.Sprintf("long_%x_", time.Now().UnixNano())})
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "long_keys", gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatal(err)
	}
	keys := make([]any, 1205)
	for i := range keys {
		keys[i] = strings.Repeat("x", 600) + fmt.Sprintf("-%04d", i)
	}
	seedPrimaryRows(t, s, "long_keys", keys, false)
	// A null key sharing the boundary prefix must participate in the limit.
	legacy := strings.Repeat("x", 600) + "-0000a"
	seedPrimaryRows(t, s, "long_keys", []any{legacy}, true)
	count := uint32(3)
	got, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "long_keys", Count: &count})
	want := []string{keys[0].(string), legacy, keys[1].(string)}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("long-key limit: count=%d error=%v", len(got), err)
	}
	all, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "long_keys"})
	if err != nil || len(all) != len(keys)+1 {
		t.Fatalf("long-key pagination: %d %v", len(all), err)
	}
}
