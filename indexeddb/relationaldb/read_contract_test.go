package relationaldb_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/client"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestObjectStorePaginationContract(t *testing.T) {
	s, err := relationaldb.NewStore("file:" + filepath.Join(t.TempDir(), "large.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "large", gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatal(err)
	}
	const size = 1005
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
	first, err := s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound("row-001000", "row-001004", true, false)), Count: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0]["id"] != "row-001001" || first[1]["id"] != "row-001002" {
		t.Fatalf("first page: %v", first)
	}
	next, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound(first[1]["id"], "row-001004", true, false)), Count: &limit})
	if err != nil || !reflect.DeepEqual(next, []string{"row-001003", "row-001004"}) {
		t.Fatalf("continuation: %v %v", next, err)
	}
	count, err := s.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "large", Query: indexeddb.ToQuery(indexeddb.Bound("row-001000", "row-001004", true, true))})
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
	if dsn := os.Getenv("GESTALT_TEST_POSTGRES_DSN"); dsn != "" {
		dsns["Postgres"] = dsn
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
			// An incompressible shared prefix exceeds PostgreSQL's B-tree entry
			// limit and puts more than one SQL page in the same prefix group.
			var shared strings.Builder
			for i := 0; i < 64; i++ {
				fmt.Fprintf(&shared, "%x", sha256.Sum256([]byte(fmt.Sprint(i))))
			}
			prefix := shared.String()
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
			next, err := s.GetAllKeys(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store, Query: indexeddb.ToQuery(indexeddb.LowerBound(want[1], true)), Count: &limit})
			if err != nil || !reflect.DeepEqual(next, []string{prefix + "-0002", prefix + "-0003"}) {
				t.Fatalf("long primary key continuation: %v", err)
			}
			records, err := s.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Query: indexeddb.ToQuery("shared"), Count: &limit})
			if err != nil || len(records) != 2 || records[0]["id"] != want[0] || records[1]["id"] != want[1] {
				t.Fatalf("duplicate index keys: count=%d error=%v", len(records), err)
			}
			count, err := s.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Query: indexeddb.ToQuery("shared")})
			if err != nil || count != 1205 {
				t.Fatalf("duplicate count: %d %v", count, err)
			}
			// Index keys with the same truncated prefix must still sort by their
			// full value, even when that reverses the primary-key order.
			tx, err = s.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{store}, Mode: gestalt.TransactionReadwrite})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 1205; i++ {
				if err := tx.Put(ctx, gestalt.IndexedDBRecordRequest{Store: store, Record: gestalt.Record{"id": fmt.Sprintf("%s-%04d", prefix, i), "group": fmt.Sprintf("%s-%04d", prefix, 1204-i)}}); err != nil {
					tx.Abort(ctx)
					t.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			first, err = s.IndexGetAllKeys(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Count: &limit})
			if err != nil || !reflect.DeepEqual(first, []string{prefix + "-1204", prefix + "-1203"}) {
				t.Fatalf("long index order: %v", err)
			}
			next, err = s.IndexGetAllKeys(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "by_group", Query: indexeddb.ToQuery(indexeddb.LowerBound(prefix+"-0001", true)), Count: &limit})
			if err != nil || !reflect.DeepEqual(next, []string{prefix + "-1202", prefix + "-1201"}) {
				t.Fatalf("long index continuation: %v", err)
			}
		})
	}
}

func TestZeroCountQueryValidationContract(t *testing.T) {
	s, err := relationaldb.NewStore("file:" + filepath.Join(t.TempDir(), "queries.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "records", gestalt.ObjectStoreOptions{Indexes: []gestalt.IndexSchema{{Name: "by_group", KeyPath: []string{"group"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "records", Record: gestalt.Record{"id": "one", "group": "shared"}}); err != nil {
		t.Fatal(err)
	}
	zero := uint32(0)
	for _, test := range []struct {
		query *client.IndexedDBQuery
		want  codes.Code
	}{
		{nil, codes.OK},
		{indexeddb.ToQuery("shared"), codes.OK},
		{&client.IndexedDBQuery{}, codes.InvalidArgument},
		{&client.IndexedDBQuery{Query: &client.IndexedDBQueryQueryKey{}}, codes.InvalidArgument},
		{&client.IndexedDBQuery{Query: &client.IndexedDBQueryQueryRange{Value: &client.KeyRange{Lower: &client.KeyValue{}}}}, codes.InvalidArgument},
	} {
		query, want := test.query, test.want
		object := gestalt.IndexedDBObjectStoreRangeRequest{Store: "records", Query: query, Count: &zero}
		index := gestalt.IndexedDBIndexQueryRequest{Store: "records", Index: "by_group", Queries: []*client.IndexedDBQuery{indexeddb.ToQuery("shared"), query}, Count: &zero}
		records, getErr := s.GetAll(ctx, object)
		keys, keysErr := s.GetAllKeys(ctx, object)
		indexedRecords, indexErr := s.IndexGetAll(ctx, index)
		indexedKeys, indexKeysErr := s.IndexGetAllKeys(ctx, index)
		for _, err := range []error{getErr, keysErr, indexErr, indexKeysErr} {
			if status.Code(err) != want {
				t.Fatalf("zero-count query %v: want %v, got %v", query, want, err)
			}
		}
		if len(records)+len(keys)+len(indexedRecords)+len(indexedKeys) != 0 {
			t.Fatal("zero-count read returned records")
		}
	}
}

func TestDateKeyRangeContract(t *testing.T) {
	s, err := relationaldb.NewStore("file:" + filepath.Join(t.TempDir(), "dates.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateObjectStore(ctx, "dates", gestalt.ObjectStoreOptions{Columns: []gestalt.ColumnDef{{Name: "id", Type: gestalt.TypeTime, PrimaryKey: true}}}); err != nil {
		t.Fatal(err)
	}
	first, last := time.Unix(0, math.MinInt64).UTC(), time.Unix(0, math.MaxInt64).UTC()
	for _, date := range []time.Time{last, first} {
		if err := s.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "dates", Record: gestalt.Record{"id": date}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, date := range []time.Time{first.Add(-time.Nanosecond), last.Add(time.Nanosecond), time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)} {
		for _, write := range []func(context.Context, gestalt.IndexedDBRecordRequest) error{s.Add, s.Put} {
			if err := write(ctx, gestalt.IndexedDBRecordRequest{Store: "dates", Record: gestalt.Record{"id": date}}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("out-of-range date %s: %v", date, err)
			}
		}
		if _, err := s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "dates", Query: indexeddb.ToQuery(indexeddb.LowerBound(date, false))}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("out-of-range date query %s: %v", date, err)
		}
	}
	got, err := s.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: "dates", Query: indexeddb.ToQuery(indexeddb.Bound(first, last, false, false))})
	if err != nil || !reflect.DeepEqual(got, []gestalt.Record{{"id": first}, {"id": last}}) {
		t.Fatalf("ordered date boundary records: %v %v", got, err)
	}
}

func TestBackfillUpgradeContract(t *testing.T) {
	dsns := map[string]string{"SQLite": "file:" + filepath.Join(t.TempDir(), "upgrade.sqlite")}
	if dsn := os.Getenv("GESTALT_TEST_MYSQL_DSN"); dsn != "" {
		dsns["MySQL"] = dsn
	}
	if dsn := os.Getenv("GESTALT_TEST_POSTGRES_DSN"); dsn != "" {
		dsns["Postgres"] = dsn
	}
	for name, dsn := range dsns {
		t.Run(name, func(t *testing.T) { testOnlineBackfillUpgrade(t, dsn) })
	}
}

func testOnlineBackfillUpgrade(t *testing.T, dsn string) {
	ctx := context.Background()
	s, err := relationaldb.NewStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	stores := []string{"records", "records-second"}
	var want []gestalt.Record
	for i := range 1005 {
		want = append(want, gestalt.Record{"id": fmt.Sprintf("persisted-%06d", i), "value": "keep me"})
	}
	for _, store := range stores {
		if err := s.CreateObjectStore(ctx, store, gestalt.ObjectStoreOptions{Indexes: []gestalt.IndexSchema{
			{Name: "by_value", KeyPath: []string{"value"}},
			{Name: "by_id", KeyPath: []string{"id"}, Unique: true},
		}}); err != nil {
			t.Fatal(err)
		}
		tx, err := s.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{Stores: []string{store}, Mode: gestalt.TransactionReadwrite})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range want {
			if err := tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: store, Record: record}); err != nil {
				_ = tx.Abort(ctx)
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	// Persisted fixture after adding the column, before the data migration.
	driver, sqlDSN := "sqlite", dsn
	if strings.HasPrefix(dsn, "mysql://") {
		driver, sqlDSN = "mysql", strings.TrimPrefix(dsn, "mysql://")
	}
	if strings.HasPrefix(dsn, "postgres://") {
		driver = "pgx"
	}
	db, err := sql.Open(driver, sqlDSN)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"_gestalt_records", "_gestalt_index_entries", "_gestalt_unique_index_entries"} {
		if _, err := db.Exec("UPDATE " + table + " SET pk_ord = NULL WHERE store_name IN ('records', 'records-second')"); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	db.Close()
	p := relationaldb.New()
	if err := p.Configure(ctx, "", map[string]any{"dsn": dsn}); err != nil {
		t.Fatalf("compatibility provider must serve before backfill: %v", err)
	}
	for _, store := range stores {
		got, err := p.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("legacy records before backfill: %v", err)
		}
		for _, index := range []string{"by_value", "by_id"} {
			got, err := p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: index})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("legacy index %s before backfill: %v", index, err)
			}
		}
		if err := p.CreateIndex(ctx, gestalt.IndexedDBCreateIndexRequest{Store: store, Name: "during_upgrade", KeyPath: []string{"value"}}); err != nil {
			t.Fatal(err)
		}
		indexed, err := p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: "during_upgrade"})
		if err != nil || !reflect.DeepEqual(indexed, want) {
			t.Fatalf("index created before backfill: %v", err)
		}
		if err := p.Put(ctx, gestalt.IndexedDBRecordRequest{Store: store, Record: want[0]}); err != nil {
			t.Fatal(err)
		}
	}
	newRecord := gestalt.Record{"id": "persisted-new", "value": "keep me"}
	for _, store := range stores {
		if err := p.Add(ctx, gestalt.IndexedDBRecordRequest{Store: store, Record: newRecord}); err != nil {
			t.Fatal(err)
		}
		got, err := p.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store})
		if err != nil || !reflect.DeepEqual(got, append(append([]gestalt.Record{}, want...), newRecord)) {
			t.Fatalf("mixed legacy and new records: %v", err)
		}
	}
	p.Close()
	if n, err := relationaldb.BackfillPrimaryKeyOrder(ctx, dsn, relationaldb.Options{}); err != nil || n != int64((len(want)-1)*len(stores)*3) {
		t.Fatalf("backfill should update only legacy rows: %d, %v", n, err)
	}
	if n, err := relationaldb.BackfillPrimaryKeyOrder(ctx, dsn, relationaldb.Options{}); err != nil || n != 0 {
		t.Fatalf("repeat backfill: %d updates, %v", n, err)
	}
	want = append(want, newRecord)
	if err := p.Configure(ctx, "", map[string]any{"dsn": dsn}); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, store := range stores {
		got, err := p.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: store})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("persisted records in %s after upgrade: %d records, %v", store, len(got), err)
		}
		for _, index := range []string{"by_value", "by_id"} {
			indexed, err := p.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{Store: store, Index: index})
			if err != nil || !reflect.DeepEqual(indexed, want) {
				t.Fatalf("persisted index %s/%s after upgrade: %d records, %v", store, index, len(indexed), err)
			}
		}
	}
}
