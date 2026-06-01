package relationaldb

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testFactory(t *testing.T) Factory {
	t.Helper()
	factory, err := OpenFactoryDSN("file:"+filepath.Join(t.TempDir(), "factory.sqlite"), Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN: %v", err)
	}
	t.Cleanup(func() { _ = factory.Close() })
	return factory
}

func databaseStore(t *testing.T, db Database) *Store {
	t.Helper()
	rel, ok := db.(*relationalDatabase)
	if !ok {
		t.Fatalf("database implementation = %T, want *relationalDatabase", db)
	}
	return rel.store
}

func usersSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true, NotNull: true},
			{Name: "email", Type: gestalt.TypeString},
			{Name: "name", Type: gestalt.TypeString},
		},
	}
}

func userRecord(id, email, name string) gestalt.Record {
	return gestalt.Record{"id": id, "email": email, "name": name}
}

func TestFactoryOpenVersionLifecycleAndCreateIndexBackfill(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	zero := uint64(0)
	if _, err := factory.Open(ctx, "app", OpenOptions{Version: &zero}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Open explicit version 0 error = %v, want InvalidArgument", err)
	}
	tooLarge := uint64(math.MaxInt64) + 1
	if _, err := factory.Open(ctx, "app", OpenOptions{Version: &tooLarge}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Open version above SQL integer range error = %v, want InvalidArgument", err)
	}

	version1 := uint64(1)
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			if up.OldVersion() != 0 || up.NewVersion() != 1 {
				t.Fatalf("upgrade version = %d -> %d, want 0 -> 1", up.OldVersion(), up.NewVersion())
			}
			if _, err := up.CreateObjectStore(ctx, "users", usersSchema()); err != nil {
				return err
			}
			if _, err := up.CreateObjectStore(ctx, "users", usersSchema()); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("duplicate CreateObjectStore error = %v, want AlreadyExists", err)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	if db.Version() != 1 {
		t.Fatalf("db.Version() = %d, want 1", db.Version())
	}
	names, err := db.ObjectStoreNames(ctx)
	if err != nil {
		t.Fatalf("ObjectStoreNames: %v", err)
	}
	if got := len(names); got != 1 || names[0] != "users" {
		t.Fatalf("ObjectStoreNames = %v, want [users]", names)
	}
	if err := databaseStore(t, db).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u1", "a@example.com", "Alice")}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	current, err := factory.OpenCurrent(ctx, "app")
	if err != nil {
		t.Fatalf("OpenCurrent: %v", err)
	}
	if current.Version() != 1 {
		t.Fatalf("OpenCurrent version = %d, want 1", current.Version())
	}
	if err := current.Close(); err != nil {
		t.Fatalf("Close current: %v", err)
	}

	version2 := uint64(2)
	upgraded, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version2,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			store, err := up.ObjectStore("users")
			if err != nil {
				return err
			}
			return store.CreateIndex(ctx, "by_email", []string{"email"}, IndexParameters{Unique: true})
		},
	})
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	defer upgraded.Close()
	if upgraded.Version() != 2 {
		t.Fatalf("upgraded.Version() = %d, want 2", upgraded.Version())
	}
	record, err := databaseStore(t, upgraded).IndexGet(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store: "users", Index: "by_email", Values: []any{"a@example.com"},
	})
	if err != nil {
		t.Fatalf("IndexGet backfilled index: %v", err)
	}
	if got := record["id"]; got != "u1" {
		t.Fatalf("IndexGet id = %v, want u1", got)
	}
}

func TestFactoryCmpUsesIndexedDBKeySemantics(t *testing.T) {
	factory := testFactory(t)

	ordered := []any{
		int64(1),
		time.Unix(1, 0).UTC(),
		"a",
		[]byte{0x00},
		[]any{"a"},
	}
	for i := 0; i < len(ordered)-1; i++ {
		cmp, err := factory.Cmp(ordered[i], ordered[i+1])
		if err != nil {
			t.Fatalf("Cmp(%T, %T): %v", ordered[i], ordered[i+1], err)
		}
		if cmp >= 0 {
			t.Fatalf("Cmp(%#v, %#v) = %d, want < 0", ordered[i], ordered[i+1], cmp)
		}
	}
	cmp, err := factory.Cmp([]any{"a", int64(2)}, []any{"a", int64(10)})
	if err != nil {
		t.Fatalf("Cmp(array): %v", err)
	}
	if cmp >= 0 {
		t.Fatalf("Cmp([a 2], [a 10]) = %d, want < 0", cmp)
	}

	for _, invalid := range []any{nil, true, map[string]any{"x": 1}, math.NaN()} {
		if _, err := factory.Cmp(invalid, int64(1)); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Cmp(%#v, 1) error = %v, want InvalidArgument", invalid, err)
		}
	}
}

func TestFactoryDeleteDuringInitialUpgradeIsBlocked(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct {
		db  Database
		err error
	}, 1)
	version1 := uint64(1)
	go func() {
		db, err := factory.Open(ctx, "app", OpenOptions{
			Version: &version1,
			Upgrade: func(context.Context, UpgradeContext) error {
				close(started)
				<-release
				return nil
			},
		})
		done <- struct {
			db  Database
			err error
		}{db: db, err: err}
	}()

	<-started
	if err := factory.DeleteDatabase(ctx, "app"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteDatabase during initial upgrade error = %v, want FailedPrecondition", err)
	}
	close(release)

	result := <-done
	if result.err != nil {
		t.Fatalf("Open after blocked delete: %v", result.err)
	}
	if result.db == nil || result.db.Version() != 1 {
		t.Fatalf("opened database = %#v, want version 1", result.db)
	}
	if err := result.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFactorySameVersionOpenQueuesBehindInitialUpgrade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory := testFactory(t)

	started := make(chan struct{})
	release := make(chan struct{})
	doneFirst := make(chan struct {
		db  Database
		err error
	}, 1)
	version1 := uint64(1)
	go func() {
		db, err := factory.Open(ctx, "app", OpenOptions{
			Version: &version1,
			Upgrade: func(context.Context, UpgradeContext) error {
				close(started)
				<-release
				return nil
			},
		})
		doneFirst <- struct {
			db  Database
			err error
		}{db: db, err: err}
	}()

	<-started
	doneSecond := make(chan struct {
		db  Database
		err error
	}, 1)
	go func() {
		db, err := factory.Open(ctx, "app", OpenOptions{Version: &version1})
		doneSecond <- struct {
			db  Database
			err error
		}{db: db, err: err}
	}()

	close(release)
	first := <-doneFirst
	if first.err != nil {
		t.Fatalf("first Open: %v", first.err)
	}
	defer first.db.Close()
	second := <-doneSecond
	if second.err != nil {
		t.Fatalf("second same-version Open: %v", second.err)
	}
	defer second.db.Close()
	if first.db.Version() != 1 || second.db.Version() != 1 {
		t.Fatalf("versions = %d and %d, want both 1", first.db.Version(), second.db.Version())
	}
}

func TestFactoryDeleteDuringInitialUpgradeIsBlockedAcrossFactories(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "cross-factory-initial-delete.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	defer first.Close()
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct {
		db  Database
		err error
	}, 1)
	version1 := uint64(1)
	go func() {
		db, err := first.Open(ctx, "app", OpenOptions{
			Version: &version1,
			Upgrade: func(context.Context, UpgradeContext) error {
				close(started)
				<-release
				return nil
			},
		})
		done <- struct {
			db  Database
			err error
		}{db: db, err: err}
	}()

	<-started
	if err := second.DeleteDatabase(ctx, "app"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-factory DeleteDatabase during initial upgrade error = %v, want FailedPrecondition", err)
	}
	close(release)

	result := <-done
	if result.err != nil {
		t.Fatalf("Open after blocked cross-factory delete: %v", result.err)
	}
	if result.db == nil || result.db.Version() != 1 {
		t.Fatalf("opened database = %#v, want version 1", result.db)
	}
	if err := result.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFactoryDatabaseNamesAreOpaqueStrings(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	names := []string{"", "app", " app "}
	for _, name := range names {
		db, err := factory.OpenCurrent(ctx, name)
		if err != nil {
			t.Fatalf("OpenCurrent(%q): %v", name, err)
		}
		if db.Name() != name {
			t.Fatalf("db.Name() = %q, want %q", db.Name(), name)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close(%q): %v", name, err)
		}
	}

	databases, err := factory.Databases(ctx)
	if err != nil {
		t.Fatalf("Databases: %v", err)
	}
	got := make(map[string]uint64, len(databases))
	for _, db := range databases {
		got[db.Name] = db.Version
	}
	for _, name := range names {
		if got[name] != 1 {
			t.Fatalf("Databases[%q] = %d, want version 1 in %+v", name, got[name], databases)
		}
	}

	if err := factory.DeleteDatabase(ctx, " app "); err != nil {
		t.Fatalf("DeleteDatabase spaced name: %v", err)
	}
	if _, err := factory.OpenCurrent(ctx, "app"); err != nil {
		t.Fatalf("OpenCurrent app after deleting spaced name: %v", err)
	}
}

func TestFactoryGeneratedPhysicalNamesStayPortable(t *testing.T) {
	factory := &relationalFactory{}
	namespace := factory.logicalNamespace("app")
	tables := []string{
		namespace + genericRecordsTableName,
		namespace + genericIndexTableName,
		namespace + genericUniqueIndexTableName,
	}
	for _, table := range tables {
		if len(baseTableName(table)) > maxPortableIdentifierLength {
			t.Fatalf("table name %q length = %d, want <= %d", table, len(baseTableName(table)), maxPortableIdentifierLength)
		}
	}

	indexNames := []string{
		portableIndexName(tables[0], "record_lookup"),
		portableIndexName(tables[0], "store"),
		portableIndexName(tables[1], "lookup"),
		portableIndexName(tables[1], "record"),
		portableIndexName(tables[1], "scan"),
		portableIndexName(tables[2], "lookup"),
		portableIndexName(tables[2], "record"),
		portableIndexName(tables[2], "scan"),
	}
	seen := make(map[string]struct{}, len(indexNames))
	for _, name := range indexNames {
		if len(name) > maxPortableIdentifierLength {
			t.Fatalf("index name %q length = %d, want <= %d", name, len(name), maxPortableIdentifierLength)
		}
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate generated index name: %q", name)
		}
		seen[name] = struct{}{}
	}
}

func TestFactoryInitialUpgradeFailureDoesNotReserveVersionZero(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	boom := errors.New("boom")
	if _, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(context.Context, UpgradeContext) error {
			return boom
		},
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("Open failed initial upgrade error = %v, want Aborted", err)
	}

	databases, err := factory.Databases(ctx)
	if err != nil {
		t.Fatalf("Databases after failed initial upgrade: %v", err)
	}
	if len(databases) != 0 {
		t.Fatalf("Databases after failed initial upgrade = %+v, want none", databases)
	}

	db, err := factory.Open(ctx, "app", OpenOptions{Version: &version1})
	if err != nil {
		t.Fatalf("Open after failed initial upgrade: %v", err)
	}
	defer db.Close()
	if db.Version() != 1 {
		t.Fatalf("db.Version() = %d, want 1", db.Version())
	}
}

func TestFactoryCloseDuringUpgradeDoesNotCommitVersion(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "close-during-upgrade.sqlite")
	factory, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN: %v", err)
	}
	relFactory := factory.(*relationalFactory)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct {
		db  Database
		err error
	}, 1)
	version1 := uint64(1)
	go func() {
		db, err := factory.Open(ctx, "app", OpenOptions{
			Version: &version1,
			Upgrade: func(context.Context, UpgradeContext) error {
				close(started)
				<-release
				return nil
			},
		})
		done <- struct {
			db  Database
			err error
		}{db: db, err: err}
	}()

	<-started
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- factory.Close()
	}()
	waitForClosedFactory(t, relFactory)
	close(release)

	result := <-done
	if status.Code(result.err) != codes.FailedPrecondition {
		t.Fatalf("Open after concurrent Close error = %v, want FailedPrecondition", result.err)
	}
	if result.db != nil {
		t.Fatalf("Open returned database after concurrent Close: %#v", result.db)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(reopened): %v", err)
	}
	defer reopened.Close()
	databases, err := reopened.Databases(ctx)
	if err != nil {
		t.Fatalf("Databases: %v", err)
	}
	if len(databases) != 0 {
		t.Fatalf("Databases after aborted close-during-upgrade = %+v, want none", databases)
	}
}

func waitForClosedFactory(t *testing.T, factory *relationalFactory) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		factory.mu.Lock()
		closed := factory.closed
		factory.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("factory did not close")
}

func TestFactoryStoreHandleCannotBypassDatabaseClose(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)
	version1 := uint64(1)
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	store := databaseStore(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u1", "a@example.com", "Alice")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("raw Store.Add after database close error = %v, want FailedPrecondition", err)
	}
}

func TestFactoryVersionChangeAndBlockedCallbacks(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	var db1 Database
	var err error
	db1, err = factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
		OnVersionChange: func(ctx context.Context, info VersionChangeInfo) error {
			if info.Name != "app" || info.OldVersion != 1 || info.NewVersion == nil || *info.NewVersion != 2 {
				t.Fatalf("versionchange info = %+v, want app 1 -> 2", info)
			}
			return db1.Close()
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}

	version2 := uint64(2)
	db2, err := factory.Open(ctx, "app", OpenOptions{Version: &version2})
	if err != nil {
		t.Fatalf("Open v2 after versionchange close: %v", err)
	}

	version3 := uint64(3)
	if _, err := factory.Open(ctx, "app", OpenOptions{Version: &version3}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Open blocked without OnBlocked error = %v, want FailedPrecondition", err)
	}

	var blockedInfo BlockedInfo
	db3, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version3,
		OnBlocked: func(ctx context.Context, info BlockedInfo) (BlockedAction, error) {
			blockedInfo = info
			if err := db2.Close(); err != nil {
				return BlockedFail, err
			}
			return BlockedWait, nil
		},
	})
	if err != nil {
		t.Fatalf("Open v3 after blocked close: %v", err)
	}
	defer db3.Close()
	if blockedInfo.OpenConnections != 1 || blockedInfo.OldVersion != 2 || blockedInfo.NewVersion == nil || *blockedInfo.NewVersion != 3 {
		t.Fatalf("blocked info = %+v, want one blocker for 2 -> 3", blockedInfo)
	}
}

func TestFactoryVersionChangeCallbackErrorDoesNotVetoUpgrade(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	var db1 Database
	var versionChangeCalls int
	var err error
	db1, err = factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
		OnVersionChange: func(context.Context, VersionChangeInfo) error {
			versionChangeCalls++
			return errors.New("ignored")
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}

	version2 := uint64(2)
	var blockedCalls int
	db2, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version2,
		OnBlocked: func(context.Context, BlockedInfo) (BlockedAction, error) {
			blockedCalls++
			if err := db1.Close(); err != nil {
				return BlockedFail, err
			}
			return BlockedWait, nil
		},
	})
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	defer db2.Close()
	if versionChangeCalls != 1 {
		t.Fatalf("versionchange calls = %d, want 1", versionChangeCalls)
	}
	if blockedCalls != 1 {
		t.Fatalf("blocked calls = %d, want 1", blockedCalls)
	}
}

func TestFactoryDatabaseVersionIsConnectionSnapshot(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	var db1 Database
	var err error
	db1, err = factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
		OnVersionChange: func(context.Context, VersionChangeInfo) error {
			return db1.Close()
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}

	version2 := uint64(2)
	db2, err := factory.Open(ctx, "app", OpenOptions{Version: &version2})
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	defer db2.Close()
	if db1.Version() != 1 {
		t.Fatalf("closed db1.Version() = %d, want connection snapshot version 1", db1.Version())
	}
	if db2.Version() != 2 {
		t.Fatalf("db2.Version() = %d, want 2", db2.Version())
	}
}

func TestFactoryClosedHandleDoesNotReceiveVersionChange(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	var versionChanges int
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
		OnVersionChange: func(context.Context, VersionChangeInfo) error {
			versionChanges++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	version2 := uint64(2)
	upgraded, err := factory.Open(ctx, "app", OpenOptions{Version: &version2})
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	defer upgraded.Close()
	if versionChanges != 0 {
		t.Fatalf("closed handle received %d versionchange callbacks, want 0", versionChanges)
	}
}

func TestFactoryLogicalDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)
	version1 := uint64(1)

	openWithUsers := func(name string) Database {
		t.Helper()
		db, err := factory.Open(ctx, name, OpenOptions{
			Version: &version1,
			Upgrade: func(ctx context.Context, up UpgradeContext) error {
				_, err := up.CreateObjectStore(ctx, "users", usersSchema())
				return err
			},
		})
		if err != nil {
			t.Fatalf("Open(%s): %v", name, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	alpha := openWithUsers("alpha")
	beta := openWithUsers("beta")
	if err := databaseStore(t, alpha).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("same", "alpha@example.com", "Alpha")}); err != nil {
		t.Fatalf("alpha Add: %v", err)
	}
	if err := databaseStore(t, beta).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("same", "beta@example.com", "Beta")}); err != nil {
		t.Fatalf("beta Add: %v", err)
	}

	alphaRecord, err := databaseStore(t, alpha).Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: "users", ID: "same"})
	if err != nil {
		t.Fatalf("alpha Get: %v", err)
	}
	betaRecord, err := databaseStore(t, beta).Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: "users", ID: "same"})
	if err != nil {
		t.Fatalf("beta Get: %v", err)
	}
	if alphaRecord["email"] == betaRecord["email"] {
		t.Fatalf("logical databases leaked record: alpha=%v beta=%v", alphaRecord, betaRecord)
	}

	databases, err := factory.Databases(ctx)
	if err != nil {
		t.Fatalf("Databases: %v", err)
	}
	if len(databases) != 2 || databases[0].Name != "alpha" || databases[1].Name != "beta" {
		t.Fatalf("Databases = %+v, want alpha and beta", databases)
	}
}

func TestFactoryRefreshesCachedStateAcrossFactories(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "shared-factory.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	defer first.Close()
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	version1 := uint64(1)
	db, err := first.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("first Open v1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	version2 := uint64(2)
	upgraded, err := second.Open(ctx, "app", OpenOptions{Version: &version2})
	if err != nil {
		t.Fatalf("second Open v2: %v", err)
	}
	if err := upgraded.Close(); err != nil {
		t.Fatalf("Close v2: %v", err)
	}

	reopened, err := first.OpenCurrent(ctx, "app")
	if err != nil {
		t.Fatalf("first OpenCurrent after external upgrade: %v", err)
	}
	if reopened.Version() != 2 {
		t.Fatalf("reopened version = %d, want 2", reopened.Version())
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	if err := second.DeleteDatabase(ctx, "app"); err != nil {
		t.Fatalf("second DeleteDatabase: %v", err)
	}
	created, err := first.OpenCurrent(ctx, "app")
	if err != nil {
		t.Fatalf("first OpenCurrent after external delete: %v", err)
	}
	defer created.Close()
	names, err := created.ObjectStoreNames(ctx)
	if err != nil {
		t.Fatalf("ObjectStoreNames after recreate: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("object stores after external delete/recreate = %v, want none", names)
	}
}

func TestFactoryBlocksUpgradeAcrossFactories(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "cross-factory-blocked.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	defer first.Close()
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	version1 := uint64(1)
	firstDB, err := first.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("first Open v1: %v", err)
	}

	version2 := uint64(2)
	if _, err := second.Open(ctx, "app", OpenOptions{Version: &version2}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second Open v2 while first handle is open error = %v, want FailedPrecondition", err)
	}

	var blocked BlockedInfo
	secondDB, err := second.Open(ctx, "app", OpenOptions{
		Version: &version2,
		OnBlocked: func(ctx context.Context, info BlockedInfo) (BlockedAction, error) {
			blocked = info
			if err := firstDB.Close(); err != nil {
				return BlockedFail, err
			}
			return BlockedWait, nil
		},
	})
	if err != nil {
		t.Fatalf("second Open v2 after closing first handle: %v", err)
	}
	defer secondDB.Close()
	if blocked.OpenConnections != 1 || blocked.OldVersion != 1 || blocked.NewVersion == nil || *blocked.NewVersion != 2 {
		t.Fatalf("blocked info = %+v, want one cross-factory open connection for 1 -> 2", blocked)
	}
}

func TestFactoryVersionChangeCallbackRunsAcrossFactories(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "cross-factory-versionchange.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	defer first.Close()
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	version1 := uint64(1)
	var firstDB Database
	firstDB, err = first.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
		OnVersionChange: func(context.Context, VersionChangeInfo) error {
			return firstDB.Close()
		},
	})
	if err != nil {
		t.Fatalf("first Open v1: %v", err)
	}

	version2 := uint64(2)
	secondDB, err := second.Open(ctx, "app", OpenOptions{Version: &version2})
	if err != nil {
		t.Fatalf("second Open v2 after cross-factory versionchange close: %v", err)
	}
	defer secondDB.Close()
	if secondDB.Version() != 2 {
		t.Fatalf("secondDB.Version() = %d, want 2", secondDB.Version())
	}
}

func TestFactoryActiveTransactionBlocksAcrossFactoriesAfterDatabaseClose(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "cross-factory-active-tx.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	defer first.Close()
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	version1 := uint64(1)
	firstDB, err := first.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("first Open v1: %v", err)
	}
	tx, err := firstDB.Transaction(ctx, []string{"users"}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("Close with active transaction: %v", err)
	}

	version2 := uint64(2)
	if _, err := second.Open(ctx, "app", OpenOptions{Version: &version2}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second Open v2 with active cross-factory tx error = %v, want FailedPrecondition", err)
	}

	var blocked BlockedInfo
	secondDB, err := second.Open(ctx, "app", OpenOptions{
		Version: &version2,
		OnBlocked: func(ctx context.Context, info BlockedInfo) (BlockedAction, error) {
			blocked = info
			if err := tx.Commit(ctx); err != nil {
				return BlockedFail, err
			}
			return BlockedWait, nil
		},
	})
	if err != nil {
		t.Fatalf("second Open v2 after committing active tx: %v", err)
	}
	defer secondDB.Close()
	if blocked.ActiveTransactions != 1 || blocked.OpenConnections != 0 {
		t.Fatalf("blocked info = %+v, want one active transaction and no open connections", blocked)
	}
}

func TestFactoryActiveTransactionBlocksAcrossFactoriesAfterFactoryClose(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "cross-factory-active-tx-factory-close.sqlite")
	first, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(first): %v", err)
	}
	second, err := OpenFactoryDSN(dsn, Options{})
	if err != nil {
		t.Fatalf("OpenFactoryDSN(second): %v", err)
	}
	defer second.Close()

	version1 := uint64(1)
	firstDB, err := first.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("first Open v1: %v", err)
	}
	tx, err := firstDB.Transaction(ctx, []string{"users"}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close with active transaction: %v", err)
	}

	version2 := uint64(2)
	if _, err := second.Open(ctx, "app", OpenOptions{Version: &version2}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second Open v2 with active tx from closed factory error = %v, want FailedPrecondition", err)
	}

	var blocked BlockedInfo
	secondDB, err := second.Open(ctx, "app", OpenOptions{
		Version: &version2,
		OnBlocked: func(ctx context.Context, info BlockedInfo) (BlockedAction, error) {
			blocked = info
			if err := tx.Commit(ctx); err != nil {
				return BlockedFail, err
			}
			return BlockedWait, nil
		},
	})
	if err != nil {
		t.Fatalf("second Open v2 after committing tx from closed factory: %v", err)
	}
	defer secondDB.Close()
	if blocked.ActiveTransactions != 1 || blocked.OpenConnections != 0 {
		t.Fatalf("blocked info = %+v, want one active transaction and no open connections", blocked)
	}
}

func TestFactoryUpgradeRollbackOnCreateIndexConflict(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			_, err := up.CreateObjectStore(ctx, "users", usersSchema())
			return err
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	if err := databaseStore(t, db).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u1", "a@example.com", "Duplicate")}); err != nil {
		t.Fatalf("Add u1: %v", err)
	}
	if err := databaseStore(t, db).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u2", "b@example.com", "Duplicate")}); err != nil {
		t.Fatalf("Add u2: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	version2 := uint64(2)
	_, err = factory.Open(ctx, "app", OpenOptions{
		Version: &version2,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			store, err := up.ObjectStore("users")
			if err != nil {
				return err
			}
			return store.CreateIndex(ctx, "by_name", []string{"name"}, IndexParameters{Unique: true})
		},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("Open v2 unique conflict error = %v, want Aborted", err)
	}

	current, err := factory.OpenCurrent(ctx, "app")
	if err != nil {
		t.Fatalf("OpenCurrent after failed upgrade: %v", err)
	}
	defer current.Close()
	if current.Version() != 1 {
		t.Fatalf("version after failed upgrade = %d, want 1", current.Version())
	}
	if _, err := databaseStore(t, current).IndexGet(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store: "users", Index: "by_name", Values: []any{"Duplicate"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("IndexGet after failed upgrade error = %v, want NotFound", err)
	}
}

func TestFactoryCreateIndexAllowsOpaqueEmptyName(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			store, err := up.CreateObjectStore(ctx, "users", usersSchema())
			if err != nil {
				return err
			}
			return store.CreateIndex(ctx, "", []string{"email"}, IndexParameters{Unique: true})
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	defer db.Close()

	if err := databaseStore(t, db).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u1", "a@example.com", "Alice")}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	record, err := databaseStore(t, db).IndexGet(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "users", Index: "", Values: []any{"a@example.com"}})
	if err != nil {
		t.Fatalf("IndexGet empty index name: %v", err)
	}
	if got := record["id"]; got != "u1" {
		t.Fatalf("IndexGet id = %v, want u1", got)
	}
}

func TestFactoryUpgradeDeleteIndexCleansRows(t *testing.T) {
	ctx := context.Background()
	factory := testFactory(t)

	version1 := uint64(1)
	db, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version1,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			store, err := up.CreateObjectStore(ctx, "users", usersSchema())
			if err != nil {
				return err
			}
			return store.CreateIndex(ctx, "by_email", []string{"email"}, IndexParameters{Unique: true})
		},
	})
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	if err := databaseStore(t, db).Add(ctx, gestalt.IndexedDBRecordRequest{Store: "users", Record: userRecord("u1", "a@example.com", "Alice")}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := databaseStore(t, db).IndexGet(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "users", Index: "by_email", Values: []any{"a@example.com"}}); err != nil {
		t.Fatalf("IndexGet before delete index: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	version2 := uint64(2)
	upgraded, err := factory.Open(ctx, "app", OpenOptions{
		Version: &version2,
		Upgrade: func(ctx context.Context, up UpgradeContext) error {
			store, err := up.ObjectStore("users")
			if err != nil {
				return err
			}
			return store.DeleteIndex(ctx, "by_email")
		},
	})
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	defer upgraded.Close()
	if _, err := databaseStore(t, upgraded).IndexGet(ctx, gestalt.IndexedDBIndexQueryRequest{Store: "users", Index: "by_email", Values: []any{"a@example.com"}}); status.Code(err) != codes.NotFound {
		t.Fatalf("IndexGet after delete index error = %v, want NotFound", err)
	}
}
