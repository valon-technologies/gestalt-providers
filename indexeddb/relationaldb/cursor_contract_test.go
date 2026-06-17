package relationaldb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	contracttest "github.com/valon-technologies/gestalt-providers/indexeddb/contracttest"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type relationalContractHarness struct {
	name             string
	dsn              string
	prefix           string
	unreadableRowCap bool
}

func TestCursorContract(t *testing.T) {
	harnesses := []contracttest.Harness{
		newRelationalContractHarness(t, "SQLite", "file:"+filepath.Join(t.TempDir(), "contract.sqlite"), true),
	}

	if dsn := os.Getenv("GESTALT_TEST_POSTGRES_DSN"); dsn != "" {
		harnesses = append(harnesses, newRelationalContractHarness(t, "Postgres", dsn, false))
	}
	if dsn := os.Getenv("GESTALT_TEST_MYSQL_DSN"); dsn != "" {
		harnesses = append(harnesses, newRelationalContractHarness(t, "MySQL", dsn, false))
	}
	if dsn := os.Getenv("GESTALT_TEST_SQLSERVER_DSN"); dsn != "" {
		harnesses = append(harnesses, newRelationalContractHarness(t, "SQLServer", dsn, false))
	}

	for _, harness := range harnesses {
		harness := harness
		t.Run(harness.Name(), func(t *testing.T) {
			contracttest.Run(t, harness)
		})
	}
}

func newRelationalContractHarness(t *testing.T, name, dsn string, unreadableRowCap bool) contracttest.Harness {
	t.Helper()
	return &relationalContractHarness{
		name:             name,
		dsn:              dsn,
		prefix:           makeRelationalContractPrefix(),
		unreadableRowCap: unreadableRowCap,
	}
}

func (h *relationalContractHarness) Name() string {
	return h.name
}

func (h *relationalContractHarness) Capabilities() contracttest.Capabilities {
	return contracttest.Capabilities{
		TypedPrimaryKeys:     true,
		NestedIndexPaths:     true,
		UnreadablePayloadRow: h.unreadableRowCap,
	}
}

func (h *relationalContractHarness) NewProvider(t *testing.T) (gestalt.IndexedDBProvider, func()) {
	t.Helper()

	provider := New()
	if err := provider.Configure(context.Background(), "", map[string]any{
		"dsn":    h.dsn,
		"prefix": h.prefix,
	}); err != nil {
		t.Fatalf("Configure(%s): %v", h.name, err)
	}

	return provider, func() {
		_ = provider.Close()
	}
}

func (h *relationalContractHarness) InsertUnreadablePayloadRow(t *testing.T, storeName, id, status string) {
	t.Helper()

	store, err := newStoreWithOptions(h.dsn, storeOptions{TablePrefix: h.prefix})
	if err != nil {
		t.Fatalf("newStoreWithOptions(%s): %v", h.name, err)
	}
	defer store.Close()

	if _, err := store.getMeta(context.Background(), storeName); err != nil {
		t.Fatalf("getMeta(%s): %v", storeName, err)
	}

	primary, err := encodeKeyValue(id)
	if err != nil {
		t.Fatalf("encodeKeyValue(id): %v", err)
	}
	indexKey, err := encodeKeyValue(status)
	if err != nil {
		t.Fatalf("encodeKeyValue(status): %v", err)
	}

	recordStmt := fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
		quoteTableName(store.dialect, store.genericRecordsTable()),
		quoteIdent(store.dialect, "store_name"),
		quoteIdent(store.dialect, "pk_hash"),
		quoteIdent(store.dialect, "pk_bytes"),
		quoteIdent(store.dialect, "record_blob"),
	)
	if _, err := store.db.ExecContext(context.Background(), store.q(recordStmt), storeName, primary.hash, primary.raw, []byte("not-a-proto-record")); err != nil {
		t.Fatalf("ExecContext(insert unreadable generic record): %v", err)
	}

	indexStmt := fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?)",
		quoteTableName(store.dialect, store.genericIndexTable()),
		quoteIdent(store.dialect, "store_name"),
		quoteIdent(store.dialect, "index_name"),
		quoteIdent(store.dialect, "index_key_hash"),
		quoteIdent(store.dialect, "index_key_bytes"),
		quoteIdent(store.dialect, "index_key_ord"),
		quoteIdent(store.dialect, "pk_hash"),
		quoteIdent(store.dialect, "pk_bytes"),
	)
	if _, err := store.db.ExecContext(context.Background(), store.q(indexStmt), storeName, "by_status", indexKey.hash, indexKey.raw, indexKey.ord, primary.hash, primary.raw); err != nil {
		t.Fatalf("ExecContext(insert unreadable generic index row): %v", err)
	}

}

func makeRelationalContractPrefix() string {
	return fmt.Sprintf("ct_%x_", time.Now().UnixNano())
}
