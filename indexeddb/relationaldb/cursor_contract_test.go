package relationaldb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	contracttest "github.com/valon-technologies/gestalt-providers/indexeddb/contracttest"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
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

func (h *relationalContractHarness) NewServer(t *testing.T) (proto.IndexedDBServer, func()) {
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

	meta, err := store.getMeta(storeName)
	if err != nil {
		t.Fatalf("getMeta(%s): %v", storeName, err)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
		quoteTableName(store.dialect, meta.table),
		quoteIdent(store.dialect, meta.pkCol),
		quoteIdent(store.dialect, "status"),
		quoteIdent(store.dialect, "payload"),
	)
	if _, err := store.db.ExecContext(context.Background(), store.q(query), id, status, "not-an-int"); err != nil {
		t.Fatalf("ExecContext(insert unreadable row): %v", err)
	}
}

func makeRelationalContractPrefix() string {
	return fmt.Sprintf("ct_%x_", time.Now().UnixNano())
}
