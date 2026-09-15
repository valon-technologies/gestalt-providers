package relationaldb

import (
	"context"
	"fmt"
)

// Migrate provisions the physical tables and indexes used by a Store.
func Migrate(ctx context.Context, dsn string, options Options) error {
	store, err := migrateAndOpenStore(ctx, dsn, options.storeOptions())
	if err != nil {
		return err
	}
	return store.Close()
}

func migrateAndOpenStore(ctx context.Context, dsn string, options storeOptions) (*Store, error) {
	if err := ensureRelationalTargetExists(ctx, dsn, options); err != nil {
		return nil, err
	}
	store, err := connectStoreWithOptions(ctx, dsn, options)
	if err != nil {
		return nil, err
	}
	if err := migrateStore(ctx, store); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

func migrateStore(ctx context.Context, store *Store) error {
	if _, err := execWithRetry(ctx, store.db, store.conn, store.q(metadataTableSQL(store.dialect, store.metadataTable()))); err != nil {
		return fmt.Errorf("relationaldb: create metadata table: %w", err)
	}
	if err := store.ensureGenericTables(ctx); err != nil {
		return err
	}
	if err := store.dropObsoleteScanIndexes(ctx); err != nil {
		return err
	}
	if err := store.validateGenericTables(ctx); err != nil {
		return fmt.Errorf("relationaldb: validate migrated physical schema: %w", err)
	}
	return nil
}

// New ordered indexes replace the old prefix-only scans. In particular, the
// old PostgreSQL index must not keep rejecting long secondary keys after upgrade.
func (s *Store) dropObsoleteScanIndexes(ctx context.Context) error {
	for _, index := range []struct{ table, suffix string }{
		{s.genericRecordsTable(), "primary_order"},
		{s.genericIndexTable(), "scan"},
		{s.genericUniqueIndexTable(), "scan"},
	} {
		name := portableIndexName(index.table, index.suffix)
		stmt := "DROP INDEX IF EXISTS " + quoteTableName(s.dialect, qualifyTableName(s.schemaName, name))
		switch s.dialect {
		case dialectMySQL:
			var exists int
			if err := s.scanOne(ctx, "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ? AND index_name = ?", []any{s.schemaName, baseTableName(index.table), name}, &exists); err != nil {
				return err
			}
			if exists == 0 {
				continue
			}
			stmt = "DROP INDEX " + quoteIdent(s.dialect, name) + " ON " + quoteTableName(s.dialect, index.table)
		case dialectSQLServer:
			stmt = "DROP INDEX IF EXISTS " + quoteIdent(s.dialect, name) + " ON " + quoteTableName(s.dialect, index.table)
		}
		if _, err := s.exec(ctx, stmt); err != nil {
			return fmt.Errorf("remove obsolete scan index %s: %w", name, err)
		}
	}
	return nil
}
