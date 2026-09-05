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
	if err := store.validateGenericTables(ctx); err != nil {
		return fmt.Errorf("relationaldb: validate migrated physical schema: %w", err)
	}
	return nil
}
