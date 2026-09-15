package relationaldb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// Lock metadata before reading it, and hold the lock through the data commit.
// Shared locks keep ordinary writers concurrent; index changes take the same
// rows exclusively so no write can commit using an obsolete index definition.
func (s *Store) lockStoreMetadata(ctx context.Context, names []string, exclusive bool) (map[string]*storeMeta, error) {
	keys := make(map[string]string, len(names))
	for _, name := range names {
		keys[s.metadataStoreKey(name)] = name
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	result := make(map[string]*storeMeta, len(keys))
	for start := 0; start < len(ordered); start += metadataReadBatchSize {
		batch := ordered[start:min(start+metadataReadBatchSize, len(ordered))]
		args := make([]any, len(batch))
		for i, key := range batch {
			args[i] = key
		}
		table := quoteTableName(s.dialect, s.metadataTable())
		where := quoteIdent(s.dialect, "name") + " IN (" + strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",") + ")"
		if exclusive {
			// A no-op UPDATE is a portable exclusive row lock, including on SQLite.
			if _, err := s.exec(ctx, "UPDATE "+table+" SET "+quoteIdent(s.dialect, "name")+" = "+quoteIdent(s.dialect, "name")+" WHERE "+where, args...); err != nil {
				return nil, err
			}
		} else if s.dialect == dialectSQLServer {
			table += " WITH (HOLDLOCK)"
		}
		query := "SELECT " + quoteIdent(s.dialect, "name") + ", " + quoteIdent(s.dialect, "schema_json") + " FROM " + table + " WHERE " + where + " ORDER BY " + quoteIdent(s.dialect, "name")
		// Locking reads see the latest committed schema even under MySQL's default
		// repeatable-read isolation, after waiting for an index build to finish.
		switch s.dialect {
		case dialectMySQL:
			query += " LOCK IN SHARE MODE"
		case dialectPostgres:
			query += " FOR SHARE"
		}
		rows, err := s.query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var key, schema string
			if err := rows.Scan(&key, &schema); err != nil {
				rows.Close()
				return nil, err
			}
			name := keys[key]
			meta, ok := decodeStoreMetadata(name, schema)
			if !ok {
				rows.Close()
				return nil, fmt.Errorf("invalid metadata for store %q", name)
			}
			result[name] = meta
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Store) withStoreWrite(ctx context.Context, name string, fn func(context.Context) error) error {
	if tx, ok := txFromContext(ctx); ok {
		if s.dialect == dialectSQLite {
			meta, err := s.lockStoreMetadata(ctx, []string{name}, false)
			if err != nil {
				return err
			}
			ctx = contextWithTx(ctx, tx, meta)
		}
		return fn(ctx)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		txCtx = contextWithTx(txCtx, tx, nil)
		meta, err := s.lockStoreMetadata(txCtx, []string{name}, false)
		if err != nil {
			return err
		}
		return fn(contextWithTx(txCtx, tx, meta))
	})
}

func (s *Store) changeIndex(ctx context.Context, name string, fn func(context.Context) error) error {
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		txCtx = contextWithTx(txCtx, tx, nil)
		if _, err := s.lockStoreMetadata(txCtx, []string{name}, true); err != nil {
			return err
		}
		return fn(txCtx)
	})
}
