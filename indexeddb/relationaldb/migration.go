package relationaldb

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Store) ObjectStoreNames(ctx context.Context) ([]string, error) {
	prefix := ""
	if s.usesNamespacedMetadata() {
		prefix = s.tablePrefix
	}
	keys, err := s.metadataKeysWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(keys))
	for _, name := range keys {
		if prefix != "" {
			name = strings.TrimPrefix(name, prefix)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (s *Store) metadataKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	rows, err := s.query(ctx,
		"SELECT "+quoteIdent(s.dialect, "name")+
			" FROM "+quoteTableName(s.dialect, s.metadataTable()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list object stores: %v", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, status.Errorf(codes.Internal, "scan object store name: %v", err)
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		keys = append(keys, name)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate object store names: %v", err)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) createObjectStoreStrict(ctx context.Context, name string, schema gestalt.ObjectStoreOptions) error {
	if _, ok, err := s.loadStoreMetadata(ctx, name); err != nil {
		return preserveStatusOrInternal("load metadata: %v", err)
	} else if ok {
		return status.Errorf(codes.AlreadyExists, "object store already exists: %s", name)
	}
	if _, inTx := txFromContext(ctx); !inTx {
		if err := s.ensureGenericTables(ctx); err != nil {
			return status.Errorf(codes.Internal, "create generic tables: %v", err)
		}
	}
	return s.persistStoreMetadata(ctx, name, schema)
}

func (s *Store) deleteObjectStoreStrict(ctx context.Context, name string) error {
	if _, ok, err := s.loadStoreMetadata(ctx, name); err != nil {
		return preserveStatusOrInternal("load metadata: %v", err)
	} else if !ok {
		return status.Errorf(codes.NotFound, "object store not found: %s", name)
	}
	if err := s.clearGeneric(ctx, name); err != nil {
		return err
	}
	_, err := s.exec(ctx,
		"DELETE FROM "+quoteTableName(s.dialect, s.metadataTable())+" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
		s.metadataStoreKey(name),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "delete object store metadata: %v", err)
	}
	return nil
}

func (s *Store) createIndexStrict(ctx context.Context, storeName, indexName string, keyPath []string, params IndexParameters) error {
	if params.MultiEntry {
		return status.Error(codes.InvalidArgument, "multiEntry indexes are not supported")
	}
	if len(keyPath) == 0 {
		return status.Error(codes.InvalidArgument, "index key path is required")
	}
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		meta, ok, err := s.loadStoreMetadata(txCtx, storeName)
		if err != nil {
			return preserveStatusOrInternal("load metadata: %v", err)
		}
		if !ok {
			return status.Errorf(codes.NotFound, "object store not found: %s", storeName)
		}
		for _, idx := range meta.indexes {
			if idx.Name == indexName {
				return status.Errorf(codes.AlreadyExists, "index already exists: %s", indexName)
			}
		}

		idx := gestalt.IndexSchema{Name: indexName, KeyPath: append([]string(nil), keyPath...), Unique: params.Unique}
		records, err := s.loadAllGenericRecords(txCtx, storeName)
		if err != nil {
			return err
		}
		for _, row := range records {
			record, err := unmarshalRecordBlob(row.recordBlob)
			if err != nil {
				return err
			}
			indexKey, ok, err := indexKeyFromRecord(record, &idx)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "record index key: %v", err)
			}
			if !ok {
				continue
			}
			encoded, err := encodeKeyValue(indexKey)
			if err != nil {
				return err
			}
			indexRow := genericIndexRow{
				indexName:     idx.Name,
				indexKeyHash:  cloneBytes(encoded.hash),
				indexKeyBytes: cloneBytes(encoded.raw),
				indexKeyOrd:   cloneBytes(encoded.ord),
				pkHash:        cloneBytes(row.pkHash),
				pkBytes:       cloneBytes(row.pkBytes),
			}
			if idx.Unique {
				if err := s.insertGenericUniqueIndexRow(txCtx, tx, storeName, indexRow); err != nil {
					return err
				}
			} else if err := s.insertGenericIndexRows(txCtx, tx, s.genericIndexTable(), storeName, []genericIndexRow{indexRow}); err != nil {
				return err
			}
		}

		schema := schemaFromMeta(meta)
		schema.Indexes = append(schema.Indexes, idx)
		return s.persistStoreMetadata(txCtx, storeName, schema)
	})
}

func (s *Store) deleteIndexStrict(ctx context.Context, storeName, indexName string) error {
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		meta, ok, err := s.loadStoreMetadata(txCtx, storeName)
		if err != nil {
			return preserveStatusOrInternal("load metadata: %v", err)
		}
		if !ok {
			return status.Errorf(codes.NotFound, "object store not found: %s", storeName)
		}
		found := false
		schema := schemaFromMeta(meta)
		nextIndexes := schema.Indexes[:0]
		for _, idx := range schema.Indexes {
			if idx.Name == indexName {
				found = true
				continue
			}
			nextIndexes = append(nextIndexes, idx)
		}
		if !found {
			return status.Errorf(codes.NotFound, "index not found: %s", indexName)
		}
		for _, table := range []string{s.genericIndexTable(), s.genericUniqueIndexTable()} {
			if _, err := tx.ExecContext(txCtx,
				s.q("DELETE FROM "+quoteTableName(s.dialect, table)+
					" WHERE "+quoteIdent(s.dialect, "store_name")+" = ? AND "+quoteIdent(s.dialect, "index_name")+" = ?"),
				storeName,
				indexName,
			); err != nil {
				return status.Errorf(codes.Internal, "delete index rows: %v", err)
			}
		}
		schema.Indexes = nextIndexes
		return s.persistStoreMetadata(txCtx, storeName, schema)
	})
}

func schemaFromMeta(meta *storeMeta) gestalt.ObjectStoreOptions {
	if meta == nil {
		return gestalt.ObjectStoreOptions{}
	}
	return gestalt.ObjectStoreOptions{
		Columns: append([]gestalt.ColumnDef(nil), meta.columns...),
		Indexes: append([]gestalt.IndexSchema(nil), meta.indexes...),
	}
}

type relationalUpgradeContext struct {
	db         Database
	store      *Store
	ctx        context.Context
	oldVersion uint64
	newVersion uint64
}

func (u *relationalUpgradeContext) OldVersion() uint64 { return u.oldVersion }
func (u *relationalUpgradeContext) NewVersion() uint64 { return u.newVersion }
func (u *relationalUpgradeContext) Database() Database { return u.db }

func (u *relationalUpgradeContext) ObjectStoreNames(ctx context.Context) ([]string, error) {
	return u.store.ObjectStoreNames(relationalOperationContext(ctx, u.ctx))
}

func (u *relationalUpgradeContext) ObjectStore(name string) (UpgradeObjectStore, error) {
	if _, err := u.store.getMeta(u.ctx, name); err != nil {
		return nil, err
	}
	return &relationalUpgradeObjectStore{store: u.store, ctx: u.ctx, name: name}, nil
}

func (u *relationalUpgradeContext) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreOptions) (UpgradeObjectStore, error) {
	if err := u.store.createObjectStoreStrict(relationalOperationContext(ctx, u.ctx), name, schema); err != nil {
		return nil, err
	}
	return &relationalUpgradeObjectStore{store: u.store, ctx: u.ctx, name: name}, nil
}

func (u *relationalUpgradeContext) DeleteObjectStore(ctx context.Context, name string) error {
	return u.store.deleteObjectStoreStrict(relationalOperationContext(ctx, u.ctx), name)
}

type upgradeDatabase struct {
	name    string
	version uint64
	store   *Store
	ctx     context.Context
}

func (db *upgradeDatabase) Name() string    { return db.name }
func (db *upgradeDatabase) Version() uint64 { return db.version }
func (db *upgradeDatabase) ObjectStoreNames(ctx context.Context) ([]string, error) {
	return db.store.ObjectStoreNames(relationalOperationContext(ctx, db.ctx))
}
func (db *upgradeDatabase) Transaction(context.Context, []string, gestalt.TransactionMode, gestalt.TransactionOptions) (gestalt.IndexedDBTransaction, error) {
	return nil, status.Error(codes.FailedPrecondition, "transactions cannot be opened during upgrade")
}
func (db *upgradeDatabase) Close() error { return nil }

type relationalUpgradeObjectStore struct {
	store *Store
	ctx   context.Context
	name  string
}

func (s *relationalUpgradeObjectStore) Name() string { return s.name }

func (s *relationalUpgradeObjectStore) Schema() gestalt.ObjectStoreOptions {
	meta, err := s.store.getMeta(s.ctx, s.name)
	if err != nil {
		return gestalt.ObjectStoreOptions{}
	}
	return schemaFromMeta(meta)
}

func (s *relationalUpgradeObjectStore) CreateIndex(ctx context.Context, name string, keyPath []string, params IndexParameters) error {
	return s.store.createIndexStrict(relationalOperationContext(ctx, s.ctx), s.name, name, keyPath, params)
}

func (s *relationalUpgradeObjectStore) DeleteIndex(ctx context.Context, name string) error {
	return s.store.deleteIndexStrict(relationalOperationContext(ctx, s.ctx), s.name, name)
}

func relationalOperationContext(ctx context.Context, txCtx context.Context) context.Context {
	if tx, ok := txFromContext(txCtx); ok {
		next := contextWithTx(ctx, tx, nil)
		if lifecycleBypassFromContext(txCtx) {
			next = contextWithLifecycleBypass(next)
		}
		return next
	}
	return ctx
}
