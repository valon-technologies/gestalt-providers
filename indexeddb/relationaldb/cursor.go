package relationaldb

import (
	"context"
	"database/sql"
	"fmt"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relationalCursor struct {
	cursorutil.Snapshot
	store     *Store
	storeName string
	meta      *storeMeta
	index     *gestalt.IndexSchema
	lazy      bool
	keyRange  *gestalt.KeyRange
	seekKey   any
	lastKey   any
}

func (c *relationalCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.Snapshot
}

func (s *Store) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	return s.openCursorSnapshot(ctx, req)
}

func (s *Store) openCursorSnapshot(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*relationalCursor, error) {
	meta, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}

	cursor := &relationalCursor{
		Snapshot:  cursorutil.NewSnapshot(req),
		store:     s,
		storeName: req.Store,
		meta:      meta,
	}
	if cursor.IndexCursor {
		cursor.index = findIndex(meta, req.Index)
		if cursor.index == nil {
			return nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
		}
	}
	if canUseLazyRelationalObjectCursor(req, meta) {
		cursor.lazy = true
		cursor.keyRange = req.Range
		return cursor, nil
	}

	var entries []cursorutil.Entry
	if cursor.IndexCursor {
		entries, err = s.genericIndexEntries(ctx, req.Store, meta, cursor.index, req.Values, req.Range, cursor.KeysOnly)
	} else {
		entries, err = s.genericObjectStoreEntries(ctx, req.Store, meta, req.Range, cursor.KeysOnly)
	}
	if err != nil {
		return nil, err
	}
	if err := cursor.Load(entries, nil); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (c *relationalCursor) Next(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		return c.nextLazyObject(ctx)
	}
	entry, _, err := c.ContinueNext()
	return entry, err
}

func (c *relationalCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		c.seekKey = key
		return c.nextLazyObject(ctx)
	}
	entry, _, err := c.Snapshot.ContinueToKey(key)
	return entry, err
}

func (c *relationalCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	if c.lazy {
		if count <= 0 {
			return nil, status.Error(codes.InvalidArgument, "advance count must be positive")
		}
		var entry *gestalt.IndexedDBCursorEntry
		var err error
		for i := 0; i < count; i++ {
			entry, err = c.Next(ctx)
			if entry == nil || err != nil {
				return entry, err
			}
		}
		return entry, nil
	}
	entry, _, err := c.Snapshot.Advance(count)
	return entry, err
}

func (c *relationalCursor) Delete(ctx context.Context) error {
	return c.DeleteCurrent(ctx)
}

func (c *relationalCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	return c.UpdateCurrent(ctx, record)
}

func (c *relationalCursor) Close() error {
	return nil
}

func canUseLazyRelationalObjectCursor(req gestalt.IndexedDBOpenCursorRequest, meta *storeMeta) bool {
	if req.Index != "" || req.Direction == gestalt.CursorPrev || req.Direction == gestalt.CursorPrevUnique {
		return false
	}
	if len(meta.columns) == 0 {
		return true
	}
	return columnType(meta, meta.pkCol) == int32(gestalt.TypeString)
}

func (c *relationalCursor) nextLazyObject(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	rows, err := c.store.openGenericObjectStoreRows(ctx, c.storeName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var best *cursorutil.Entry
	for rows.Next() {
		var row genericRecordRow
		if err := rows.Scan(&row.pkHash, &row.pkBytes, &row.recordBlob); err != nil {
			return nil, err
		}
		entry, err := c.entryFromGenericRow(row)
		if err != nil {
			return nil, err
		}
		if c.lastKey != nil && cursorutil.CompareValues(entry.Key, c.lastKey) <= 0 {
			continue
		}
		if c.seekKey != nil && cursorutil.CompareValues(entry.Key, c.seekKey) < 0 {
			continue
		}
		filtered, err := c.ApplyRange([]cursorutil.Entry{entry}, c.keyRange)
		if err != nil {
			return nil, err
		}
		if len(filtered) == 0 {
			continue
		}
		if best == nil || cursorutil.CompareValues(entry.Key, best.Key) < 0 {
			selected := entry
			best = &selected
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	c.seekKey = nil
	if best == nil {
		return nil, nil
	}
	c.lastKey = best.Key
	c.Entries = []cursorutil.Entry{*best}
	c.Pos = 0
	return c.CurrentEntry()
}

func (s *Store) openGenericObjectStoreRows(ctx context.Context, store string) (*sql.Rows, error) {
	query := "SELECT " + quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") +
		" FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ?" +
		" ORDER BY " + quoteIdent(s.dialect, "pk_bytes") + " ASC"
	return s.query(ctx, query, store)
}

func (c *relationalCursor) entryFromGenericRow(row genericRecordRow) (cursorutil.Entry, error) {
	primaryKeyValue, err := decodeKeyValue(row.pkBytes)
	if err != nil {
		return cursorutil.Entry{}, err
	}
	var record gestalt.Record
	if !c.KeysOnly {
		record, err = unmarshalRecordBlob(row.recordBlob)
		if err != nil {
			return cursorutil.Entry{}, err
		}
	}
	return cursorutil.Entry{
		Key:             primaryKeyValue,
		PrimaryKey:      fmt.Sprint(primaryKeyValue),
		PrimaryKeyValue: primaryKeyValue,
		Record:          record,
	}, nil
}

func (c *relationalCursor) DeleteCurrent(ctx context.Context) error {
	entry, err := c.Current()
	if err != nil {
		return err
	}
	return c.store.deleteGenericByValue(ctx, c.storeName, entry.PrimaryKeyValue)
}

func (c *relationalCursor) UpdateCurrent(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	entry, err := c.Current()
	if err != nil {
		return nil, err
	}
	cloned, err := cursorutil.CloneRecordWithField(record, c.meta.pkCol, entry.PrimaryKeyValue)
	if err != nil {
		return nil, err
	}
	if err := c.store.Put(ctx, gestalt.IndexedDBRecordRequest{
		Store:  c.storeName,
		Record: cloned,
	}); err != nil {
		return nil, err
	}

	c.Entries[c.Pos].Record = cloned
	return c.CurrentEntry()
}
