package relationaldb

import (
	"context"

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
	entry, _, err := c.ContinueNext()
	return entry, err
}

func (c *relationalCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	entry, _, err := c.Snapshot.ContinueToKey(key)
	return entry, err
}

func (c *relationalCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
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
