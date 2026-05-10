package relationaldb

import (
	"context"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relationalCursor struct {
	cursorutil.Snapshot
	store     *Store
	storeName string
	meta      *storeMeta
	index     *proto.IndexSchema
}

func (c *relationalCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.Snapshot
}

func (s *Store) OpenCursor(stream grpc.BidiStreamingServer[proto.CursorClientMessage, proto.CursorResponse]) error {
	return cursorutil.Serve(stream, func(ctx context.Context, req *proto.OpenCursorRequest) (cursorutil.Runtime, error) {
		return s.openCursorSnapshot(ctx, req)
	})
}

func (s *Store) openCursorSnapshot(ctx context.Context, req *proto.OpenCursorRequest) (*relationalCursor, error) {
	meta, err := s.getMetaForContext(ctx, req.GetStore())
	if err != nil {
		return nil, err
	}
	storeName, err := s.scopedStoreName(ctx, req.GetStore())
	if err != nil {
		return nil, err
	}

	cursor := &relationalCursor{
		Snapshot:  cursorutil.NewSnapshot(req),
		store:     s,
		storeName: storeName,
		meta:      meta,
	}
	if cursor.IndexCursor {
		cursor.index = findIndex(meta, req.GetIndex())
		if cursor.index == nil {
			return nil, status.Errorf(codes.NotFound, "index not found: %s", req.GetIndex())
		}
	}

	var entries []cursorutil.Entry
	if cursor.IndexCursor {
		entries, err = s.genericIndexEntries(ctx, storeName, meta, cursor.index, req.GetValues(), req.GetRange(), cursor.KeysOnly)
	} else {
		entries, err = s.genericObjectStoreEntries(ctx, storeName, meta, req.GetRange(), cursor.KeysOnly)
	}
	if err != nil {
		return nil, err
	}
	if err := cursor.Load(entries, nil); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (c *relationalCursor) DeleteCurrent(ctx context.Context) error {
	entry, err := c.Current()
	if err != nil {
		return err
	}
	return c.store.deleteGenericByValue(ctx, c.storeName, entry.PrimaryKeyValue)
}

func (c *relationalCursor) UpdateCurrent(ctx context.Context, record *proto.Record) (*proto.CursorEntry, error) {
	entry, err := c.Current()
	if err != nil {
		return nil, err
	}
	cloned, err := cursorutil.CloneRecordWithField(record, c.meta.pkCol, entry.PrimaryKeyValue)
	if err != nil {
		return nil, err
	}

	if err := c.store.putGeneric(ctx, c.storeName, c.meta, cloned); err != nil {
		return nil, err
	}

	// Preserve the cursor's existing key/range ordering after in-place updates.
	c.Entries[c.Pos].Record = cloned
	return c.CurrentEntry()
}
