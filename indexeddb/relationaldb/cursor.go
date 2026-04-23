package relationaldb

import (
	"context"
	"fmt"

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
	meta, err := s.getMeta(req.GetStore())
	if err != nil {
		return nil, err
	}

	cursor := &relationalCursor{
		Snapshot:  cursorutil.NewSnapshot(req),
		store:     s,
		storeName: req.GetStore(),
		meta:      meta,
	}
	if cursor.IndexCursor {
		cursor.index = findIndex(meta, req.GetIndex())
		if cursor.index == nil {
			return nil, status.Errorf(codes.NotFound, "index not found: %s", req.GetIndex())
		}
	}

	records, err := s.cursorRecords(ctx, cursor, req)
	if err != nil {
		return nil, err
	}
	entries, err := cursorutil.EntriesFromRecords(records, cursor.entryFromRecord, nil)
	if err != nil {
		return nil, err
	}
	if err := cursor.Load(entries, req.GetRange()); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (s *Store) cursorRecords(ctx context.Context, cursor *relationalCursor, req *proto.OpenCursorRequest) ([]*proto.Record, error) {
	if isDocumentStore(cursor.meta) {
		if req.GetIndex() == "" {
			resp, err := s.GetAll(ctx, &proto.ObjectStoreRangeRequest{Store: req.GetStore(), Range: req.GetRange()})
			if err != nil {
				return nil, err
			}
			return resp.GetRecords(), nil
		}
		resp, err := s.IndexGetAll(ctx, &proto.IndexQueryRequest{
			Store:  req.GetStore(),
			Index:  req.GetIndex(),
			Values: req.GetValues(),
			Range:  req.GetRange(),
		})
		if err != nil {
			return nil, err
		}
		return resp.GetRecords(), nil
	}

	if !cursor.KeysOnly {
		if req.GetIndex() == "" {
			resp, err := s.GetAll(ctx, &proto.ObjectStoreRangeRequest{Store: req.GetStore()})
			if err != nil {
				return nil, err
			}
			return resp.GetRecords(), nil
		}

		resp, err := s.IndexGetAll(ctx, &proto.IndexQueryRequest{
			Store:  req.GetStore(),
			Index:  req.GetIndex(),
			Values: req.GetValues(),
		})
		if err != nil {
			return nil, err
		}
		return resp.GetRecords(), nil
	}

	cols := cursorRecordColumns(cursor)
	if req.GetIndex() == "" {
		query, args, err := selectColumnsWithRange(s.dialect, cursor.meta, req.GetRange(), cols)
		if err != nil {
			return nil, err
		}
		rows, err := s.query(ctx, query, args...)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "open_cursor get_all scan: %v", err)
		}
		defer rows.Close()
		records, err := scanRows(rows, cols)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "open_cursor get_all scan: %v", err)
		}
		return records, nil
	}

	query, args, err := indexSelectQuery(s.dialect, cursor.meta, &proto.IndexQueryRequest{
		Store:  req.GetStore(),
		Index:  req.GetIndex(),
		Values: req.GetValues(),
	}, false, colList(s.dialect, cols))
	if err != nil {
		return nil, err
	}
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open_cursor index_get_all: %v", err)
	}
	defer rows.Close()
	records, err := scanRows(rows, cols)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open_cursor index_get_all scan: %v", err)
	}
	return records, nil
}

func cursorRecordColumns(cursor *relationalCursor) []*proto.ColumnDef {
	seen := map[string]struct{}{cursor.meta.pkCol: {}}
	names := []string{cursor.meta.pkCol}
	if cursor.IndexCursor {
		for _, name := range cursor.index.GetKeyPath() {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}

	cols := make([]*proto.ColumnDef, 0, len(names))
	for _, col := range cursor.meta.columns {
		if _, ok := seen[col.Name]; ok {
			cols = append(cols, col)
		}
	}
	return cols
}

func selectColumnsWithRange(d dialect, m *storeMeta, kr *proto.KeyRange, cols []*proto.ColumnDef) (string, []any, error) {
	if len(cols) == 0 {
		return "", nil, status.Error(codes.InvalidArgument, "cursor columns are required")
	}

	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", colList(d, cols), quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", colList(d, cols), quoteTableName(d, m.table), where), args, nil
}

func (c *relationalCursor) entryFromRecord(record *proto.Record) (cursorutil.Entry, error) {
	primaryKeyValue, err := recordFieldAny(record, c.meta.pkCol)
	if err != nil {
		return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
	}
	key := primaryKeyValue
	if c.IndexCursor {
		indexKey, ok, err := indexKeyFromRecord(record, c.index)
		if err != nil {
			return cursorutil.Entry{}, status.Errorf(codes.InvalidArgument, "record index key: %v", err)
		}
		if !ok {
			return cursorutil.Entry{}, status.Error(codes.InvalidArgument, "record index key is missing")
		}
		key = normalizeDocumentBound(indexKey)
	}

	return cursorutil.Entry{
		Key:             key,
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
	return c.store.deleteByPrimaryKeyValue(ctx, c.meta, entry.PrimaryKeyValue)
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

	if _, err := c.store.Put(ctx, &proto.RecordRequest{
		Store:  c.storeName,
		Record: cloned,
	}); err != nil {
		return nil, err
	}

	// Preserve the cursor's existing key/range ordering after in-place updates.
	c.Entries[c.Pos].Record = cloned
	return c.CurrentEntry()
}
