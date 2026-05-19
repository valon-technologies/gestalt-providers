package relationaldb

import (
	"context"
	"fmt"
	"sort"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relationalCursor struct {
	cursorutil.LazyCursor
	store            *Store
	storeName        string
	meta             *storeMeta
	index            *gestalt.IndexSchema
	req              gestalt.IndexedDBOpenCursorRequest
	page             []cursorutil.Entry
	sourceKey        any
	sourcePrimaryKey any
	sourceStarted    bool
	exhausted        bool
}

const relationalCursorPageSize = 100

func (c *relationalCursor) SnapshotState() *cursorutil.Snapshot {
	return &c.LazyCursor.Snapshot
}

func (s *Store) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	return s.openCursor(ctx, req)
}

func (s *Store) openCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (*relationalCursor, error) {
	meta, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}

	cursor := &relationalCursor{
		LazyCursor: cursorutil.NewLazyCursor(req),
		store:      s,
		storeName:  req.Store,
		meta:       meta,
		req:        req,
	}
	if cursor.IndexCursor {
		cursor.index = findIndex(meta, req.Index)
		if cursor.index == nil {
			return nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
		}
	}
	return cursor, nil
}

func (c *relationalCursor) Next(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.Next(ctx, c.nextEntry)
}

func (c *relationalCursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.ContinueToKey(ctx, key, c.nextEntry)
}

func (c *relationalCursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	return c.LazyCursor.Advance(ctx, count, c.nextEntry)
}

func (c *relationalCursor) Delete(ctx context.Context) error {
	return c.DeleteCurrent(ctx)
}

func (c *relationalCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	return c.UpdateCurrent(ctx, record)
}

func (c *relationalCursor) Close() error {
	c.page = nil
	c.exhausted = true
	return nil
}

func (c *relationalCursor) nextEntry(ctx context.Context) (*cursorutil.Entry, error) {
	for len(c.page) == 0 {
		if c.exhausted {
			return nil, nil
		}
		if err := c.loadPage(ctx); err != nil {
			return nil, err
		}
		if len(c.page) == 0 {
			c.exhausted = true
			return nil, nil
		}
	}

	entry := c.page[0]
	c.page = c.page[1:]
	c.sourceKey = entry.Key
	c.sourcePrimaryKey = entry.PrimaryKeyValue
	c.sourceStarted = true
	return &entry, nil
}

func (c *relationalCursor) loadPage(ctx context.Context) error {
	var (
		candidates []relationalCursorCandidate
		err        error
	)
	if c.IndexCursor {
		candidates, err = c.collectIndexPage(ctx)
	} else {
		candidates, err = c.collectObjectStorePage(ctx)
	}
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		c.page = nil
		return nil
	}
	entries, err := c.materializeCandidates(ctx, candidates)
	if err != nil {
		return err
	}
	c.page = entries
	return nil
}

type relationalCursorCandidate struct {
	entry      cursorutil.Entry
	recordBlob []byte
	pkHash     []byte
	pkBytes    []byte
}

func (c *relationalCursor) collectObjectStorePage(ctx context.Context) ([]relationalCursorCandidate, error) {
	rows, err := c.store.query(ctx,
		"SELECT "+quoteIdent(c.store.dialect, "pk_hash")+", "+
			quoteIdent(c.store.dialect, "pk_bytes")+", "+
			quoteIdent(c.store.dialect, "record_blob")+
			" FROM "+quoteTableName(c.store.dialect, c.store.genericRecordsTable())+
			" WHERE "+quoteIdent(c.store.dialect, "store_name")+" = ?",
		c.storeName,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open cursor page: %v", err)
	}
	defer rows.Close()

	var page []relationalCursorCandidate
	for rows.Next() {
		var row genericRecordRow
		if err := rows.Scan(&row.pkHash, &row.pkBytes, &row.recordBlob); err != nil {
			return nil, status.Errorf(codes.Internal, "scan cursor page: %v", err)
		}
		candidate, ok, err := c.objectStoreCandidate(row)
		if err != nil {
			return nil, err
		}
		if ok {
			page = c.addCandidate(page, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate cursor page: %v", err)
	}
	return page, nil
}

func (c *relationalCursor) objectStoreCandidate(row genericRecordRow) (relationalCursorCandidate, bool, error) {
	primaryKeyValue, err := decodeKeyValue(row.pkBytes)
	if err != nil {
		return relationalCursorCandidate{}, false, err
	}
	entry := cursorutil.Entry{
		Key:             primaryKeyValue,
		PrimaryKey:      fmt.Sprint(primaryKeyValue),
		PrimaryKeyValue: primaryKeyValue,
	}
	if ok, err := c.entryEligible(entry); err != nil || !ok {
		return relationalCursorCandidate{}, false, err
	}
	return relationalCursorCandidate{entry: entry, recordBlob: cloneBytes(row.recordBlob)}, true, nil
}

func (c *relationalCursor) collectIndexPage(ctx context.Context) ([]relationalCursorCandidate, error) {
	page, err := c.collectIndexTablePage(ctx, c.store.genericIndexTable(), nil)
	if err != nil {
		return nil, err
	}
	return c.collectIndexTablePage(ctx, c.store.genericUniqueIndexTable(), page)
}

func (c *relationalCursor) collectIndexTablePage(ctx context.Context, table string, page []relationalCursorCandidate) ([]relationalCursorCandidate, error) {
	rows, err := c.store.query(ctx,
		"SELECT "+quoteIdent(c.store.dialect, "index_name")+", "+
			quoteIdent(c.store.dialect, "index_key_hash")+", "+
			quoteIdent(c.store.dialect, "index_key_bytes")+", "+
			quoteIdent(c.store.dialect, "pk_hash")+", "+
			quoteIdent(c.store.dialect, "pk_bytes")+
			" FROM "+quoteTableName(c.store.dialect, table)+
			" WHERE "+quoteIdent(c.store.dialect, "store_name")+" = ? AND "+
			quoteIdent(c.store.dialect, "index_name")+" = ?",
		c.storeName,
		c.index.Name,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open cursor page: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row genericIndexRow
		if err := rows.Scan(&row.indexName, &row.indexKeyHash, &row.indexKeyBytes, &row.pkHash, &row.pkBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "scan cursor page: %v", err)
		}
		candidate, ok, err := c.indexCandidate(row)
		if err != nil {
			return nil, err
		}
		if ok {
			page = c.addCandidate(page, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate cursor page: %v", err)
	}
	return page, nil
}

func (c *relationalCursor) indexCandidate(row genericIndexRow) (relationalCursorCandidate, bool, error) {
	indexKeyValue, err := decodeKeyValue(row.indexKeyBytes)
	if err != nil {
		return relationalCursorCandidate{}, false, err
	}
	primaryKeyValue, err := decodeKeyValue(row.pkBytes)
	if err != nil {
		return relationalCursorCandidate{}, false, err
	}
	entry := cursorutil.Entry{
		Key:             normalizeDocumentBound(indexKeyValue),
		PrimaryKey:      fmt.Sprint(primaryKeyValue),
		PrimaryKeyValue: primaryKeyValue,
	}
	filtered, err := filterEntriesByPrefix([]cursorutil.Entry{entry}, c.req.Values)
	if err != nil || len(filtered) == 0 {
		return relationalCursorCandidate{}, false, err
	}
	if ok, err := c.entryEligible(entry); err != nil || !ok {
		return relationalCursorCandidate{}, false, err
	}
	return relationalCursorCandidate{
		entry:   entry,
		pkHash:  cloneBytes(row.pkHash),
		pkBytes: cloneBytes(row.pkBytes),
	}, true, nil
}

func (c *relationalCursor) entryEligible(entry cursorutil.Entry) (bool, error) {
	if c.sourceStarted && !c.entryAfterSource(entry) {
		return false, nil
	}
	filtered, err := c.ApplyRange([]cursorutil.Entry{entry}, c.req.Range)
	if err != nil {
		return false, err
	}
	return len(filtered) != 0, nil
}

func (c *relationalCursor) entryAfterSource(entry cursorutil.Entry) bool {
	cmp := compareRelationalCursorPosition(entry.Key, entry.PrimaryKeyValue, c.sourceKey, c.sourcePrimaryKey)
	if c.Reverse {
		return cmp < 0
	}
	return cmp > 0
}

func (c *relationalCursor) addCandidate(page []relationalCursorCandidate, candidate relationalCursorCandidate) []relationalCursorCandidate {
	page = append(page, candidate)
	c.sortCandidates(page)
	if len(page) > relationalCursorPageSize {
		page = page[:relationalCursorPageSize]
	}
	return page
}

func (c *relationalCursor) sortCandidates(page []relationalCursorCandidate) {
	sort.Slice(page, func(i, j int) bool {
		cmp := compareRelationalCursorEntries(page[i].entry, page[j].entry)
		if c.Reverse {
			return cmp > 0
		}
		return cmp < 0
	})
}

func (c *relationalCursor) materializeCandidates(ctx context.Context, candidates []relationalCursorCandidate) ([]cursorutil.Entry, error) {
	entries := make([]cursorutil.Entry, 0, len(candidates))
	for _, candidate := range candidates {
		entry := candidate.entry
		if !c.KeysOnly {
			record, err := c.candidateRecord(ctx, candidate)
			if err != nil {
				return nil, err
			}
			entry.Record = record
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (c *relationalCursor) candidateRecord(ctx context.Context, candidate relationalCursorCandidate) (gestalt.Record, error) {
	if !c.IndexCursor {
		return unmarshalRecordBlob(candidate.recordBlob)
	}
	recordRow, err := c.store.loadGenericRecordByPrimaryDirect(ctx, c.storeName, candidate.pkHash, candidate.pkBytes)
	if err != nil {
		return nil, err
	}
	if recordRow == nil {
		return nil, status.Error(codes.Internal, "index row points to missing record")
	}
	return unmarshalRecordBlob(recordRow.recordBlob)
}

func compareRelationalCursorEntries(a, b cursorutil.Entry) int {
	return compareRelationalCursorPosition(a.Key, a.PrimaryKeyValue, b.Key, b.PrimaryKeyValue)
}

func compareRelationalCursorPosition(aKey, aPrimary, bKey, bPrimary any) int {
	if cmp := cursorutil.CompareValues(aKey, bKey); cmp != 0 {
		return cmp
	}
	return cursorutil.CompareValues(aPrimary, bPrimary)
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
