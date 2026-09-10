package relationaldb

import (
	"context"
	"fmt"
	"sort"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relationalCursor struct {
	cursorutil.LazyCursor
	store     *Store
	storeName string
	meta      *storeMeta
	index     *gestalt.IndexSchema
	page      []cursorutil.Entry
	remaining []relationalCursorCandidate
	loaded    bool
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
	c.remaining = nil
	c.loaded = true
	return nil
}

func (c *relationalCursor) nextEntry(ctx context.Context) (*cursorutil.Entry, error) {
	for len(c.page) == 0 {
		if err := c.loadPage(ctx); err != nil {
			return nil, err
		}
		if len(c.page) == 0 {
			return nil, nil
		}
	}

	entry := c.page[0]
	c.page = c.page[1:]
	return &entry, nil
}

func (c *relationalCursor) loadPage(ctx context.Context) error {
	if !c.loaded {
		var err error
		if c.IndexCursor {
			c.remaining, err = c.collectIndexCandidates(ctx)
		} else {
			c.remaining, err = c.collectObjectStoreCandidates(ctx)
		}
		if err != nil {
			return err
		}
		c.loaded = true
	}
	for len(c.remaining) > 0 {
		end := min(relationalCursorPageSize, len(c.remaining))
		entries, err := c.materializeCandidates(ctx, c.remaining[:end])
		if err != nil {
			return err
		}
		c.remaining = c.remaining[end:]
		if len(entries) > 0 {
			c.page = entries
			return nil
		}
	}
	c.page = nil
	return nil
}

type relationalCursorCandidate struct {
	entry   cursorutil.Entry
	pkHash  []byte
	pkBytes []byte
}

func (c *relationalCursor) collectObjectStoreCandidates(ctx context.Context) ([]relationalCursorCandidate, error) {
	rows, err := c.store.query(ctx,
		"SELECT "+quoteIdent(c.store.dialect, "pk_hash")+", "+
			quoteIdent(c.store.dialect, "pk_bytes")+
			" FROM "+quoteTableName(c.store.dialect, c.store.genericRecordsTable())+
			" WHERE "+quoteIdent(c.store.dialect, "store_name")+" = ?",
		c.storeName,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load cursor keys: %v", err)
	}
	defer rows.Close()

	var candidates []relationalCursorCandidate
	for rows.Next() {
		var row genericRecordRow
		if err := rows.Scan(&row.pkHash, &row.pkBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "scan cursor keys: %v", err)
		}
		candidate, ok, err := c.objectStoreCandidate(row)
		if err != nil {
			return nil, err
		}
		if ok {
			candidates = append(candidates, candidate)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate cursor keys: %v", err)
	}
	c.sortCandidates(candidates)
	return candidates, nil
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
	return relationalCursorCandidate{
		entry:   entry,
		pkHash:  cloneBytes(row.pkHash),
		pkBytes: cloneBytes(row.pkBytes),
	}, true, nil
}

func (c *relationalCursor) collectIndexCandidates(ctx context.Context) ([]relationalCursorCandidate, error) {
	table := c.store.genericIndexTable()
	if c.index.Unique {
		table = c.store.genericUniqueIndexTable()
	}
	lo, hi, loOpen, hiOpen, err := orderedQueryBounds(c.Query)
	if err != nil {
		return nil, err
	}

	var candidates []relationalCursorCandidate
	err = c.store.scanGenericIndexRowsByRange(ctx, table, c.storeName, c.index.Name, lo, hi, loOpen, hiOpen, func(row genericIndexRow) error {
		candidate, ok, err := c.indexCandidate(row)
		if err != nil {
			return err
		}
		if ok {
			candidates = append(candidates, candidate)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.sortCandidates(candidates)
	return candidates, nil
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
		Key:             indexKeyValue,
		PrimaryKey:      fmt.Sprint(primaryKeyValue),
		PrimaryKeyValue: primaryKeyValue,
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
	return indexeddb.MatchQuery(entry.Key, c.Query)
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
	hashes := make([][]byte, len(candidates))
	for i, candidate := range candidates {
		hashes[i] = candidate.pkHash
	}
	records, err := c.store.loadGenericRecordRowsByPKHashes(ctx, c.storeName, hashes, !c.KeysOnly)
	if err != nil {
		return nil, err
	}

	entries := make([]cursorutil.Entry, 0, len(candidates))
	for _, candidate := range candidates {
		entry := candidate.entry
		record, ok := records[genericRecordLookupKey(candidate.pkHash, candidate.pkBytes)]
		if !ok {
			continue
		}
		if !c.KeysOnly {
			var err error
			entry.Record, err = unmarshalRecordBlob(record.recordBlob)
			if err != nil {
				return nil, err
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
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
