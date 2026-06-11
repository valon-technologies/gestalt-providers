package cursorutil

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Entry = gestalt.IndexedDBCursorSnapshotEntry

type Snapshot struct {
	gestalt.IndexedDBCursorSnapshot
}

type LazyCursor struct {
	Snapshot
	Range     *gestalt.KeyRange
	exhausted bool
}

type NextEntryFunc func(context.Context) (*Entry, error)

type Runtime interface {
	SnapshotState() *Snapshot
	DeleteCurrent(context.Context) error
	UpdateCurrent(context.Context, gestalt.Record) (*gestalt.IndexedDBCursorEntry, error)
}

func NewSnapshot(req gestalt.IndexedDBOpenCursorRequest) Snapshot {
	return Snapshot{IndexedDBCursorSnapshot: gestalt.NewIndexedDBCursorSnapshot(req)}
}

func NewLazyCursor(req gestalt.IndexedDBOpenCursorRequest) LazyCursor {
	return LazyCursor{
		Snapshot: NewSnapshot(req),
		Range:    req.Range,
	}
}

func EntriesFromRecords(records []gestalt.Record, build func(gestalt.Record) (Entry, error), skip func(error) bool) ([]Entry, error) {
	entries := make([]Entry, 0, len(records))
	for _, record := range records {
		entry, err := build(record)
		if err != nil {
			if skip != nil && skip(err) {
				continue
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *Snapshot) Load(entries []Entry, r *gestalt.KeyRange) error {
	return s.IndexedDBCursorSnapshot.Load(entries, r)
}

func (s *Snapshot) ApplyRange(entries []Entry, r *gestalt.KeyRange) ([]Entry, error) {
	return s.IndexedDBCursorSnapshot.ApplyRange(entries, r)
}

func (s *Snapshot) ContinueNext() (*gestalt.IndexedDBCursorEntry, bool, error) {
	entry, err := s.Next()
	return cursorEntry(entry), entry != nil, err
}

func (s *Snapshot) ContinueToKey(target any) (*gestalt.IndexedDBCursorEntry, bool, error) {
	entry, err := s.IndexedDBCursorSnapshot.ContinueToKey(target)
	return cursorEntry(entry), entry != nil, err
}

func (s *Snapshot) Advance(count int) (*gestalt.IndexedDBCursorEntry, bool, error) {
	entry, err := s.IndexedDBCursorSnapshot.Advance(count)
	return cursorEntry(entry), entry != nil, err
}

func (s *Snapshot) Current() (*Entry, error) {
	return s.IndexedDBCursorSnapshot.Current()
}

func (s *Snapshot) CurrentEntry() (*gestalt.IndexedDBCursorEntry, error) {
	entry, err := s.Current()
	if err != nil {
		return nil, err
	}
	return cursorEntry(entry), nil
}

func (c *LazyCursor) Next(ctx context.Context, next NextEntryFunc) (*gestalt.IndexedDBCursorEntry, error) {
	return c.nextMatching(ctx, next, nil, false)
}

func (c *LazyCursor) ContinueToKey(ctx context.Context, target any, next NextEntryFunc) (*gestalt.IndexedDBCursorEntry, error) {
	return c.nextMatching(ctx, next, target, true)
}

func (c *LazyCursor) Advance(ctx context.Context, count int, next NextEntryFunc) (*gestalt.IndexedDBCursorEntry, error) {
	if count <= 0 {
		return nil, gestalt.InvalidArgument("advance count must be positive")
	}
	var entry *gestalt.IndexedDBCursorEntry
	var err error
	for i := 0; i < count; i++ {
		entry, err = c.Next(ctx, next)
		if entry == nil || err != nil {
			return entry, err
		}
	}
	return entry, nil
}

func (c *LazyCursor) nextMatching(ctx context.Context, next NextEntryFunc, target any, hasTarget bool) (*gestalt.IndexedDBCursorEntry, error) {
	if c.exhausted {
		return nil, nil
	}
	var previous *Entry
	if current, err := c.Current(); err == nil {
		previous = current
	}
	for {
		entry, err := next(ctx)
		if entry == nil || err != nil {
			if entry == nil && err == nil {
				c.exhausted = true
			}
			return nil, err
		}
		if ok, err := c.entryInRange(*entry); err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		if previous != nil && c.Unique && c.IndexCursor && CompareValues(entry.Key, previous.Key) == 0 {
			continue
		}
		if hasTarget && !c.entryReachesTarget(*entry, target) {
			continue
		}
		c.Entries = []Entry{*entry}
		c.Pos = 0
		return c.CurrentEntry()
	}
}

func (c *LazyCursor) entryInRange(entry Entry) (bool, error) {
	filtered, err := c.ApplyRange([]Entry{entry}, c.Range)
	if err != nil {
		return false, err
	}
	return len(filtered) != 0, nil
}

func (c *LazyCursor) entryReachesTarget(entry Entry, target any) bool {
	cmp := CompareValues(entry.Key, target)
	if c.Reverse {
		return cmp <= 0
	}
	return cmp >= 0
}

func CloneRecordWithField(record gestalt.Record, field string, value any) (gestalt.Record, error) {
	return gestalt.CloneIndexedDBRecordWithField(record, field, value)
}

func DirectRecordField(record gestalt.Record, field string) (any, error) {
	return gestalt.IndexedDBRecordField(record, field)
}

func RangeBounds(r *gestalt.KeyRange, indexCursor bool) (any, any, error) {
	return gestalt.IndexedDBRangeBounds(r, indexCursor)
}

func CompareValues(a, b any) int {
	return gestalt.CompareIndexedDBValues(a, b)
}

func cursorEntry(entry *Entry) *gestalt.IndexedDBCursorEntry {
	if entry == nil {
		return nil
	}
	return &gestalt.IndexedDBCursorEntry{
		Key:        entry.Key,
		PrimaryKey: entry.PrimaryKey,
		Record:     entry.Record,
	}
}
