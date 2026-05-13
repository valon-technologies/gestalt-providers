package cursorutil

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Entry = gestalt.IndexedDBCursorSnapshotEntry

type Snapshot struct {
	gestalt.IndexedDBCursorSnapshot
}

type Runtime interface {
	SnapshotState() *Snapshot
	DeleteCurrent(context.Context) error
	UpdateCurrent(context.Context, gestalt.Record) (*gestalt.IndexedDBCursorEntry, error)
}

func NewSnapshot(req gestalt.IndexedDBOpenCursorRequest) Snapshot {
	return Snapshot{IndexedDBCursorSnapshot: gestalt.NewIndexedDBCursorSnapshot(req)}
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
