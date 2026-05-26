package temporal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storeTemporalSchedules         = "workflow_temporal_schedules"
	storeTemporalEventTriggers     = "workflow_temporal_event_triggers"
	storeTemporalEventTriggerKeys  = "workflow_temporal_event_trigger_keys"
	storeTemporalDefinitions       = "workflow_temporal_definitions"
	storeTemporalRunProjections    = "workflow_temporal_v4_run_projections"
	storeTemporalRunIdempotency    = "workflow_temporal_v4_run_idempotency"
	storeTemporalSignalIdempotency = "workflow_temporal_v4_signal_idempotency"
	storeTemporalWorkflowKeys      = "workflow_temporal_v4_workflow_keys"

	indexByMatchKey  = "by_match_key"
	indexByTriggerID = "by_trigger_id"
)

type workflowStateStore struct {
	db      indexeddb.Database
	scopeID string

	schedules         indexeddb.ObjectStore
	eventTriggers     indexeddb.ObjectStore
	eventTriggerKeys  indexeddb.ObjectStore
	definitions       indexeddb.ObjectStore
	runProjections    indexeddb.ObjectStore
	runIdempotency    indexeddb.ObjectStore
	signalIdempotency indexeddb.ObjectStore
	workflowKeys      indexeddb.ObjectStore
}

func openWorkflowStateStore(ctx context.Context, scopeID string, db indexeddb.Database) (*workflowStateStore, error) {
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return nil, fmt.Errorf("scopeID is required")
	}
	if db == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}
	if err := ensureWorkflowStateStores(ctx, db); err != nil {
		return nil, err
	}
	store := &workflowStateStore{
		scopeID:           scopeID,
		db:                db,
		schedules:         db.ObjectStore(storeTemporalSchedules),
		eventTriggers:     db.ObjectStore(storeTemporalEventTriggers),
		eventTriggerKeys:  db.ObjectStore(storeTemporalEventTriggerKeys),
		definitions:       db.ObjectStore(storeTemporalDefinitions),
		runProjections:    db.ObjectStore(storeTemporalRunProjections),
		runIdempotency:    db.ObjectStore(storeTemporalRunIdempotency),
		signalIdempotency: db.ObjectStore(storeTemporalSignalIdempotency),
		workflowKeys:      db.ObjectStore(storeTemporalWorkflowKeys),
	}
	return store, nil
}

func ensureWorkflowStateStores(ctx context.Context, db indexeddb.Database) error {
	if db == nil {
		return nil
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalSchedules, temporalScheduleSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow schedule store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalEventTriggers, temporalEventTriggerSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow event trigger store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalEventTriggerKeys, temporalEventTriggerKeySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow event trigger key store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalDefinitions, temporalDefinitionSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow definition store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalRunProjections, temporalRunProjectionSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow run projection store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalRunIdempotency, temporalRunIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow run idempotency store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalSignalIdempotency, temporalSignalIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow signal idempotency store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalWorkflowKeys, temporalWorkflowKeySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow key store: %w", err)
	}
	return nil
}

func temporalScheduleSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func temporalEventTriggerSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "paused", Type: gestalt.TypeBool},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func temporalEventTriggerKeySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: indexByMatchKey, KeyPath: []string{"match_key"}},
			{Name: indexByTriggerID, KeyPath: []string{"trigger_id"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "match_key", Type: gestalt.TypeString, NotNull: true},
			{Name: "trigger_id", Type: gestalt.TypeString, NotNull: true},
		},
	}
}

func unsupportedTemporalRunID(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	_, err := decodeTemporalRunHandle(id)
	return err != nil
}

func temporalRunProjectionSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "workflow_key", Type: gestalt.TypeString},
			{Name: "status", Type: gestalt.TypeInt},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "started_at", Type: gestalt.TypeTime},
			{Name: "completed_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func temporalRunIdempotencySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "idempotency_key", Type: gestalt.TypeString},
			{Name: "fingerprint", Type: gestalt.TypeString, NotNull: true},
			{Name: "status", Type: gestalt.TypeString, NotNull: true},
			{Name: "run_id", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "expires_at", Type: gestalt.TypeTime},
			{Name: "run_payload", Type: gestalt.TypeBytes},
		},
	}
}

func temporalSignalIdempotencySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "idempotency_key", Type: gestalt.TypeString},
			{Name: "operation", Type: gestalt.TypeString},
			{Name: "fingerprint", Type: gestalt.TypeString, NotNull: true},
			{Name: "status", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "workflow_key", Type: gestalt.TypeString},
			{Name: "run_id", Type: gestalt.TypeString},
			{Name: "signal_id", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "expires_at", Type: gestalt.TypeTime},
			{Name: "response_payload", Type: gestalt.TypeBytes},
			{Name: "run_payload", Type: gestalt.TypeBytes},
		},
	}
}

func temporalWorkflowKeySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "workflow_key", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString, NotNull: true},
			{Name: "run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "temporal_workflow_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "temporal_run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "status", Type: gestalt.TypeInt, NotNull: true},
			{Name: "created_at", Type: gestalt.TypeTime, NotNull: true},
			{Name: "updated_at", Type: gestalt.TypeTime, NotNull: true},
		},
	}
}

func (s *workflowStateStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *workflowStateStore) putSchedule(ctx context.Context, schedule *gestalt.BoundWorkflowSchedule) error {
	if schedule == nil || strings.TrimSpace(schedule.ID) == "" {
		return nil
	}
	return s.schedules.Put(ctx, s.scheduleRecord(schedule))
}

func (s *workflowStateStore) getSchedule(ctx context.Context, id string) (*gestalt.BoundWorkflowSchedule, bool, error) {
	record, err := s.schedules.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	schedule, err := scheduleInputFromRecord(record)
	if err != nil {
		return nil, false, err
	}
	return schedule, strings.TrimSpace(schedule.ID) != "", nil
}

func (s *workflowStateStore) listSchedules(ctx context.Context) ([]*gestalt.BoundWorkflowSchedule, error) {
	records, err := s.schedules.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	schedules := make([]*gestalt.BoundWorkflowSchedule, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		schedule, err := scheduleInputFromRecord(record)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(schedule.ID) != "" {
			schedules = append(schedules, schedule)
		}
	}
	return schedules, nil
}

func (s *workflowStateStore) deleteSchedule(ctx context.Context, id string) error {
	err := s.schedules.Delete(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	return err
}

func (s *workflowStateStore) putRun(ctx context.Context, run *gestalt.BoundWorkflowRun) error {
	if run == nil || strings.TrimSpace(run.ID) == "" {
		return nil
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalRunProjections}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return fmt.Errorf("begin run projection transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	if _, err := s.putRunInTransaction(ctx, tx.ObjectStore(storeTemporalRunProjections), run); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit run projection: %w", err)
	}
	committed = true
	return nil
}

func (s *workflowStateStore) putRunInTransaction(ctx context.Context, store indexeddb.TransactionObjectStore, run *gestalt.BoundWorkflowRun) (*gestalt.BoundWorkflowRun, error) {
	if run == nil || strings.TrimSpace(run.ID) == "" {
		return nil, nil
	}
	run.ID = strings.TrimSpace(run.ID)
	if unsupportedTemporalRunID(run.ID) {
		return nil, nil
	}
	existing, found, err := s.getRunInTransaction(ctx, store, run.ID)
	if err != nil {
		return nil, fmt.Errorf("load run projection: %w", err)
	}
	if found && workflowRunTerminal(existing.Status) && !workflowRunTerminal(run.Status) {
		return existing, nil
	}
	if err := store.Put(ctx, s.runRecord(run)); err != nil {
		return nil, fmt.Errorf("store run projection: %w", err)
	}
	return run, nil
}

func (s *workflowStateStore) getRun(ctx context.Context, id string) (*gestalt.BoundWorkflowRun, bool, error) {
	record, err := s.runProjections.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	run, err := runFromRecord(record)
	if err == nil && unsupportedTemporalRunID(run.ID) {
		return nil, false, nil
	}
	return run, err == nil && strings.TrimSpace(run.ID) != "", err
}

func (s *workflowStateStore) getRunInTransaction(ctx context.Context, store indexeddb.TransactionObjectStore, id string) (*gestalt.BoundWorkflowRun, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, s.scopedID(strings.TrimSpace(id)))
	if err != nil || !found {
		return nil, false, err
	}
	run, err := runFromRecord(record)
	if err == nil && unsupportedTemporalRunID(run.ID) {
		return nil, false, nil
	}
	return run, err == nil && strings.TrimSpace(run.ID) != "", err
}

func (s *workflowStateStore) listRuns(ctx context.Context, req *gestalt.ListWorkflowProviderRunsRequest) ([]*gestalt.BoundWorkflowRun, string, error) {
	pageSize := effectiveRunListPageSize(req)
	pageToken := ""
	if req != nil {
		pageToken = req.PageToken
	}
	cursorAfter, err := decodeRunListPageToken(pageToken, s.scopeID, req)
	if err != nil {
		return nil, "", err
	}
	cursor, err := s.runProjections.OpenCursor(ctx, s.runProjectionScopeRange(), gestalt.CursorNext)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = cursor.Close() }()

	runs := make([]*gestalt.BoundWorkflowRun, 0, pageSize+1)
	for cursor.Continue() {
		record, err := cursor.Value()
		if err != nil {
			return nil, "", err
		}
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		if req != nil && req.Status != gestalt.WorkflowRunStatusValueUnspecified &&
			gestalt.WorkflowRunStatus(recordInt64(record, "status")) != req.Status {
			continue
		}
		run, err := runFromRecord(record)
		if err != nil {
			return nil, "", err
		}
		if unsupportedTemporalRunID(run.ID) || !runMatchesListRequest(run, req) {
			continue
		}
		if cursorAfter != nil && !runSortsAfterRunListCursor(run, cursorAfter) {
			continue
		}
		runs = append(runs, run)
		if len(runs) > pageSize+1 {
			sortWorkflowRunsNewestFirst(runs)
			runs = runs[:pageSize+1]
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, "", err
	}
	sortWorkflowRunsNewestFirst(runs)
	if len(runs) > pageSize {
		page := runs[:pageSize]
		return page, encodeRunListPageToken(page[len(page)-1], s.scopeID, req), nil
	}
	return runs, "", nil
}

func (s *workflowStateStore) runProjectionScopeRange() *gestalt.KeyRange {
	prefix := s.scopeID + "\x00"
	return &gestalt.KeyRange{
		Lower:     prefix,
		Upper:     s.scopeID + "\x01",
		UpperOpen: true,
	}
}

const runListPageTokenVersion = 1

type runListPageToken struct {
	Version   int                       `json:"v"`
	ScopeID   string                    `json:"scopeId"`
	CreatedAt string                    `json:"createdAt"`
	RunID     string                    `json:"runId"`
	Status    gestalt.WorkflowRunStatus `json:"status,omitempty"`
}

type runListCursor struct {
	CreatedAt time.Time
	RunID     string
}

func encodeRunListPageToken(lastRun *gestalt.BoundWorkflowRun, scopeID string, req *gestalt.ListWorkflowProviderRunsRequest) string {
	if lastRun == nil || strings.TrimSpace(lastRun.ID) == "" {
		return ""
	}
	token := runListPageToken{
		Version:   runListPageTokenVersion,
		ScopeID:   strings.TrimSpace(scopeID),
		CreatedAt: lastRun.CreatedAt.UTC().Format(time.RFC3339Nano),
		RunID:     strings.TrimSpace(lastRun.ID),
		Status:    runListRequestStatus(req),
	}
	encoded, err := json.Marshal(token)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeRunListPageToken(raw, scopeID string, req *gestalt.ListWorkflowProviderRunsRequest) (*runListCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "page token is invalid")
	}
	var token runListPageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return nil, status.Error(codes.InvalidArgument, "page token is invalid")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(token.CreatedAt))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "page token is invalid")
	}
	if token.Version != runListPageTokenVersion ||
		strings.TrimSpace(token.ScopeID) != strings.TrimSpace(scopeID) ||
		strings.TrimSpace(token.RunID) == "" ||
		token.Status != runListRequestStatus(req) {
		return nil, status.Error(codes.InvalidArgument, "page token is invalid")
	}
	return &runListCursor{CreatedAt: createdAt.UTC(), RunID: strings.TrimSpace(token.RunID)}, nil
}

func effectiveRunListPageSize(req *gestalt.ListWorkflowProviderRunsRequest) int {
	if req == nil || req.PageSize <= 0 {
		return 100
	}
	if req.PageSize > 200 {
		return 200
	}
	return int(req.PageSize)
}

func runMatchesListRequest(run *gestalt.BoundWorkflowRun, req *gestalt.ListWorkflowProviderRunsRequest) bool {
	if run == nil || req == nil {
		return run != nil
	}
	if req.Status != gestalt.WorkflowRunStatusValueUnspecified && run.Status != req.Status {
		return false
	}
	return true
}

func runListRequestStatus(req *gestalt.ListWorkflowProviderRunsRequest) gestalt.WorkflowRunStatus {
	if req == nil {
		return gestalt.WorkflowRunStatusValueUnspecified
	}
	return req.Status
}

func sortWorkflowRunsNewestFirst(runs []*gestalt.BoundWorkflowRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		left := runs[i]
		right := runs[j]
		if left == nil {
			return false
		}
		if right == nil {
			return true
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.After(right.CreatedAt)
		}
		return strings.TrimSpace(left.ID) < strings.TrimSpace(right.ID)
	})
}

func runSortsAfterRunListCursor(run *gestalt.BoundWorkflowRun, cursor *runListCursor) bool {
	if run == nil || cursor == nil {
		return false
	}
	createdAt := run.CreatedAt.UTC()
	if !createdAt.Equal(cursor.CreatedAt) {
		return createdAt.Before(cursor.CreatedAt)
	}
	return strings.TrimSpace(run.ID) > strings.TrimSpace(cursor.RunID)
}

type workflowKeyRecord struct {
	ID                 string
	WorkflowKey        string
	OwnerKey           string
	RunID              string
	TemporalWorkflowID string
	TemporalRunID      string
	Status             gestalt.WorkflowRunStatus
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (s *workflowStateStore) claimWorkflowKeyRun(ctx context.Context, workflowKey string, run *gestalt.BoundWorkflowRun, now time.Time) (*gestalt.BoundWorkflowRun, bool, error) {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		owner, claimed, err := s.claimWorkflowKeyRunOnce(ctx, workflowKey, run, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return owner, claimed, err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return nil, false, err
		}
	}
	return nil, false, status.Error(codes.Aborted, "workflow key claim raced too many times")
}

func (s *workflowStateStore) claimWorkflowKeyRunOnce(ctx context.Context, workflowKey string, run *gestalt.BoundWorkflowRun, now time.Time) (*gestalt.BoundWorkflowRun, bool, error) {
	record, run, err := s.workflowKeyRecordForRun(workflowKey, run, now)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalWorkflowKeys, storeTemporalRunProjections}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	keyStore := tx.ObjectStore(storeTemporalWorkflowKeys)
	runStore := tx.ObjectStore(storeTemporalRunProjections)
	existingRecord, found, err := transactionGetRecord(ctx, keyStore, record.ID)
	if err != nil {
		return nil, false, err
	}
	if found {
		existingKey := workflowKeyFromRecord(existingRecord)
		existingRun, runFound, err := s.getRunInTransaction(ctx, runStore, existingKey.RunID)
		if err != nil {
			return nil, false, err
		}
		if runFound && existingRun.ID != run.ID && !workflowRunTerminal(existingRun.Status) {
			if err := tx.Commit(ctx); err != nil {
				return nil, false, err
			}
			committed = true
			return existingRun, false, nil
		}
		if runFound && existingRun.ID == run.ID && !existingKey.CreatedAt.IsZero() {
			record.CreatedAt = existingKey.CreatedAt
		}
	}
	storedRun, err := s.putRunInTransaction(ctx, runStore, run)
	if err != nil {
		return nil, false, err
	}
	if storedRun == nil {
		return nil, false, status.Error(codes.InvalidArgument, "run is required")
	}
	record.RunID = storedRun.ID
	record.Status = storedRun.Status
	if found {
		if err := keyStore.Put(ctx, s.workflowKeyRecord(record)); err != nil {
			return nil, false, err
		}
	} else if err := keyStore.Add(ctx, s.workflowKeyRecord(record)); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	committed = true
	return storedRun, true, nil
}

func (s *workflowStateStore) getWorkflowKeyRun(ctx context.Context, workflowKey string) (*gestalt.BoundWorkflowRun, bool, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, false, nil
	}
	record, err := s.workflowKeys.Get(ctx, s.workflowKeyID(workflowKey))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	key := workflowKeyFromRecord(record)
	if key.RunID == "" {
		return nil, false, nil
	}
	return s.getRun(ctx, key.RunID)
}

func (s *workflowStateStore) clearWorkflowKeyRun(ctx context.Context, workflowKey, runID string) (bool, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	runID = strings.TrimSpace(runID)
	if workflowKey == "" || runID == "" {
		return false, nil
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalWorkflowKeys}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeTemporalWorkflowKeys)
	record, found, err := transactionGetRecord(ctx, store, s.workflowKeyID(workflowKey))
	if err != nil {
		return false, err
	}
	if !found {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		committed = true
		return false, nil
	}
	key := workflowKeyFromRecord(record)
	if key.RunID != runID {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		committed = true
		return false, nil
	}
	if err := store.Delete(ctx, key.ID); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

func (s *workflowStateStore) workflowKeyRecordForRun(workflowKey string, run *gestalt.BoundWorkflowRun, now time.Time) (workflowKeyRecord, *gestalt.BoundWorkflowRun, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	if run == nil {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run is required")
	}
	run.ID = strings.TrimSpace(run.ID)
	if run.ID == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return workflowKeyRecord{}, nil, status.Errorf(codes.InvalidArgument, "decode run_id: %v", err)
	}
	if handle.RunTemporalRunID == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id is missing run_temporal_run_id")
	}
	if handle.WorkflowKey != "" && handle.WorkflowKey != workflowKey {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id workflow_key does not match claim")
	}
	run.WorkflowKey = strings.TrimSpace(run.WorkflowKey)
	if run.WorkflowKey == "" {
		run.WorkflowKey = workflowKey
	} else if run.WorkflowKey != workflowKey {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run workflow_key does not match claim")
	}
	ownerKey := strings.TrimSpace(handle.OwnerKey)
	if ownerKey == "" {
		ownerKey = targetOwnerKeyInput(run.Target)
	}
	if ownerKey == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "owner_key is required")
	}
	if targetKey := targetOwnerKeyInput(run.Target); targetKey != "" && targetKey != ownerKey {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run target owner_key does not match claim")
	}
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	return workflowKeyRecord{
		ID:                 s.workflowKeyID(workflowKey),
		WorkflowKey:        workflowKey,
		OwnerKey:           ownerKey,
		RunID:              run.ID,
		TemporalWorkflowID: handle.RunWorkflowID,
		TemporalRunID:      handle.RunTemporalRunID,
		Status:             run.Status,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, run, nil
}

type runIdempotencyRecord struct {
	ID             string
	OwnerKey       string
	IdempotencyKey string
	Fingerprint    string
	Status         string
	RunID          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ExpiresAt      time.Time
	RunPayload     []byte
}

type runIdempotencyConflictError struct {
	Key string
}

func (e *runIdempotencyConflictError) Error() string {
	return fmt.Sprintf("idempotency key %q is already reserved for a different request", e.Key)
}

const runIdempotencyMaxAttempts = 5

func (s *workflowStateStore) reserveRunIdempotency(ctx context.Context, ownerKey, key, fingerprint string, retention time.Duration, now time.Time) (*runIdempotencyRecord, bool, error) {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		entry, existing, err := s.reserveRunIdempotencyOnce(ctx, ownerKey, key, fingerprint, retention, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return entry, existing, err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return nil, false, err
		}
	}
	return nil, false, status.Error(codes.Aborted, "workflow run idempotency reservation raced too many times")
}

func (s *workflowStateStore) reserveRunIdempotencyOnce(ctx context.Context, ownerKey, key, fingerprint string, retention time.Duration, now time.Time) (*runIdempotencyRecord, bool, error) {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	fingerprint = strings.TrimSpace(fingerprint)
	if ownerKey == "" || key == "" || fingerprint == "" {
		return nil, false, nil
	}
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	now = now.UTC()
	id := s.runIdempotencyID(ownerKey, key)
	tx, err := s.db.Transaction(ctx, []string{storeTemporalRunIdempotency}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeTemporalRunIdempotency)
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return nil, false, err
	}
	replaceExpired := false
	for _, record := range records {
		if recordString(record, "id") != id {
			continue
		}
		existing := runIdempotencyFromRecord(record)
		if existing.ExpiresAt.IsZero() || existing.ExpiresAt.After(now) {
			if existing.Fingerprint != fingerprint {
				return nil, false, &runIdempotencyConflictError{Key: key}
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, false, err
			}
			committed = true
			return &existing, true, nil
		}
		replaceExpired = true
		break
	}
	reserved := runIdempotencyRecord{
		ID:             id,
		OwnerKey:       ownerKey,
		IdempotencyKey: key,
		Fingerprint:    fingerprint,
		Status:         "reserved",
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(retention),
	}
	if replaceExpired {
		if err := store.Put(ctx, s.runIdempotencyRecord(reserved)); err != nil {
			return nil, false, err
		}
	} else {
		if err := store.Add(ctx, s.runIdempotencyRecord(reserved)); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	committed = true
	return &reserved, false, nil
}

func (s *workflowStateStore) completeRunIdempotency(ctx context.Context, ownerKey, key, fingerprint string, run *gestalt.BoundWorkflowRun, retention time.Duration, now time.Time) error {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		err := s.completeRunIdempotencyOnce(ctx, ownerKey, key, fingerprint, run, retention, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return err
		}
	}
	return status.Error(codes.Aborted, "workflow run idempotency completion raced too many times")
}

func (s *workflowStateStore) completeRunIdempotencyOnce(ctx context.Context, ownerKey, key, fingerprint string, run *gestalt.BoundWorkflowRun, retention time.Duration, now time.Time) error {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	fingerprint = strings.TrimSpace(fingerprint)
	if ownerKey == "" || key == "" || fingerprint == "" || run == nil {
		return nil
	}
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	now = now.UTC()
	id := s.runIdempotencyID(ownerKey, key)
	tx, err := s.db.Transaction(ctx, []string{storeTemporalRunIdempotency}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeTemporalRunIdempotency)
	createdAt := now
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return err
	}
	for _, existingRecord := range records {
		if recordString(existingRecord, "id") != id {
			continue
		}
		existing := runIdempotencyFromRecord(existingRecord)
		if existing.Fingerprint != fingerprint {
			if existing.ExpiresAt.IsZero() || existing.ExpiresAt.After(now) {
				return &runIdempotencyConflictError{Key: key}
			}
			return nil
		}
		if existing.Status == "completed" && runInputFromPayload(existing.RunPayload) != nil {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			committed = true
			return nil
		}
		if !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
		break
	}
	record := runIdempotencyRecord{
		ID:             id,
		OwnerKey:       ownerKey,
		IdempotencyKey: key,
		Fingerprint:    fingerprint,
		Status:         "completed",
		RunID:          strings.TrimSpace(run.ID),
		CreatedAt:      createdAt,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(retention),
		RunPayload:     nativePayload(run),
	}
	if err := store.Put(ctx, s.runIdempotencyRecord(record)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

type signalIdempotencyReserveRequest struct {
	Key                  string
	Operation            string
	Fingerprint          string
	OwnerKey             string
	WorkflowKey          string
	RunID                string
	SignalID             string
	AllowPayloadVariance bool
}

type signalIdempotencyRecord struct {
	ID              string
	IdempotencyKey  string
	Operation       string
	Fingerprint     string
	Status          string
	OwnerKey        string
	WorkflowKey     string
	RunID           string
	SignalID        string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExpiresAt       time.Time
	ResponsePayload []byte
	RunPayload      []byte
}

func (s *workflowStateStore) reserveSignalIdempotency(ctx context.Context, req signalIdempotencyReserveRequest, retention time.Duration, now time.Time) (*signalIdempotencyRecord, bool, error) {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		entry, existing, err := s.reserveSignalIdempotencyOnce(ctx, req, retention, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return entry, existing, err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return nil, false, err
		}
	}
	return nil, false, status.Error(codes.Aborted, "workflow signal idempotency reservation raced too many times")
}

func (s *workflowStateStore) reserveSignalIdempotencyOnce(ctx context.Context, req signalIdempotencyReserveRequest, retention time.Duration, now time.Time) (*signalIdempotencyRecord, bool, error) {
	req.Key = strings.TrimSpace(req.Key)
	req.Fingerprint = strings.TrimSpace(req.Fingerprint)
	if req.Key == "" || req.Fingerprint == "" {
		return nil, false, nil
	}
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	now = now.UTC()
	id := s.signalIdempotencyID(req.Key)
	tx, err := s.db.Transaction(ctx, []string{storeTemporalSignalIdempotency}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeTemporalSignalIdempotency)
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return nil, false, err
	}
	replaceExpired := false
	for _, record := range records {
		if recordString(record, "id") != id {
			continue
		}
		existing := signalIdempotencyFromRecord(record)
		if existing.ExpiresAt.IsZero() || existing.ExpiresAt.After(now) {
			if existing.Fingerprint != req.Fingerprint && !req.AllowPayloadVariance {
				return nil, false, &runIdempotencyConflictError{Key: req.Key}
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, false, err
			}
			committed = true
			return &existing, true, nil
		}
		replaceExpired = true
		break
	}
	reserved := signalIdempotencyRecord{
		ID:             id,
		IdempotencyKey: req.Key,
		Operation:      strings.TrimSpace(req.Operation),
		Fingerprint:    req.Fingerprint,
		Status:         "reserved",
		OwnerKey:       strings.TrimSpace(req.OwnerKey),
		WorkflowKey:    strings.TrimSpace(req.WorkflowKey),
		RunID:          strings.TrimSpace(req.RunID),
		SignalID:       strings.TrimSpace(req.SignalID),
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(retention),
	}
	if replaceExpired {
		if err := store.Put(ctx, s.signalIdempotencyRecord(reserved)); err != nil {
			return nil, false, err
		}
	} else {
		if err := store.Add(ctx, s.signalIdempotencyRecord(reserved)); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	committed = true
	return &reserved, false, nil
}

func (s *workflowStateStore) completeSignalIdempotency(ctx context.Context, key, fingerprint string, resp *gestalt.SignalWorkflowRunResponse, allowPayloadVariance bool, retention time.Duration, now time.Time) error {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		err := s.completeSignalIdempotencyOnce(ctx, key, fingerprint, resp, allowPayloadVariance, retention, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return err
		}
	}
	return status.Error(codes.Aborted, "workflow signal idempotency completion raced too many times")
}

func (s *workflowStateStore) completeSignalIdempotencyOnce(ctx context.Context, key, fingerprint string, resp *gestalt.SignalWorkflowRunResponse, allowPayloadVariance bool, retention time.Duration, now time.Time) error {
	key = strings.TrimSpace(key)
	fingerprint = strings.TrimSpace(fingerprint)
	if key == "" || fingerprint == "" || resp == nil {
		return nil
	}
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	now = now.UTC()
	id := s.signalIdempotencyID(key)
	tx, err := s.db.Transaction(ctx, []string{storeTemporalSignalIdempotency}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeTemporalSignalIdempotency)
	createdAt := now
	var existing signalIdempotencyRecord
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return err
	}
	for _, existingRecord := range records {
		if recordString(existingRecord, "id") != id {
			continue
		}
		existing = signalIdempotencyFromRecord(existingRecord)
		if existing.Fingerprint != fingerprint {
			if existing.ExpiresAt.IsZero() || existing.ExpiresAt.After(now) {
				if !allowPayloadVariance {
					return &runIdempotencyConflictError{Key: key}
				}
				if existing.Status == "completed" && signalResponseInputFromPayload(existing.ResponsePayload) != nil {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					committed = true
					return nil
				}
				if !existing.CreatedAt.IsZero() {
					createdAt = existing.CreatedAt
				}
				break
			}
			break
		}
		if existing.Status == "completed" && signalResponseInputFromPayload(existing.ResponsePayload) != nil {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			committed = true
			return nil
		}
		if !existing.CreatedAt.IsZero() {
			createdAt = existing.CreatedAt
		}
		break
	}
	workflowKey := existing.WorkflowKey
	if workflowKey == "" {
		workflowKey = resp.WorkflowKey
	}
	runID := existing.RunID
	if runID == "" && resp.Run != nil {
		runID = resp.Run.ID
	}
	signalID := existing.SignalID
	if signalID == "" && resp.Signal != nil {
		signalID = resp.Signal.ID
	}
	record := signalIdempotencyRecord{
		ID:              id,
		IdempotencyKey:  key,
		Operation:       existing.Operation,
		Fingerprint:     fingerprint,
		Status:          "completed",
		OwnerKey:        existing.OwnerKey,
		WorkflowKey:     workflowKey,
		RunID:           runID,
		SignalID:        signalID,
		CreatedAt:       createdAt,
		UpdatedAt:       now,
		ExpiresAt:       now.Add(retention),
		ResponsePayload: nativePayload(resp),
		RunPayload:      nativePayload(resp.Run),
	}
	if err := store.Put(ctx, s.signalIdempotencyRecord(record)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func isRunIdempotencyRetryableConflict(err error) bool {
	if errors.Is(err, gestalt.ErrAlreadyExists) {
		return true
	}
	switch status.Code(err) {
	case codes.AlreadyExists, codes.Aborted:
		return true
	case codes.Internal:
		msg := strings.ToLower(err.Error())
		return strings.Contains(msg, "database is locked") ||
			strings.Contains(msg, "database table is locked") ||
			strings.Contains(msg, "lock wait timeout") ||
			strings.Contains(msg, "deadlock") ||
			strings.Contains(msg, "could not serialize access")
	default:
		return false
	}
}

func yieldRunIdempotencyRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		runtime.Gosched()
		return nil
	}
}

func (s *workflowStateStore) putTrigger(ctx context.Context, trigger *gestalt.BoundWorkflowEventTrigger) error {
	if trigger == nil || strings.TrimSpace(trigger.ID) == "" {
		return nil
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalEventTriggers, storeTemporalEventTriggerKeys}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	triggerStore := tx.ObjectStore(storeTemporalEventTriggers)
	keyStore := tx.ObjectStore(storeTemporalEventTriggerKeys)
	if _, err := keyStore.Index(indexByTriggerID).Delete(ctx, s.scopedID(trigger.ID)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}
	if err := triggerStore.Put(ctx, s.triggerRecord(trigger)); err != nil {
		return err
	}
	for _, key := range matchKeysInput(targetOwnerKeyInput(trigger.Target), trigger.Match) {
		if err := keyStore.Put(ctx, gestalt.Record{
			"id":         s.scopedID(key, trigger.ID),
			"scope_id":   s.scopeID,
			"match_key":  s.scopedID(key),
			"trigger_id": s.scopedID(trigger.ID),
		}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *workflowStateStore) getTrigger(ctx context.Context, id string) (*gestalt.BoundWorkflowEventTrigger, bool, error) {
	record, err := s.eventTriggers.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	trigger, err := triggerFromRecord(record)
	return trigger, err == nil && strings.TrimSpace(trigger.ID) != "", err
}

func (s *workflowStateStore) listTriggers(ctx context.Context) ([]*gestalt.BoundWorkflowEventTrigger, error) {
	records, err := s.eventTriggers.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	triggers := make([]*gestalt.BoundWorkflowEventTrigger, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		trigger, err := triggerFromRecord(record)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, trigger)
	}
	return triggers, nil
}

func (s *workflowStateStore) matchTriggers(ctx context.Context, ownerKey string, event *gestalt.WorkflowEvent) ([]*gestalt.BoundWorkflowEventTrigger, error) {
	seen := map[string]struct{}{}
	triggers := make([]*gestalt.BoundWorkflowEventTrigger, 0)
	for _, key := range eventLookupKeysInput(ownerKey, event) {
		keyRecords, err := s.eventTriggerKeys.Index(indexByMatchKey).GetAll(ctx, nil, s.scopedID(key))
		if errors.Is(err, gestalt.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, keyRecord := range keyRecords {
			triggerID := s.unscopedID(recordString(keyRecord, "trigger_id"))
			if triggerID == "" {
				continue
			}
			if _, ok := seen[triggerID]; ok {
				continue
			}
			trigger, found, err := s.getTrigger(ctx, triggerID)
			if err != nil {
				return nil, err
			}
			if !found || !eventMatchesTriggerInput(event, trigger) {
				continue
			}
			seen[triggerID] = struct{}{}
			triggers = append(triggers, trigger)
		}
	}
	return triggers, nil
}

func (s *workflowStateStore) deleteTrigger(ctx context.Context, id string) (bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return false, nil
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalEventTriggers, storeTemporalEventTriggerKeys}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	triggerStore := tx.ObjectStore(storeTemporalEventTriggers)
	keyStore := tx.ObjectStore(storeTemporalEventTriggerKeys)
	if _, err := triggerStore.Get(ctx, s.scopedID(id)); errors.Is(err, gestalt.ErrNotFound) {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		committed = true
		return false, nil
	} else if err != nil {
		return false, err
	}
	if _, err := keyStore.Index(indexByTriggerID).Delete(ctx, s.scopedID(id)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return false, err
	}
	if err := triggerStore.Delete(ctx, s.scopedID(id)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

func (s *workflowStateStore) scheduleRecord(schedule *gestalt.BoundWorkflowSchedule) gestalt.Record {
	payload := nativePayload(schedule)
	now := time.Now().UTC()
	createdAt := schedule.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := schedule.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	return gestalt.Record{
		"id":         s.scopedID(schedule.ID),
		"scope_id":   s.scopeID,
		"owner_key":  targetOwnerKeyInput(schedule.Target),
		"created_at": createdAt.UTC(),
		"updated_at": updatedAt.UTC(),
		"payload":    payload,
	}
}

func (s *workflowStateStore) runRecord(run *gestalt.BoundWorkflowRun) gestalt.Record {
	payload := nativePayload(run)
	now := time.Now().UTC()
	createdAt := run.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	return gestalt.Record{
		"id":           s.scopedID(run.ID),
		"scope_id":     s.scopeID,
		"owner_key":    targetOwnerKeyInput(run.Target),
		"workflow_key": strings.TrimSpace(run.WorkflowKey),
		"status":       int64(run.Status),
		"created_at":   createdAt.UTC(),
		"started_at":   optionalTime(run.StartedAt),
		"completed_at": optionalTime(run.CompletedAt),
		"payload":      payload,
	}
}

func runFromRecord(record gestalt.Record) (*gestalt.BoundWorkflowRun, error) {
	return decodeNativePayload[gestalt.BoundWorkflowRun](recordBytes(record, "payload"), "workflow run")
}

func (s *workflowStateStore) runIdempotencyID(ownerKey, key string) string {
	return s.scopedID("start", hashID(ownerKey, key))
}

func (s *workflowStateStore) signalIdempotencyID(key string) string {
	return s.scopedID("signal", hashID(key))
}

func (s *workflowStateStore) workflowKeyID(workflowKey string) string {
	return s.scopedID("workflow-key", hashID(workflowKey))
}

func (s *workflowStateStore) workflowKeyRecord(record workflowKeyRecord) gestalt.Record {
	return gestalt.Record{
		"id":                   strings.TrimSpace(record.ID),
		"scope_id":             s.scopeID,
		"workflow_key":         strings.TrimSpace(record.WorkflowKey),
		"owner_key":            strings.TrimSpace(record.OwnerKey),
		"run_id":               strings.TrimSpace(record.RunID),
		"temporal_workflow_id": strings.TrimSpace(record.TemporalWorkflowID),
		"temporal_run_id":      strings.TrimSpace(record.TemporalRunID),
		"status":               int64(record.Status),
		"created_at":           record.CreatedAt.UTC(),
		"updated_at":           record.UpdatedAt.UTC(),
	}
}

func workflowKeyFromRecord(record gestalt.Record) workflowKeyRecord {
	out := workflowKeyRecord{
		ID:                 recordString(record, "id"),
		WorkflowKey:        recordString(record, "workflow_key"),
		OwnerKey:           recordString(record, "owner_key"),
		RunID:              recordString(record, "run_id"),
		TemporalWorkflowID: recordString(record, "temporal_workflow_id"),
		TemporalRunID:      recordString(record, "temporal_run_id"),
		Status:             gestalt.WorkflowRunStatus(recordInt64(record, "status")),
	}
	if createdAt := recordTime(record, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := recordTime(record, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	return out
}

func (s *workflowStateStore) runIdempotencyRecord(record runIdempotencyRecord) gestalt.Record {
	return gestalt.Record{
		"id":              strings.TrimSpace(record.ID),
		"scope_id":        s.scopeID,
		"owner_key":       strings.TrimSpace(record.OwnerKey),
		"idempotency_key": strings.TrimSpace(record.IdempotencyKey),
		"fingerprint":     strings.TrimSpace(record.Fingerprint),
		"status":          strings.TrimSpace(record.Status),
		"run_id":          strings.TrimSpace(record.RunID),
		"created_at":      record.CreatedAt.UTC(),
		"updated_at":      record.UpdatedAt.UTC(),
		"expires_at":      record.ExpiresAt.UTC(),
		"run_payload":     append([]byte(nil), record.RunPayload...),
	}
}

func runIdempotencyFromRecord(record gestalt.Record) runIdempotencyRecord {
	out := runIdempotencyRecord{
		ID:             recordString(record, "id"),
		OwnerKey:       recordString(record, "owner_key"),
		IdempotencyKey: recordString(record, "idempotency_key"),
		Fingerprint:    recordString(record, "fingerprint"),
		Status:         recordString(record, "status"),
		RunID:          recordString(record, "run_id"),
		RunPayload:     recordBytes(record, "run_payload"),
	}
	if createdAt := recordTime(record, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := recordTime(record, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	if expiresAt := recordTime(record, "expires_at"); expiresAt != nil {
		out.ExpiresAt = expiresAt.UTC()
	}
	return out
}

func (s *workflowStateStore) signalIdempotencyRecord(record signalIdempotencyRecord) gestalt.Record {
	return gestalt.Record{
		"id":               strings.TrimSpace(record.ID),
		"scope_id":         s.scopeID,
		"idempotency_key":  strings.TrimSpace(record.IdempotencyKey),
		"operation":        strings.TrimSpace(record.Operation),
		"fingerprint":      strings.TrimSpace(record.Fingerprint),
		"status":           strings.TrimSpace(record.Status),
		"owner_key":        strings.TrimSpace(record.OwnerKey),
		"workflow_key":     strings.TrimSpace(record.WorkflowKey),
		"run_id":           strings.TrimSpace(record.RunID),
		"signal_id":        strings.TrimSpace(record.SignalID),
		"created_at":       record.CreatedAt.UTC(),
		"updated_at":       record.UpdatedAt.UTC(),
		"expires_at":       record.ExpiresAt.UTC(),
		"response_payload": append([]byte(nil), record.ResponsePayload...),
		"run_payload":      append([]byte(nil), record.RunPayload...),
	}
}

func signalIdempotencyFromRecord(record gestalt.Record) signalIdempotencyRecord {
	out := signalIdempotencyRecord{
		ID:              recordString(record, "id"),
		IdempotencyKey:  recordString(record, "idempotency_key"),
		Operation:       recordString(record, "operation"),
		Fingerprint:     recordString(record, "fingerprint"),
		Status:          recordString(record, "status"),
		OwnerKey:        recordString(record, "owner_key"),
		WorkflowKey:     recordString(record, "workflow_key"),
		RunID:           recordString(record, "run_id"),
		SignalID:        recordString(record, "signal_id"),
		ResponsePayload: recordBytes(record, "response_payload"),
		RunPayload:      recordBytes(record, "run_payload"),
	}
	if createdAt := recordTime(record, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := recordTime(record, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	if expiresAt := recordTime(record, "expires_at"); expiresAt != nil {
		out.ExpiresAt = expiresAt.UTC()
	}
	return out
}

func (s *workflowStateStore) triggerRecord(trigger *gestalt.BoundWorkflowEventTrigger) gestalt.Record {
	payload := nativePayload(trigger)
	now := time.Now().UTC()
	createdAt := trigger.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := trigger.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	return gestalt.Record{
		"id":         s.scopedID(trigger.ID),
		"scope_id":   s.scopeID,
		"owner_key":  targetOwnerKeyInput(trigger.Target),
		"paused":     trigger.Paused,
		"created_at": createdAt.UTC(),
		"updated_at": updatedAt.UTC(),
		"payload":    payload,
	}
}

func triggerFromRecord(record gestalt.Record) (*gestalt.BoundWorkflowEventTrigger, error) {
	return decodeNativePayload[gestalt.BoundWorkflowEventTrigger](recordBytes(record, "payload"), "workflow event trigger")
}

func scheduleInputFromRecord(record gestalt.Record) (*gestalt.BoundWorkflowSchedule, error) {
	return decodeNativePayload[gestalt.BoundWorkflowSchedule](recordBytes(record, "payload"), "workflow schedule")
}

func nativePayload(value any) []byte {
	if value == nil {
		return nil
	}
	payload, _ := json.Marshal(value)
	return payload
}

func decodeNativePayload[T any](payload []byte, kind string) (*T, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty %s payload", kind)
	}
	var input T
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, err
	}
	return &input, nil
}

func runInputFromPayload(payload []byte) *gestalt.BoundWorkflowRun {
	run, err := decodeNativePayload[gestalt.BoundWorkflowRun](payload, "workflow run")
	if err != nil {
		return nil
	}
	return run
}

func signalResponseInputFromPayload(payload []byte) *gestalt.SignalWorkflowRunResponse {
	resp, err := decodeNativePayload[gestalt.SignalWorkflowRunResponse](payload, "workflow signal response")
	if err != nil {
		return nil
	}
	return resp
}

func recordBytes(record gestalt.Record, key string) []byte {
	switch value := record[key].(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	default:
		return nil
	}
}

func recordString(record gestalt.Record, key string) string {
	value, _ := record[key].(string)
	return strings.TrimSpace(value)
}

func recordInt64(record gestalt.Record, key string) int64 {
	switch value := record[key].(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func recordTime(record gestalt.Record, key string) *time.Time {
	switch value := record[key].(type) {
	case time.Time:
		asTime := value.UTC()
		return &asTime
	case *time.Time:
		if value == nil {
			return nil
		}
		asTime := value.UTC()
		return &asTime
	default:
		return nil
	}
}

func optionalTime(value *time.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	asTime := value.UTC()
	return &asTime
}

func transactionGetRecord(ctx context.Context, store indexeddb.TransactionObjectStore, id string) (gestalt.Record, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, false, nil
	}
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return nil, false, err
	}
	for _, record := range records {
		if recordString(record, "id") == id {
			return record, true, nil
		}
	}
	return nil, false, nil
}

func (s *workflowStateStore) scopedID(parts ...string) string {
	cleaned := make([]string, 0, len(parts)+1)
	cleaned = append(cleaned, s.scopeID)
	for _, part := range parts {
		cleaned = append(cleaned, strings.TrimSpace(part))
	}
	return strings.Join(cleaned, "\x00")
}

func (s *workflowStateStore) unscopedID(id string) string {
	prefix := s.scopeID + "\x00"
	return strings.TrimSpace(strings.TrimPrefix(id, prefix))
}
