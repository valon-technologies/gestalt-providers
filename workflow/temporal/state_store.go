package temporal

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	storeTemporalSchedules         = "workflow_temporal_schedules"
	storeTemporalEventTriggers     = "workflow_temporal_event_triggers"
	storeTemporalEventTriggerKeys  = "workflow_temporal_event_trigger_keys"
	storeTemporalExecutionRefs     = "workflow_temporal_execution_refs"
	storeTemporalRunProjections    = "workflow_temporal_v4_run_projections"
	storeTemporalRunIdempotency    = "workflow_temporal_v4_run_idempotency"
	storeTemporalSignalIdempotency = "workflow_temporal_v4_signal_idempotency"
	storeTemporalWorkflowKeys      = "workflow_temporal_v4_workflow_keys"

	indexBySubject   = "by_subject"
	indexByMatchKey  = "by_match_key"
	indexByTriggerID = "by_trigger_id"
)

var deprecatedWorkflowStateStores = []string{
	"workflow_temporal_runs",
	"workflow_temporal_workflow_keys",
	"workflow_temporal_idempotency",
}

type workflowStateStore struct {
	db      *gestalt.IndexedDBClient
	scopeID string

	schedules         *gestalt.ObjectStoreClient
	eventTriggers     *gestalt.ObjectStoreClient
	eventTriggerKeys  *gestalt.ObjectStoreClient
	executionRefs     *gestalt.ObjectStoreClient
	runProjections    *gestalt.ObjectStoreClient
	runIdempotency    *gestalt.ObjectStoreClient
	signalIdempotency *gestalt.ObjectStoreClient
	workflowKeys      *gestalt.ObjectStoreClient
}

func openWorkflowStateStore(ctx context.Context, binding, scopeID string) (*workflowStateStore, error) {
	binding = strings.TrimSpace(binding)
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return nil, fmt.Errorf("scopeID is required")
	}
	var (
		db  *gestalt.IndexedDBClient
		err error
	)
	if binding == "" {
		db, err = gestalt.IndexedDB()
	} else {
		db, err = gestalt.IndexedDB(binding)
	}
	if err != nil {
		return nil, fmt.Errorf("connect indexeddb: %w", err)
	}
	if err := deleteDeprecatedWorkflowStateStores(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureWorkflowStateStores(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &workflowStateStore{
		scopeID:           scopeID,
		db:                db,
		schedules:         db.ObjectStore(storeTemporalSchedules),
		eventTriggers:     db.ObjectStore(storeTemporalEventTriggers),
		eventTriggerKeys:  db.ObjectStore(storeTemporalEventTriggerKeys),
		executionRefs:     db.ObjectStore(storeTemporalExecutionRefs),
		runProjections:    db.ObjectStore(storeTemporalRunProjections),
		runIdempotency:    db.ObjectStore(storeTemporalRunIdempotency),
		signalIdempotency: db.ObjectStore(storeTemporalSignalIdempotency),
		workflowKeys:      db.ObjectStore(storeTemporalWorkflowKeys),
	}
	return store, nil
}

func deleteDeprecatedWorkflowStateStores(ctx context.Context, db *gestalt.IndexedDBClient) error {
	if db == nil {
		return nil
	}
	for _, name := range deprecatedWorkflowStateStores {
		if err := db.DeleteObjectStore(ctx, name); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return fmt.Errorf("delete deprecated indexeddb object store %s: %w", name, err)
		}
	}
	return nil
}

func ensureWorkflowStateStores(ctx context.Context, db *gestalt.IndexedDBClient) error {
	if db == nil {
		return nil
	}
	if err := db.CreateObjectStore(ctx, storeTemporalRunProjections, temporalRunProjectionSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow run projection store: %w", err)
	}
	if err := db.CreateObjectStore(ctx, storeTemporalRunIdempotency, temporalRunIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow run idempotency store: %w", err)
	}
	if err := db.CreateObjectStore(ctx, storeTemporalSignalIdempotency, temporalSignalIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow signal idempotency store: %w", err)
	}
	if err := db.CreateObjectStore(ctx, storeTemporalWorkflowKeys, temporalWorkflowKeySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow key store: %w", err)
	}
	return nil
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

func (s *workflowStateStore) putSchedule(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	schedule = cloneSchedule(schedule)
	if schedule == nil || strings.TrimSpace(schedule.GetId()) == "" {
		return nil
	}
	return s.schedules.Put(ctx, s.scheduleRecord(schedule))
}

func (s *workflowStateStore) getSchedule(ctx context.Context, id string) (*proto.BoundWorkflowSchedule, bool, error) {
	record, err := s.schedules.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var schedule proto.BoundWorkflowSchedule
	if err := unmarshalRecordPayload(record, &schedule); err != nil {
		return nil, false, err
	}
	return cloneSchedule(&schedule), schedule.GetId() != "", nil
}

func (s *workflowStateStore) listSchedules(ctx context.Context) ([]*proto.BoundWorkflowSchedule, error) {
	records, err := s.schedules.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	schedules := make([]*proto.BoundWorkflowSchedule, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		var schedule proto.BoundWorkflowSchedule
		if err := unmarshalRecordPayload(record, &schedule); err != nil {
			return nil, err
		}
		if schedule.GetId() != "" {
			schedules = append(schedules, cloneSchedule(&schedule))
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

func (s *workflowStateStore) putRun(ctx context.Context, run *proto.BoundWorkflowRun) error {
	run = cloneRun(run)
	if run == nil || strings.TrimSpace(run.GetId()) == "" {
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

func (s *workflowStateStore) putRunInTransaction(ctx context.Context, store *gestalt.TransactionObjectStore, run *proto.BoundWorkflowRun) (*proto.BoundWorkflowRun, error) {
	run = cloneRun(run)
	if run == nil || strings.TrimSpace(run.GetId()) == "" {
		return nil, nil
	}
	existing, found, err := s.getRunInTransaction(ctx, store, run.GetId())
	if err != nil {
		return nil, fmt.Errorf("load run projection: %w", err)
	}
	if found && workflowRunTerminal(existing.GetStatus()) && !workflowRunTerminal(run.GetStatus()) {
		return existing, nil
	}
	if err := store.Put(ctx, s.runRecord(run)); err != nil {
		return nil, fmt.Errorf("store run projection: %w", err)
	}
	return run, nil
}

func (s *workflowStateStore) getRun(ctx context.Context, id string) (*proto.BoundWorkflowRun, bool, error) {
	record, err := s.runProjections.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	run, err := runFromRecord(record)
	return run, err == nil && run.GetId() != "", err
}

func (s *workflowStateStore) getRunInTransaction(ctx context.Context, store *gestalt.TransactionObjectStore, id string) (*proto.BoundWorkflowRun, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, s.scopedID(strings.TrimSpace(id)))
	if err != nil || !found {
		return nil, false, err
	}
	run, err := runFromRecord(record)
	return run, err == nil && run.GetId() != "", err
}

func (s *workflowStateStore) listRuns(ctx context.Context) ([]*proto.BoundWorkflowRun, error) {
	records, err := s.runProjections.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	runs := make([]*proto.BoundWorkflowRun, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		run, err := runFromRecord(record)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	sortRuns(runs)
	return runs, nil
}

type workflowKeyRecord struct {
	ID                 string
	WorkflowKey        string
	OwnerKey           string
	RunID              string
	TemporalWorkflowID string
	TemporalRunID      string
	Status             proto.WorkflowRunStatus
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (s *workflowStateStore) claimWorkflowKeyRun(ctx context.Context, workflowKey string, run *proto.BoundWorkflowRun, now time.Time) (*proto.BoundWorkflowRun, bool, error) {
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

func (s *workflowStateStore) claimWorkflowKeyRunOnce(ctx context.Context, workflowKey string, run *proto.BoundWorkflowRun, now time.Time) (*proto.BoundWorkflowRun, bool, error) {
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
		if runFound && existingRun.GetId() != run.GetId() && !workflowRunTerminal(existingRun.GetStatus()) {
			if err := tx.Commit(ctx); err != nil {
				return nil, false, err
			}
			committed = true
			return existingRun, false, nil
		}
		if runFound && existingRun.GetId() == run.GetId() && !existingKey.CreatedAt.IsZero() {
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
	record.RunID = storedRun.GetId()
	record.Status = storedRun.GetStatus()
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

func (s *workflowStateStore) getWorkflowKeyRun(ctx context.Context, workflowKey string) (*proto.BoundWorkflowRun, bool, error) {
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

func (s *workflowStateStore) workflowKeyRecordForRun(workflowKey string, run *proto.BoundWorkflowRun, now time.Time) (workflowKeyRecord, *proto.BoundWorkflowRun, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	run = cloneRun(run)
	if run == nil {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run is required")
	}
	run.Id = strings.TrimSpace(run.GetId())
	if run.GetId() == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	handle, err := decodeV3RunHandle(run.GetId())
	if err != nil {
		return workflowKeyRecord{}, nil, status.Errorf(codes.InvalidArgument, "decode run_id: %v", err)
	}
	if handle.RunTemporalRunID == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id is missing run_temporal_run_id")
	}
	if handle.WorkflowKey != "" && handle.WorkflowKey != workflowKey {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run_id workflow_key does not match claim")
	}
	run.WorkflowKey = strings.TrimSpace(run.GetWorkflowKey())
	if run.GetWorkflowKey() == "" {
		run.WorkflowKey = workflowKey
	} else if run.GetWorkflowKey() != workflowKey {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "run workflow_key does not match claim")
	}
	ownerKey := strings.TrimSpace(handle.OwnerKey)
	if ownerKey == "" {
		ownerKey = targetOwnerKey(run.GetTarget())
	}
	if ownerKey == "" {
		return workflowKeyRecord{}, nil, status.Error(codes.InvalidArgument, "owner_key is required")
	}
	if targetKey := targetOwnerKey(run.GetTarget()); targetKey != "" && targetKey != ownerKey {
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
		RunID:              run.GetId(),
		TemporalWorkflowID: handle.RunWorkflowID,
		TemporalRunID:      handle.RunTemporalRunID,
		Status:             run.GetStatus(),
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

func (s *workflowStateStore) completeRunIdempotency(ctx context.Context, ownerKey, key, fingerprint string, run *proto.BoundWorkflowRun, retention time.Duration, now time.Time) error {
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

func (s *workflowStateStore) completeRunIdempotencyOnce(ctx context.Context, ownerKey, key, fingerprint string, run *proto.BoundWorkflowRun, retention time.Duration, now time.Time) error {
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
		if existing.Status == "completed" && runFromPayload(existing.RunPayload) != nil {
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
		RunID:          strings.TrimSpace(run.GetId()),
		CreatedAt:      createdAt,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(retention),
		RunPayload:     protoPayload(run),
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
	Key         string
	Operation   string
	Fingerprint string
	OwnerKey    string
	WorkflowKey string
	RunID       string
	SignalID    string
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
			if existing.Fingerprint != req.Fingerprint {
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

func (s *workflowStateStore) completeSignalIdempotency(ctx context.Context, key, fingerprint string, resp *proto.SignalWorkflowRunResponse, retention time.Duration, now time.Time) error {
	for attempt := 0; attempt < runIdempotencyMaxAttempts; attempt++ {
		err := s.completeSignalIdempotencyOnce(ctx, key, fingerprint, resp, retention, now)
		if err == nil || !isRunIdempotencyRetryableConflict(err) {
			return err
		}
		if err := yieldRunIdempotencyRetry(ctx); err != nil {
			return err
		}
	}
	return status.Error(codes.Aborted, "workflow signal idempotency completion raced too many times")
}

func (s *workflowStateStore) completeSignalIdempotencyOnce(ctx context.Context, key, fingerprint string, resp *proto.SignalWorkflowRunResponse, retention time.Duration, now time.Time) error {
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
				return &runIdempotencyConflictError{Key: key}
			}
			return nil
		}
		if existing.Status == "completed" && signalResponseFromPayload(existing.ResponsePayload) != nil {
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
		workflowKey = resp.GetWorkflowKey()
	}
	runID := existing.RunID
	if runID == "" {
		runID = resp.GetRun().GetId()
	}
	signalID := existing.SignalID
	if signalID == "" {
		signalID = resp.GetSignal().GetId()
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
		ResponsePayload: protoPayload(resp),
		RunPayload:      protoPayload(resp.GetRun()),
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

func (s *workflowStateStore) putTrigger(ctx context.Context, trigger *proto.BoundWorkflowEventTrigger) error {
	trigger = cloneTrigger(trigger)
	if trigger == nil || strings.TrimSpace(trigger.GetId()) == "" {
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
	if _, err := keyStore.Index(indexByTriggerID).Delete(ctx, s.scopedID(trigger.GetId())); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}
	if err := triggerStore.Put(ctx, s.triggerRecord(trigger)); err != nil {
		return err
	}
	for _, key := range matchKeys(targetOwnerKey(trigger.GetTarget()), trigger.GetMatch()) {
		if err := keyStore.Put(ctx, gestalt.Record{
			"id":         s.scopedID(key, trigger.GetId()),
			"scope_id":   s.scopeID,
			"match_key":  s.scopedID(key),
			"trigger_id": s.scopedID(trigger.GetId()),
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

func (s *workflowStateStore) getTrigger(ctx context.Context, id string) (*proto.BoundWorkflowEventTrigger, bool, error) {
	record, err := s.eventTriggers.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	trigger, err := triggerFromRecord(record)
	return trigger, err == nil && trigger.GetId() != "", err
}

func (s *workflowStateStore) listTriggers(ctx context.Context) ([]*proto.BoundWorkflowEventTrigger, error) {
	records, err := s.eventTriggers.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	triggers := make([]*proto.BoundWorkflowEventTrigger, 0, len(records))
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

func (s *workflowStateStore) matchTriggers(ctx context.Context, ownerKey string, event *proto.WorkflowEvent) ([]*proto.BoundWorkflowEventTrigger, error) {
	seen := map[string]struct{}{}
	triggers := make([]*proto.BoundWorkflowEventTrigger, 0)
	for _, key := range eventLookupKeys(ownerKey, event) {
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
			if !found || !eventMatchesTrigger(event, trigger) {
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

func (s *workflowStateStore) putExecutionRef(ctx context.Context, ref *proto.WorkflowExecutionReference) error {
	ref = cloneExecutionReference(ref)
	if ref == nil || strings.TrimSpace(ref.GetId()) == "" {
		return nil
	}
	return s.executionRefs.Put(ctx, s.executionRefRecord(ref))
}

func (s *workflowStateStore) getExecutionRef(ctx context.Context, id string) (*proto.WorkflowExecutionReference, bool, error) {
	record, err := s.executionRefs.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	ref, err := executionRefFromRecord(record)
	return ref, err == nil && ref.GetId() != "", err
}

func (s *workflowStateStore) listExecutionRefs(ctx context.Context, subjectID string) ([]*proto.WorkflowExecutionReference, error) {
	var (
		records []gestalt.Record
		err     error
	)
	subjectID = strings.TrimSpace(subjectID)
	if subjectID == "" {
		records, err = s.executionRefs.GetAll(ctx, nil)
	} else {
		records, err = s.executionRefs.Index(indexBySubject).GetAll(ctx, nil, subjectID)
	}
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	refs := make([]*proto.WorkflowExecutionReference, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		ref, err := executionRefFromRecord(record)
		if err != nil {
			return nil, err
		}
		if subjectID == "" || ref.GetSubjectId() == subjectID {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

func (s *workflowStateStore) scheduleRecord(schedule *proto.BoundWorkflowSchedule) gestalt.Record {
	payload, _ := marshalProto(schedule)
	now := time.Now().UTC()
	return gestalt.Record{
		"id":         s.scopedID(schedule.GetId()),
		"scope_id":   s.scopeID,
		"owner_key":  targetOwnerKey(schedule.GetTarget()),
		"created_at": timeFromProtoOrNow(schedule.GetCreatedAt(), now),
		"updated_at": timeFromProtoOrNow(schedule.GetUpdatedAt(), now),
		"payload":    payload,
	}
}

func (s *workflowStateStore) runRecord(run *proto.BoundWorkflowRun) gestalt.Record {
	payload, _ := marshalProto(run)
	now := time.Now().UTC()
	return gestalt.Record{
		"id":           s.scopedID(run.GetId()),
		"scope_id":     s.scopeID,
		"owner_key":    targetOwnerKey(run.GetTarget()),
		"workflow_key": strings.TrimSpace(run.GetWorkflowKey()),
		"status":       int64(run.GetStatus()),
		"created_at":   timeFromProtoOrNow(run.GetCreatedAt(), now),
		"started_at":   timeFromProto(run.GetStartedAt()),
		"completed_at": timeFromProto(run.GetCompletedAt()),
		"payload":      payload,
	}
}

func runFromRecord(record gestalt.Record) (*proto.BoundWorkflowRun, error) {
	var run proto.BoundWorkflowRun
	if err := unmarshalRecordPayload(record, &run); err != nil {
		return nil, err
	}
	return cloneRun(&run), nil
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
		Status:             proto.WorkflowRunStatus(recordInt64(record, "status")),
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

func (s *workflowStateStore) triggerRecord(trigger *proto.BoundWorkflowEventTrigger) gestalt.Record {
	payload, _ := marshalProto(trigger)
	now := time.Now().UTC()
	return gestalt.Record{
		"id":         s.scopedID(trigger.GetId()),
		"scope_id":   s.scopeID,
		"owner_key":  targetOwnerKey(trigger.GetTarget()),
		"paused":     trigger.GetPaused(),
		"created_at": timeFromProtoOrNow(trigger.GetCreatedAt(), now),
		"updated_at": timeFromProtoOrNow(trigger.GetUpdatedAt(), now),
		"payload":    payload,
	}
}

func triggerFromRecord(record gestalt.Record) (*proto.BoundWorkflowEventTrigger, error) {
	var trigger proto.BoundWorkflowEventTrigger
	if err := unmarshalRecordPayload(record, &trigger); err != nil {
		return nil, err
	}
	return cloneTrigger(&trigger), nil
}

func (s *workflowStateStore) executionRefRecord(ref *proto.WorkflowExecutionReference) gestalt.Record {
	payload, _ := marshalProto(ref)
	return gestalt.Record{
		"id":            s.scopedID(ref.GetId()),
		"scope_id":      s.scopeID,
		"provider_name": ref.GetProviderName(),
		"subject_id":    ref.GetSubjectId(),
		"created_at":    timeFromProto(ref.GetCreatedAt()),
		"revoked_at":    timeFromProto(ref.GetRevokedAt()),
		"payload":       payload,
	}
}

func executionRefFromRecord(record gestalt.Record) (*proto.WorkflowExecutionReference, error) {
	var ref proto.WorkflowExecutionReference
	if err := unmarshalRecordPayload(record, &ref); err != nil {
		return nil, err
	}
	return cloneExecutionReference(&ref), nil
}

func marshalProto(msg gproto.Message) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	return gproto.MarshalOptions{Deterministic: true}.Marshal(msg)
}

func unmarshalRecordPayload(record gestalt.Record, msg gproto.Message) error {
	payload := recordBytes(record, "payload")
	if len(payload) == 0 {
		return fmt.Errorf("record %q has empty payload", recordString(record, "id"))
	}
	return gproto.Unmarshal(payload, msg)
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

func transactionGetRecord(ctx context.Context, store *gestalt.TransactionObjectStore, id string) (gestalt.Record, bool, error) {
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

func timeFromProto(value *timestamppb.Timestamp) *time.Time {
	if value == nil || !value.IsValid() {
		return nil
	}
	asTime := value.AsTime().UTC()
	return &asTime
}

func timeFromProtoOrNow(value *timestamppb.Timestamp, fallback time.Time) time.Time {
	if value == nil || !value.IsValid() {
		return fallback.UTC()
	}
	return value.AsTime().UTC()
}
