package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storeTemporalDefinitions       = "workflow_temporal_definitions"
	storeTemporalRunIdempotency    = "workflow_temporal_run_idempotency"
	storeTemporalSignalIdempotency = "workflow_temporal_signal_idempotency"
)

type workflowStateStore struct {
	db      indexeddb.Database
	scopeID string

	definitions       indexeddb.ObjectStore
	runIdempotency    indexeddb.ObjectStore
	signalIdempotency indexeddb.ObjectStore
}

type matchedWorkflowActivation struct {
	Definition *gestalt.WorkflowDefinition
	Activation gestalt.WorkflowActivation
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
		definitions:       db.ObjectStore(storeTemporalDefinitions),
		runIdempotency:    db.ObjectStore(storeTemporalRunIdempotency),
		signalIdempotency: db.ObjectStore(storeTemporalSignalIdempotency),
	}
	return store, nil
}

func ensureWorkflowStateStores(ctx context.Context, db indexeddb.Database) error {
	if db == nil {
		return nil
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalDefinitions, temporalDefinitionSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow definition store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalRunIdempotency, temporalRunIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow run idempotency store: %w", err)
	}
	if _, err := db.CreateObjectStore(ctx, storeTemporalSignalIdempotency, temporalSignalIdempotencySchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return fmt.Errorf("create workflow signal idempotency store: %w", err)
	}
	return nil
}

func temporalRunIdempotencySchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
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

func temporalSignalIdempotencySchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
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

func (s *workflowStateStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
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
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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

func (s *workflowStateStore) completeRunIdempotency(ctx context.Context, ownerKey, key, fingerprint string, run *gestalt.WorkflowRun, retention time.Duration, now time.Time) error {
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

func (s *workflowStateStore) completeRunIdempotencyOnce(ctx context.Context, ownerKey, key, fingerprint string, run *gestalt.WorkflowRun, retention time.Duration, now time.Time) error {
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
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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
		if existing.Status == "completed" && workflowRunFromPayload(existing.RunPayload) != nil {
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
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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

func (s *workflowStateStore) runIdempotencyID(ownerKey, key string) string {
	return s.scopedID("start", hashID(ownerKey, key))
}

func (s *workflowStateStore) signalIdempotencyID(key string) string {
	return s.scopedID("signal", hashID(key))
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

func workflowRunFromPayload(payload []byte) *gestalt.WorkflowRun {
	run, err := decodeNativePayload[gestalt.WorkflowRun](payload, "workflow run")
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

func transactionGetRecord(ctx context.Context, store indexeddb.TransactionObjectStore, id string) (gestalt.Record, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, false, nil
	}
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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
