package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	storeTemporalRuns             = "workflow_temporal_runs"
	storeTemporalWorkflowKeys     = "workflow_temporal_workflow_keys"
	storeTemporalIdempotency      = "workflow_temporal_idempotency"
	storeTemporalSchedules        = "workflow_temporal_schedules"
	storeTemporalEventTriggers    = "workflow_temporal_event_triggers"
	storeTemporalEventTriggerKeys = "workflow_temporal_event_trigger_keys"
	storeTemporalExecutionRefs    = "workflow_temporal_execution_refs"

	indexBySubject   = "by_subject"
	indexByMatchKey  = "by_match_key"
	indexByTriggerID = "by_trigger_id"
)

type workflowStateStore struct {
	db      *gestalt.IndexedDBClient
	scopeID string

	runs             *gestalt.ObjectStoreClient
	workflowKeys     *gestalt.ObjectStoreClient
	idempotency      *gestalt.ObjectStoreClient
	schedules        *gestalt.ObjectStoreClient
	eventTriggers    *gestalt.ObjectStoreClient
	eventTriggerKeys *gestalt.ObjectStoreClient
	executionRefs    *gestalt.ObjectStoreClient
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
	store := &workflowStateStore{
		scopeID:          scopeID,
		db:               db,
		runs:             db.ObjectStore(storeTemporalRuns),
		workflowKeys:     db.ObjectStore(storeTemporalWorkflowKeys),
		idempotency:      db.ObjectStore(storeTemporalIdempotency),
		schedules:        db.ObjectStore(storeTemporalSchedules),
		eventTriggers:    db.ObjectStore(storeTemporalEventTriggers),
		eventTriggerKeys: db.ObjectStore(storeTemporalEventTriggerKeys),
		executionRefs:    db.ObjectStore(storeTemporalExecutionRefs),
	}
	return store, nil
}

func (s *workflowStateStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *workflowStateStore) putRun(ctx context.Context, run *proto.BoundWorkflowRun) error {
	run = cloneRun(run)
	if run == nil || strings.TrimSpace(run.GetId()) == "" {
		return nil
	}
	existing, runFound, err := s.getRun(ctx, run.GetId())
	if err != nil {
		return err
	}
	if runFound && workflowRunTerminal(existing.GetStatus()) && !workflowRunTerminal(run.GetStatus()) {
		return nil
	}
	now := time.Now().UTC()
	workflowKey := strings.TrimSpace(run.GetWorkflowKey())
	keyFound := false
	if workflowKey != "" && !workflowRunTerminal(run.GetStatus()) {
		if _, err := s.workflowKeys.Get(ctx, s.scopedID(workflowKey)); err == nil {
			keyFound = true
		} else if !errors.Is(err, gestalt.ErrNotFound) {
			return err
		}
	}
	tx, err := s.db.Transaction(ctx, []string{storeTemporalRuns, storeTemporalWorkflowKeys}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	runStore := tx.ObjectStore(storeTemporalRuns)
	keyStore := tx.ObjectStore(storeTemporalWorkflowKeys)
	if runFound {
		if err := runStore.Delete(ctx, s.scopedID(run.GetId())); err != nil {
			return err
		}
	}
	if err := runStore.Put(ctx, s.runRecord(run, now)); err != nil {
		return err
	}
	if workflowKey != "" && !workflowRunTerminal(run.GetStatus()) {
		if keyFound {
			if err := keyStore.Delete(ctx, s.scopedID(workflowKey)); err != nil {
				return err
			}
		}
		if err := keyStore.Put(ctx, gestalt.Record{
			"id":         s.scopedID(workflowKey),
			"scope_id":   s.scopeID,
			"run_id":     run.GetId(),
			"created_at": timeFromProtoOrNow(run.GetCreatedAt(), now),
			"updated_at": now,
		}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	if workflowKey != "" && workflowRunTerminal(run.GetStatus()) {
		if record, err := s.workflowKeys.Get(ctx, s.scopedID(workflowKey)); err == nil && recordString(record, "run_id") == run.GetId() {
			if err := s.workflowKeys.Delete(ctx, s.scopedID(workflowKey)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
				return err
			}
		} else if err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return err
		}
	}
	return nil
}

func (s *workflowStateStore) getRun(ctx context.Context, id string) (*proto.BoundWorkflowRun, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, false, nil
	}
	record, err := s.runs.Get(ctx, s.scopedID(id))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	run, err := runFromRecord(record)
	return run, err == nil && run.GetId() != "", err
}

func (s *workflowStateStore) listRuns(ctx context.Context) ([]*proto.BoundWorkflowRun, error) {
	records, err := s.runs.GetAll(ctx, nil)
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
	return runs, nil
}

func (s *workflowStateStore) putWorkflowKey(ctx context.Context, workflowKey string, run *proto.BoundWorkflowRun) error {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" || run == nil || strings.TrimSpace(run.GetId()) == "" {
		return nil
	}
	storedRun, found, err := s.getRun(ctx, run.GetId())
	if err != nil {
		return err
	}
	if found && workflowRunTerminal(storedRun.GetStatus()) {
		if err := s.workflowKeys.Delete(ctx, s.scopedID(workflowKey)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return err
		}
		return nil
	}
	if workflowRunTerminal(run.GetStatus()) {
		if record, err := s.workflowKeys.Get(ctx, s.scopedID(workflowKey)); err == nil && recordString(record, "run_id") == run.GetId() {
			if err := s.workflowKeys.Delete(ctx, s.scopedID(workflowKey)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
				return err
			}
		} else if err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			return err
		}
		return nil
	}
	now := time.Now().UTC()
	return s.workflowKeys.Put(ctx, gestalt.Record{
		"id":         s.scopedID(workflowKey),
		"scope_id":   s.scopeID,
		"run_id":     run.GetId(),
		"created_at": timeFromProtoOrNow(run.GetCreatedAt(), now),
		"updated_at": now,
	})
}

func (s *workflowStateStore) getWorkflowKey(ctx context.Context, workflowKey string) (*proto.BoundWorkflowRun, bool, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, false, nil
	}
	record, err := s.workflowKeys.Get(ctx, s.scopedID(workflowKey))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	runID := recordString(record, "run_id")
	run, found, err := s.getRun(ctx, runID)
	if err != nil || !found {
		return nil, found, err
	}
	if workflowRunTerminal(run.GetStatus()) {
		_ = s.workflowKeys.Delete(ctx, s.scopedID(workflowKey))
		return nil, false, nil
	}
	return run, true, nil
}

func (s *workflowStateStore) putIdempotency(ctx context.Context, ownerKey, key string, resp *proto.SignalWorkflowRunResponse) error {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	if ownerKey == "" || key == "" || resp == nil {
		return nil
	}
	payload, err := marshalProto(resp)
	if err != nil {
		return err
	}
	return s.idempotency.Put(ctx, gestalt.Record{
		"id":              s.scopedID(ownerKey, key),
		"scope_id":        s.scopeID,
		"owner_key":       ownerKey,
		"idempotency_key": key,
		"created_at":      time.Now().UTC(),
		"payload":         payload,
	})
}

func (s *workflowStateStore) getIdempotency(ctx context.Context, ownerKey, key string) (*proto.SignalWorkflowRunResponse, bool, error) {
	record, err := s.idempotency.Get(ctx, s.scopedID(ownerKey, key))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var resp proto.SignalWorkflowRunResponse
	if err := unmarshalRecordPayload(record, &resp); err != nil {
		return nil, false, err
	}
	return cloneSignalResponse(&resp), true, nil
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

func (s *workflowStateStore) deleteSchedule(ctx context.Context, id string) error {
	err := s.schedules.Delete(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	return err
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

func (s *workflowStateStore) runRecord(run *proto.BoundWorkflowRun, now time.Time) gestalt.Record {
	payload, _ := marshalProto(run)
	return gestalt.Record{
		"id":           s.scopedID(run.GetId()),
		"scope_id":     s.scopeID,
		"owner_key":    targetOwnerKey(run.GetTarget()),
		"workflow_key": strings.TrimSpace(run.GetWorkflowKey()),
		"status":       run.GetStatus().String(),
		"created_at":   timeFromProtoOrNow(run.GetCreatedAt(), now),
		"updated_at":   now,
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
