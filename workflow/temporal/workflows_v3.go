package temporal

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	runHandleKindV3 = "temporal-run-v3"

	queryLedgerGet = "gestalt.ledger.get"

	updateLaneSignalOrStart = "gestalt.workflow_key.signal_or_start"
	updateLaneStartRun      = "gestalt.workflow_key.start_run"
	updateLaneSignalRun     = "gestalt.workflow_key.signal_run"
	updateLaneCancelRun     = "gestalt.workflow_key.cancel_run"
	updateLedgerReserve     = "gestalt.ledger.reserve"
	updateLedgerComplete    = "gestalt.ledger.complete"

	signalRunAddV3       = "gestalt.run.add_signal_v3"
	signalRunCancelV3    = "gestalt.run.cancel_v3"
	signalLaneAck        = "gestalt.workflow_key.ack"
	signalLaneRunDone    = "gestalt.workflow_key.run_done"
	signalLedgerCompact  = "gestalt.ledger.compact"
	signalRunIndexBestV3 = "gestalt.run.index_best_effort_v3"

	workflowInvokeMetadataWorkflowKey = "workflow_key"
)

type runWorkflowV3Input struct {
	ProviderName                  string
	ScopeID                       string
	IndexShardCount               int
	TaskQueue                     string
	WorkflowRunTimeoutNS          time.Duration
	WorkflowTaskTimeoutNS         time.Duration
	ActivityStartToCloseTimeoutNS time.Duration
	ExecutionRef                  string
	WorkflowKey                   string
	LaneWorkflowID                string
	LaneTemporalRunID             string
	LogicalRunKey                 string
	OwnerKey                      string
	TargetPayload                 []byte
	TriggerPayload                []byte
	CreatedByPayload              []byte
	RequireSignal                 bool
	ScheduleID                    string
}

type laneWorkflowInput struct {
	ProviderName                  string
	ScopeID                       string
	IndexShardCount               int
	TaskQueue                     string
	WorkflowRunTimeoutNS          time.Duration
	WorkflowTaskTimeoutNS         time.Duration
	ActivityStartToCloseTimeoutNS time.Duration
	IdempotencyRetentionNS        time.Duration
	WorkflowKey                   string
}

type laneWorkflowSnapshot struct {
	Input          laneWorkflowInput
	Active         *laneActiveRun
	NextRunNumber  int64
	NextSequence   int64
	Responses      map[string]*laneStoredResponse
	CompletedAtMap map[string]time.Time
}

type laneActiveRun struct {
	LogicalRunKey    string
	RunWorkflowID    string
	RunTemporalRunID string
	RunID            string
	OwnerKey         string
	TargetPayload    []byte
	TriggerPayload   []byte
	CreatedByPayload []byte
	ExecutionRef     string
	CreatedAt        time.Time
}

type laneStoredResponse struct {
	ResponsePayload []byte
	StoredAt        time.Time
	RequestID       string
}

type laneSignalRequest struct {
	OwnerKey         string
	TargetPayload    []byte
	ExecutionRef     string
	CreatedByPayload []byte
	SignalPayload    []byte
	RequestID        string
	IdempotencyKey   string
}

type laneStartRequest struct {
	OwnerKey         string
	TargetPayload    []byte
	ExecutionRef     string
	CreatedByPayload []byte
	RequestID        string
}

type laneSignalRunRequest struct {
	RunID          string
	OwnerKey       string
	SignalPayload  []byte
	RequestID      string
	IdempotencyKey string
}

type laneCancelRequest struct {
	RunID  string
	Reason string
}

type runSignalMessage struct {
	AckID         string
	SignalPayload []byte
}

type runCancelMessage struct {
	AckID  string
	Reason string
}

type laneAckMessage struct {
	AckID           string
	ResponsePayload []byte
	RunPayload      []byte
	Error           string
}

type laneRunCompleted struct {
	LogicalRunKey string
	RunPayload    []byte
}

type ownerLedgerInput struct {
	ScopeID                string
	Shard                  int
	IdempotencyRetentionNS time.Duration
	Snapshot               *ownerLedgerSnapshot
}

type ownerLedgerSnapshot struct {
	Entries map[string]*ownerLedgerEntry
}

type ownerLedgerEntry struct {
	Key             string
	Status          string
	Operation       string
	Fingerprint     string
	OwnerKey        string
	WorkflowKey     string
	RunID           string
	SignalID        string
	LaneWorkflowID  string
	LaneUpdateID    string
	ResponsePayload []byte
	RunPayload      []byte
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExpiresAt       time.Time
}

type ownerLedgerReserveRequest struct {
	Key            string
	Operation      string
	Fingerprint    string
	OwnerKey       string
	WorkflowKey    string
	RunID          string
	SignalID       string
	LaneWorkflowID string
	LaneUpdateID   string
	RetentionNS    time.Duration
}

type ownerLedgerReserveResponse struct {
	Entry    *ownerLedgerEntry
	Existing bool
}

type ownerLedgerCompleteRequest struct {
	Key             string
	Fingerprint     string
	ResponsePayload []byte
	RunPayload      []byte
	RetentionNS     time.Duration
}

func gestaltOwnerLedgerWorkflow(ctx workflow.Context, input ownerLedgerInput) error {
	state := &ownerLedgerSnapshot{Entries: map[string]*ownerLedgerEntry{}}
	if input.Snapshot != nil && input.Snapshot.Entries != nil {
		state = input.Snapshot
	}
	retention := input.IdempotencyRetentionNS
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	compact := func(now time.Time) {
		for key, entry := range state.Entries {
			if entry == nil || (!entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now)) {
				delete(state.Entries, key)
			}
		}
	}
	_ = workflow.SetQueryHandler(ctx, queryLedgerGet, func(key string) (*ownerLedgerEntry, error) {
		entry := state.Entries[strings.TrimSpace(key)]
		if entry == nil {
			return &ownerLedgerEntry{}, nil
		}
		return cloneLedgerEntry(entry), nil
	})
	if err := workflow.SetUpdateHandler(ctx, updateLedgerReserve, func(ctx workflow.Context, req ownerLedgerReserveRequest) (*ownerLedgerReserveResponse, error) {
		now := workflow.Now(ctx).UTC()
		compact(now)
		req.Key = strings.TrimSpace(req.Key)
		req.Fingerprint = strings.TrimSpace(req.Fingerprint)
		if req.Key == "" || req.Fingerprint == "" {
			return nil, fmt.Errorf("invalid_argument: ledger key and fingerprint are required")
		}
		if existing := state.Entries[req.Key]; existing != nil {
			if existing.Fingerprint != req.Fingerprint {
				return nil, fmt.Errorf("failed_precondition: idempotency key %q is already reserved for a different request", req.Key)
			}
			return &ownerLedgerReserveResponse{Entry: cloneLedgerEntry(existing), Existing: true}, nil
		}
		retention := req.RetentionNS
		if retention <= 0 {
			retention = input.IdempotencyRetentionNS
		}
		if retention <= 0 {
			retention = defaultIdempotencyRetention
		}
		entry := &ownerLedgerEntry{
			Key:            req.Key,
			Status:         "reserved",
			Operation:      strings.TrimSpace(req.Operation),
			Fingerprint:    req.Fingerprint,
			OwnerKey:       strings.TrimSpace(req.OwnerKey),
			WorkflowKey:    strings.TrimSpace(req.WorkflowKey),
			RunID:          strings.TrimSpace(req.RunID),
			SignalID:       strings.TrimSpace(req.SignalID),
			LaneWorkflowID: strings.TrimSpace(req.LaneWorkflowID),
			LaneUpdateID:   strings.TrimSpace(req.LaneUpdateID),
			CreatedAt:      now,
			UpdatedAt:      now,
			ExpiresAt:      now.Add(retention),
		}
		state.Entries[entry.Key] = entry
		return &ownerLedgerReserveResponse{Entry: cloneLedgerEntry(entry)}, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, updateLedgerComplete, func(ctx workflow.Context, req ownerLedgerCompleteRequest) (*ownerLedgerEntry, error) {
		now := workflow.Now(ctx).UTC()
		compact(now)
		key := strings.TrimSpace(req.Key)
		fingerprint := strings.TrimSpace(req.Fingerprint)
		entry := state.Entries[key]
		if entry == nil {
			return nil, fmt.Errorf("failed_precondition: ledger key %q was not reserved", key)
		}
		if entry.Fingerprint != fingerprint {
			return nil, fmt.Errorf("failed_precondition: ledger key %q is reserved for a different request", key)
		}
		retention := req.RetentionNS
		if retention <= 0 {
			retention = input.IdempotencyRetentionNS
		}
		if retention <= 0 {
			retention = defaultIdempotencyRetention
		}
		entry.Status = "completed"
		entry.ResponsePayload = append([]byte(nil), req.ResponsePayload...)
		entry.RunPayload = append([]byte(nil), req.RunPayload...)
		entry.UpdatedAt = now
		entry.ExpiresAt = now.Add(retention)
		return cloneLedgerEntry(entry), nil
	}); err != nil {
		return err
	}
	compactCh := workflow.GetSignalChannel(ctx, signalLedgerCompact)
	for {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(compactCh, func(c workflow.ReceiveChannel, more bool) {
			var reason string
			c.Receive(ctx, &reason)
			compact(workflow.Now(ctx).UTC())
		})
		selector.Select(ctx)
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() && workflow.AllHandlersFinished(ctx) {
			compact(workflow.Now(ctx).UTC())
			return workflow.NewContinueAsNewError(ctx, gestaltOwnerLedgerWorkflow, ownerLedgerInput{
				ScopeID:                input.ScopeID,
				Shard:                  input.Shard,
				IdempotencyRetentionNS: retention,
				Snapshot:               state,
			})
		}
	}
}

func gestaltWorkflowKeyLaneV1(ctx workflow.Context, snapshot laneWorkflowSnapshot) error {
	state := snapshot
	if state.Responses == nil {
		state.Responses = map[string]*laneStoredResponse{}
	}
	if state.CompletedAtMap == nil {
		state.CompletedAtMap = map[string]time.Time{}
	}
	if state.NextRunNumber <= 0 {
		state.NextRunNumber = 1
	}
	if state.NextSequence <= 0 {
		state.NextSequence = 1
	}
	retention := state.Input.IdempotencyRetentionNS
	if retention <= 0 {
		retention = defaultIdempotencyRetention
	}
	info := workflow.GetInfo(ctx)
	mutex := workflow.NewMutex(ctx)
	ackCh := workflow.GetSignalChannel(ctx, signalLaneAck)
	doneCh := workflow.GetSignalChannel(ctx, signalLaneRunDone)
	acks := map[string]*laneAckMessage{}

	drain := func() {
		for {
			var ack laneAckMessage
			if !ackCh.ReceiveAsync(&ack) {
				break
			}
			ack.AckID = strings.TrimSpace(ack.AckID)
			if ack.AckID != "" {
				acks[ack.AckID] = &ack
			}
		}
		for {
			var done laneRunCompleted
			if !doneCh.ReceiveAsync(&done) {
				break
			}
			if state.Active != nil && strings.TrimSpace(done.LogicalRunKey) == state.Active.LogicalRunKey {
				if run := runFromPayload(done.RunPayload); run != nil && workflowRunTerminal(run.GetStatus()) {
					state.CompletedAtMap[state.Active.LogicalRunKey] = workflow.Now(ctx).UTC()
					state.Active = nil
				}
			}
		}
		now := workflow.Now(ctx).UTC()
		for key, stored := range state.Responses {
			if stored == nil || !stored.StoredAt.Add(retention).After(now) {
				delete(state.Responses, key)
			}
		}
		for key, completedAt := range state.CompletedAtMap {
			if !completedAt.Add(retention).After(now) {
				delete(state.CompletedAtMap, key)
			}
		}
	}

	waitAck := func(ctx workflow.Context, ackID string) (*laneAckMessage, error) {
		ackID = strings.TrimSpace(ackID)
		if ackID == "" {
			return nil, fmt.Errorf("invalid_argument: ack id is required")
		}
		for {
			drain()
			if ack := acks[ackID]; ack != nil {
				delete(acks, ackID)
				if ack.Error != "" {
					return nil, fmt.Errorf("%s", ack.Error)
				}
				return ack, nil
			}
			if err := workflow.Await(ctx, func() bool {
				drain()
				return acks[ackID] != nil
			}); err != nil {
				return nil, err
			}
		}
	}

	startChild := func(ctx workflow.Context, ownerKey string, target *proto.BoundWorkflowTarget, executionRef string, createdBy *proto.WorkflowActor, requireSignal bool) (*laneActiveRun, error) {
		now := workflow.Now(ctx).UTC()
		logicalRunKey := fmt.Sprintf("%020d", state.NextRunNumber)
		state.NextRunNumber++
		trigger := newManualTrigger()
		runWorkflowID := workflowID(state.Input.ScopeID, "run-v3-key", state.Input.WorkflowKey, logicalRunKey)
		childInput := runWorkflowV3Input{
			ProviderName:                  state.Input.ProviderName,
			ScopeID:                       state.Input.ScopeID,
			IndexShardCount:               state.Input.IndexShardCount,
			TaskQueue:                     state.Input.TaskQueue,
			WorkflowRunTimeoutNS:          state.Input.WorkflowRunTimeoutNS,
			WorkflowTaskTimeoutNS:         state.Input.WorkflowTaskTimeoutNS,
			ActivityStartToCloseTimeoutNS: state.Input.ActivityStartToCloseTimeoutNS,
			ExecutionRef:                  strings.TrimSpace(executionRef),
			WorkflowKey:                   state.Input.WorkflowKey,
			LaneWorkflowID:                info.WorkflowExecution.ID,
			LaneTemporalRunID:             info.WorkflowExecution.RunID,
			LogicalRunKey:                 logicalRunKey,
			OwnerKey:                      strings.TrimSpace(ownerKey),
			TargetPayload:                 protoPayload(target),
			TriggerPayload:                protoPayload(trigger),
			CreatedByPayload:              protoPayload(createdBy),
			RequireSignal:                 requireSignal,
		}
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:               runWorkflowID,
			TaskQueue:                state.Input.TaskQueue,
			WorkflowRunTimeout:       state.Input.WorkflowRunTimeoutNS,
			WorkflowTaskTimeout:      state.Input.WorkflowTaskTimeoutNS,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
			WaitForCancellation:      false,
			WorkflowExecutionTimeout: state.Input.WorkflowRunTimeoutNS,
		})
		future := workflow.ExecuteChildWorkflow(childCtx, gestaltRunWorkflowV3, childInput)
		var execution workflow.Execution
		if err := future.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
			return nil, err
		}
		runID := encodeV3RunHandle(v3RunHandle{
			Kind:             runHandleKindV3,
			LaneWorkflowID:   info.WorkflowExecution.ID,
			RunWorkflowID:    execution.ID,
			RunTemporalRunID: execution.RunID,
			LogicalRunKey:    logicalRunKey,
			WorkflowKey:      state.Input.WorkflowKey,
			OwnerKey:         ownerKey,
		})
		return &laneActiveRun{
			LogicalRunKey:    logicalRunKey,
			RunWorkflowID:    execution.ID,
			RunTemporalRunID: execution.RunID,
			RunID:            runID,
			OwnerKey:         strings.TrimSpace(ownerKey),
			TargetPayload:    protoPayload(target),
			TriggerPayload:   protoPayload(trigger),
			CreatedByPayload: protoPayload(createdBy),
			ExecutionRef:     strings.TrimSpace(executionRef),
			CreatedAt:        now,
		}, nil
	}

	runFromActive := func(active *laneActiveRun) *proto.BoundWorkflowRun {
		if active == nil {
			return nil
		}
		return &proto.BoundWorkflowRun{
			Id:           active.RunID,
			Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:       targetFromPayload(active.TargetPayload),
			Trigger:      triggerFromPayload(active.TriggerPayload),
			CreatedAt:    timestamppb.New(active.CreatedAt),
			CreatedBy:    actorFromPayload(active.CreatedByPayload),
			ExecutionRef: strings.TrimSpace(active.ExecutionRef),
			WorkflowKey:  state.Input.WorkflowKey,
		}
	}
	if err := workflow.SetQueryHandler(ctx, queryLaneRun, func() (*proto.BoundWorkflowRun, error) {
		return runFromActive(state.Active), nil
	}); err != nil {
		return err
	}

	deliverSignal := func(ctx workflow.Context, active *laneActiveRun, reqID string, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, error) {
		if active == nil {
			return nil, fmt.Errorf("failed_precondition: workflow key %q has no active run", state.Input.WorkflowKey)
		}
		assigned := cloneSignal(signal)
		if assigned.GetSequence() <= 0 {
			assigned.Sequence = state.NextSequence
		}
		if assigned.GetSequence() >= state.NextSequence {
			state.NextSequence = assigned.GetSequence() + 1
		}
		if strings.TrimSpace(assigned.GetId()) == "" {
			assigned.Id = "signal:" + hashID(active.RunID, assigned.GetName(), fmt.Sprintf("%d", assigned.GetSequence()), assigned.GetIdempotencyKey())
		}
		ackID := "ack:" + hashID(info.WorkflowExecution.ID, active.RunID, assigned.GetId(), reqID)
		msg := runSignalMessage{AckID: ackID, SignalPayload: protoPayload(assigned)}
		if err := workflow.SignalExternalWorkflow(ctx, active.RunWorkflowID, active.RunTemporalRunID, signalRunAddV3, msg).Get(ctx, nil); err != nil {
			return nil, err
		}
		ack, err := waitAck(ctx, ackID)
		if err != nil {
			return nil, err
		}
		if resp := signalResponseFromPayload(ack.ResponsePayload); resp != nil {
			return resp, nil
		}
		return &proto.SignalWorkflowRunResponse{
			Run:         runFromActive(active),
			Signal:      cloneSignal(assigned),
			StartedRun:  false,
			WorkflowKey: state.Input.WorkflowKey,
		}, nil
	}

	handleSignalOrStart := func(ctx workflow.Context, req laneSignalRequest, requireExisting bool) (*proto.SignalWorkflowRunResponse, error) {
		if err := mutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer mutex.Unlock()
		drain()
		key := strings.TrimSpace(req.IdempotencyKey)
		if key != "" {
			if stored := state.Responses[key]; stored != nil {
				if resp := signalResponseFromPayload(stored.ResponsePayload); resp != nil {
					return resp, nil
				}
			}
		}
		if requireExisting && state.Active == nil {
			return nil, fmt.Errorf("failed_precondition: workflow key %q has no matching active run", state.Input.WorkflowKey)
		}
		started := false
		if state.Active == nil {
			active, err := startChild(ctx, req.OwnerKey, targetFromPayload(req.TargetPayload), req.ExecutionRef, actorFromPayload(req.CreatedByPayload), true)
			if err != nil {
				return nil, err
			}
			state.Active = active
			started = true
		}
		resp, err := deliverSignal(ctx, state.Active, req.RequestID, signalFromPayload(req.SignalPayload))
		if err != nil {
			state.Active = nil
			active, startErr := startChild(ctx, req.OwnerKey, targetFromPayload(req.TargetPayload), req.ExecutionRef, actorFromPayload(req.CreatedByPayload), true)
			if startErr != nil {
				return nil, err
			}
			state.Active = active
			started = true
			resp, err = deliverSignal(ctx, state.Active, req.RequestID, signalFromPayload(req.SignalPayload))
			if err != nil {
				return nil, err
			}
		}
		resp.StartedRun = started
		resp.WorkflowKey = state.Input.WorkflowKey
		if key != "" {
			state.Responses[key] = &laneStoredResponse{ResponsePayload: protoPayload(resp), StoredAt: workflow.Now(ctx).UTC(), RequestID: strings.TrimSpace(req.RequestID)}
		}
		return resp, nil
	}

	if err := workflow.SetUpdateHandler(ctx, updateLaneSignalOrStart, func(ctx workflow.Context, req laneSignalRequest) (*proto.SignalWorkflowRunResponse, error) {
		return handleSignalOrStart(ctx, req, false)
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, updateLaneSignalRun, func(ctx workflow.Context, req laneSignalRunRequest) (*proto.SignalWorkflowRunResponse, error) {
		sreq := laneSignalRequest{
			OwnerKey:       req.OwnerKey,
			SignalPayload:  append([]byte(nil), req.SignalPayload...),
			RequestID:      req.RequestID,
			IdempotencyKey: req.IdempotencyKey,
		}
		if state.Active == nil || strings.TrimSpace(req.RunID) != state.Active.RunID {
			return nil, fmt.Errorf("failed_precondition: workflow key %q has no matching active run", state.Input.WorkflowKey)
		}
		sreq.TargetPayload = append([]byte(nil), state.Active.TargetPayload...)
		sreq.ExecutionRef = state.Active.ExecutionRef
		sreq.CreatedByPayload = append([]byte(nil), state.Active.CreatedByPayload...)
		return handleSignalOrStart(ctx, sreq, true)
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, updateLaneStartRun, func(ctx workflow.Context, req laneStartRequest) (*proto.BoundWorkflowRun, error) {
		if err := mutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer mutex.Unlock()
		drain()
		if state.Active != nil {
			return nil, fmt.Errorf("failed_precondition: workflow key %q already has an active run", state.Input.WorkflowKey)
		}
		active, err := startChild(ctx, req.OwnerKey, targetFromPayload(req.TargetPayload), req.ExecutionRef, actorFromPayload(req.CreatedByPayload), false)
		if err != nil {
			return nil, err
		}
		state.Active = active
		return runFromActive(active), nil
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, updateLaneCancelRun, func(ctx workflow.Context, req laneCancelRequest) (*proto.BoundWorkflowRun, error) {
		if err := mutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer mutex.Unlock()
		drain()
		if state.Active == nil || strings.TrimSpace(req.RunID) != state.Active.RunID {
			return nil, fmt.Errorf("failed_precondition: workflow key %q has no matching active run", state.Input.WorkflowKey)
		}
		ackID := "cancel:" + hashID(info.WorkflowExecution.ID, state.Active.RunID, req.Reason)
		msg := runCancelMessage{AckID: ackID, Reason: strings.TrimSpace(req.Reason)}
		if err := workflow.SignalExternalWorkflow(ctx, state.Active.RunWorkflowID, state.Active.RunTemporalRunID, signalRunCancelV3, msg).Get(ctx, nil); err != nil {
			return nil, err
		}
		ack, err := waitAck(ctx, ackID)
		if err != nil {
			return nil, err
		}
		run := runFromPayload(ack.RunPayload)
		state.CompletedAtMap[state.Active.LogicalRunKey] = workflow.Now(ctx).UTC()
		state.Active = nil
		return run, nil
	}); err != nil {
		return err
	}

	for {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(ackCh, func(c workflow.ReceiveChannel, more bool) {
			var ack laneAckMessage
			c.Receive(ctx, &ack)
			ack.AckID = strings.TrimSpace(ack.AckID)
			if ack.AckID != "" {
				acks[ack.AckID] = &ack
			}
		})
		selector.AddReceive(doneCh, func(c workflow.ReceiveChannel, more bool) {
			var done laneRunCompleted
			c.Receive(ctx, &done)
			if state.Active != nil && strings.TrimSpace(done.LogicalRunKey) == state.Active.LogicalRunKey {
				if run := runFromPayload(done.RunPayload); run != nil && workflowRunTerminal(run.GetStatus()) {
					state.CompletedAtMap[state.Active.LogicalRunKey] = workflow.Now(ctx).UTC()
					state.Active = nil
				}
			}
		})
		selector.Select(ctx)
		drain()
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() && workflow.AllHandlersFinished(ctx) && !mutex.IsLocked() {
			return workflow.NewContinueAsNewError(ctx, gestaltWorkflowKeyLaneV1, state)
		}
	}
}

func gestaltRunWorkflowV3(ctx workflow.Context, input runWorkflowV3Input) (*proto.BoundWorkflowRun, error) {
	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UTC()
	if input.ScheduleID != "" {
		input.TriggerPayload = protoPayload(scheduleTrigger(input.ScheduleID, now))
	}
	publicID := encodeV3RunHandle(v3RunHandle{
		Kind:             runHandleKindV3,
		LaneWorkflowID:   input.LaneWorkflowID,
		RunWorkflowID:    info.WorkflowExecution.ID,
		RunTemporalRunID: info.WorkflowExecution.RunID,
		LogicalRunKey:    input.LogicalRunKey,
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	state := &proto.BoundWorkflowRun{
		Id:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       targetFromPayload(input.TargetPayload),
		Trigger:      triggerFromPayload(input.TriggerPayload),
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    actorFromPayload(input.CreatedByPayload),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	pendingSignals := make([]*proto.WorkflowSignal, 0)
	nextSignalSequence := int64(1)
	signalCount := 0
	signalCh := workflow.GetSignalChannel(ctx, signalRunAddV3)
	cancelCh := workflow.GetSignalChannel(ctx, signalRunCancelV3)
	runMutex := workflow.NewMutex(ctx)

	project := func(ctx workflow.Context) {
		_ = signalRunIndex(ctx, input.ScopeID, input.IndexShardCount, state)
	}
	ack := func(ctx workflow.Context, msg laneAckMessage) {
		if strings.TrimSpace(input.LaneWorkflowID) == "" || strings.TrimSpace(msg.AckID) == "" {
			return
		}
		_ = workflow.SignalExternalWorkflow(ctx, input.LaneWorkflowID, input.LaneTemporalRunID, signalLaneAck, msg).Get(ctx, nil)
	}
	sendDone := func(ctx workflow.Context) {
		if strings.TrimSpace(input.LaneWorkflowID) == "" || strings.TrimSpace(input.LogicalRunKey) == "" {
			return
		}
		_ = workflow.SignalExternalWorkflow(ctx, input.LaneWorkflowID, input.LaneTemporalRunID, signalLaneRunDone, laneRunCompleted{LogicalRunKey: input.LogicalRunKey, RunPayload: protoPayload(state)}).Get(ctx, nil)
	}
	appendSignal := func(signal *proto.WorkflowSignal) *proto.WorkflowSignal {
		signal = cloneSignal(signal)
		if signal.GetSequence() <= 0 {
			signal.Sequence = nextSignalSequence
		}
		if signal.GetSequence() >= nextSignalSequence {
			nextSignalSequence = signal.GetSequence() + 1
		}
		if strings.TrimSpace(signal.GetId()) == "" {
			signal.Id = "signal:" + hashID(state.GetId(), signal.GetName(), fmt.Sprintf("%d", signal.GetSequence()), signal.GetIdempotencyKey())
		}
		pendingSignals = append(pendingSignals, signal)
		signalCount++
		return signal
	}
	handleSignalMessage := func(ctx workflow.Context, msg runSignalMessage) {
		if err := runMutex.Lock(ctx); err != nil {
			ack(ctx, laneAckMessage{AckID: msg.AckID, Error: err.Error()})
			return
		}
		defer runMutex.Unlock()
		if workflowRunTerminal(state.GetStatus()) {
			ack(ctx, laneAckMessage{AckID: msg.AckID, Error: fmt.Sprintf("failed_precondition: workflow run %q is %s", state.GetId(), state.GetStatus().String())})
			return
		}
		signal := appendSignal(signalFromPayload(msg.SignalPayload))
		ack(ctx, laneAckMessage{AckID: msg.AckID, ResponsePayload: protoPayload(&proto.SignalWorkflowRunResponse{
			Run:         cloneRun(state),
			Signal:      cloneSignal(signal),
			StartedRun:  signalCount == 1 && state.GetStartedAt() == nil,
			WorkflowKey: strings.TrimSpace(state.GetWorkflowKey()),
		})})
	}
	handleCancelMessage := func(ctx workflow.Context, msg runCancelMessage) {
		if err := runMutex.Lock(ctx); err != nil {
			ack(ctx, laneAckMessage{AckID: msg.AckID, Error: err.Error()})
			return
		}
		defer runMutex.Unlock()
		if state.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			ack(ctx, laneAckMessage{AckID: msg.AckID, Error: fmt.Sprintf("failed_precondition: workflow run %q is %s; only pending runs can be canceled", state.GetId(), state.GetStatus().String())})
			return
		}
		completedAt := workflow.Now(ctx).UTC()
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
		state.CompletedAt = timestamppb.New(completedAt)
		state.StatusMessage = strings.TrimSpace(msg.Reason)
		if state.GetStatusMessage() == "" {
			state.StatusMessage = "canceled"
		}
		project(ctx)
		ack(ctx, laneAckMessage{AckID: msg.AckID, RunPayload: protoPayload(state)})
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var signalMsg runSignalMessage
			var cancelMsg runCancelMessage
			var gotSignal bool
			var gotCancel bool
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(signalCh, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, &signalMsg)
				gotSignal = true
			})
			selector.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, &cancelMsg)
				gotCancel = true
			})
			selector.Select(ctx)
			if gotSignal {
				handleSignalMessage(ctx, signalMsg)
			}
			if gotCancel {
				handleCancelMessage(ctx, cancelMsg)
			}
			if workflowRunTerminal(state.GetStatus()) {
				return
			}
		}
	})

	if err := workflow.SetQueryHandler(ctx, queryRunState, func() (*proto.BoundWorkflowRun, error) {
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if workflowRunTerminal(state.GetStatus()) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.GetId(), state.GetStatus().String())
		}
		signal = appendSignal(signal)
		return &proto.SignalWorkflowRunResponse{
			Run:         cloneRun(state),
			Signal:      cloneSignal(signal),
			StartedRun:  signalCount == 1 && state.GetStartedAt() == nil,
			WorkflowKey: strings.TrimSpace(state.GetWorkflowKey()),
		}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateCancelRun, func(ctx workflow.Context, reason string) (*proto.BoundWorkflowRun, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if state.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s; only pending runs can be canceled", state.GetId(), state.GetStatus().String())
		}
		completedAt := workflow.Now(ctx).UTC()
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
		state.CompletedAt = timestamppb.New(completedAt)
		state.StatusMessage = strings.TrimSpace(reason)
		if state.GetStatusMessage() == "" {
			state.StatusMessage = "canceled"
		}
		project(ctx)
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}

	project(ctx)
	if input.RequireSignal {
		_ = workflow.Await(ctx, func() bool {
			return len(pendingSignals) > 0 || workflowRunTerminal(state.GetStatus())
		})
	}
	for !workflowRunTerminal(state.GetStatus()) {
		if len(pendingSignals) == 0 && input.RequireSignal {
			_ = workflow.Await(ctx, func() bool {
				return len(pendingSignals) > 0 || workflowRunTerminal(state.GetStatus())
			})
			if workflowRunTerminal(state.GetStatus()) {
				break
			}
		}
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		startedAt := workflow.Now(ctx).UTC()
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
		state.StartedAt = timestamppb.New(startedAt)
		state.CompletedAt = nil
		state.StatusMessage = ""
		project(ctx)
		batch := pendingSignals
		pendingSignals = nil
		runMutex.Unlock()
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		invokeReq := &proto.InvokeWorkflowOperationRequest{
			Target:       cloneTarget(state.GetTarget()),
			RunId:        state.GetId(),
			Trigger:      cloneRunTrigger(state.GetTrigger()),
			Metadata:     workflowInvokeMetadata(state.GetWorkflowKey()),
			CreatedBy:    cloneActor(state.GetCreatedBy()),
			ExecutionRef: strings.TrimSpace(state.GetExecutionRef()),
			Signals:      cloneSignals(batch),
		}
		var resp proto.InvokeWorkflowOperationResponse
		invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeOperation, invokeReq).Get(activityCtx, &resp)
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		completedAt := workflow.Now(ctx).UTC()
		state.CompletedAt = timestamppb.New(completedAt)
		if invokeErr != nil {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			state.StatusMessage = invokeErr.Error()
		} else if resp.GetStatus() >= http.StatusBadRequest {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			state.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			state.ResultBody = resp.GetBody()
		} else {
			state.ResultBody = resp.GetBody()
			if len(pendingSignals) > 0 {
				state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				state.CompletedAt = nil
				project(ctx)
				runMutex.Unlock()
				continue
			}
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			state.StatusMessage = ""
		}
		runMutex.Unlock()
		break
	}
	project(ctx)
	sendDone(ctx)
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	return cloneRun(state), nil
}

func workflowInvokeMetadata(workflowKey string) *structpb.Struct {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil
	}
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			workflowInvokeMetadataWorkflowKey: structpb.NewStringValue(workflowKey),
		},
	}
}

func cloneLedgerEntry(entry *ownerLedgerEntry) *ownerLedgerEntry {
	if entry == nil {
		return nil
	}
	out := *entry
	out.ResponsePayload = append([]byte(nil), entry.ResponsePayload...)
	out.RunPayload = append([]byte(nil), entry.RunPayload...)
	return &out
}
