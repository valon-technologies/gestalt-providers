package temporal

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	queryRunState = "gestalt.run_state"

	updateAddSignal         = "gestalt.add_signal"
	updateCancelRun         = "gestalt.cancel_run"
	updateEnsureScope       = "gestalt.ensure_scope"
	updatePutRun            = "gestalt.index.put_run"
	updateGetRun            = "gestalt.index.get_run"
	updateListRuns          = "gestalt.index.list_runs"
	updatePutWorkflowKey    = "gestalt.index.put_workflow_key"
	updateGetWorkflowKey    = "gestalt.index.get_workflow_key"
	updatePutIdempotency    = "gestalt.index.put_idempotency"
	updateGetIdempotency    = "gestalt.index.get_idempotency"
	updatePutSchedule       = "gestalt.index.put_schedule"
	updateGetSchedule       = "gestalt.index.get_schedule"
	updateListSchedules     = "gestalt.index.list_schedules"
	updateDeleteSchedule    = "gestalt.index.delete_schedule"
	updatePutTrigger        = "gestalt.index.put_trigger"
	updateGetTrigger        = "gestalt.index.get_trigger"
	updateListTriggers      = "gestalt.index.list_triggers"
	updateMatchTriggers     = "gestalt.index.match_triggers"
	updateDeleteTrigger     = "gestalt.index.delete_trigger"
	updatePutRef            = "gestalt.index.put_ref"
	updateGetRef            = "gestalt.index.get_ref"
	updateListRefs          = "gestalt.index.list_refs"
	updateListRefsBySubject = "gestalt.index.list_refs_by_subject"
	updateDeleteRef         = "gestalt.index.delete_ref"

	signalIndexPutRun = "gestalt.index.signal_put_run"
)

type scopeMetadata struct {
	ScopeID         string `json:"scope_id"`
	IndexShardCount int    `json:"index_shard_count"`
}

type indexInput struct {
	ScopeID string `json:"scope_id"`
	Shard   int    `json:"shard"`
}

type runWorkflowOptions struct {
	ProviderName                  string
	ScopeID                       string
	IndexShardCount               int
	ExecutionRef                  string
	WorkflowKey                   string
	ActivityStartToCloseTimeoutNS time.Duration
	RequireSignal                 bool
	ScheduleID                    string
}

type indexState struct {
	Runs         map[string]*proto.BoundWorkflowRun
	WorkflowKeys map[string]string
	Idempotency  map[string]*proto.SignalWorkflowRunResponse
	Schedules    map[string]*proto.BoundWorkflowSchedule
	Triggers     map[string]*proto.BoundWorkflowEventTrigger
	TriggerKeys  map[string][]string
	Refs         map[string]*proto.WorkflowExecutionReference
	SubjectRefs  map[string][]string
}

func scopeMetadataWorkflow(ctx workflow.Context, initial scopeMetadata) error {
	stored := initial
	if err := workflow.SetUpdateHandler(ctx, updateEnsureScope, func(ctx workflow.Context, desired scopeMetadata) error {
		desired.ScopeID = strings.TrimSpace(desired.ScopeID)
		if stored.ScopeID == "" {
			stored = desired
			return nil
		}
		if stored.ScopeID != desired.ScopeID || stored.IndexShardCount != desired.IndexShardCount {
			return fmt.Errorf("failed_precondition: temporal workflow scope %q was initialized with indexShardCount=%d", stored.ScopeID, stored.IndexShardCount)
		}
		return nil
	}); err != nil {
		return err
	}
	return workflow.Await(ctx, func() bool { return false })
}

func indexWorkflow(ctx workflow.Context, input indexInput) error {
	state := indexState{
		Runs:         map[string]*proto.BoundWorkflowRun{},
		WorkflowKeys: map[string]string{},
		Idempotency:  map[string]*proto.SignalWorkflowRunResponse{},
		Schedules:    map[string]*proto.BoundWorkflowSchedule{},
		Triggers:     map[string]*proto.BoundWorkflowEventTrigger{},
		TriggerKeys:  map[string][]string{},
		Refs:         map[string]*proto.WorkflowExecutionReference{},
		SubjectRefs:  map[string][]string{},
	}
	putRun := func(run *proto.BoundWorkflowRun) *proto.BoundWorkflowRun {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			return nil
		}
		if existing := state.Runs[run.GetId()]; existing != nil && workflowRunTerminal(existing.GetStatus()) && !workflowRunTerminal(run.GetStatus()) {
			return cloneRun(existing)
		}
		state.Runs[run.GetId()] = run
		if key := strings.TrimSpace(run.GetWorkflowKey()); key != "" && !workflowRunTerminal(run.GetStatus()) {
			state.WorkflowKeys[key] = run.GetId()
		} else if key := strings.TrimSpace(run.GetWorkflowKey()); key != "" && state.WorkflowKeys[key] == run.GetId() {
			delete(state.WorkflowKeys, key)
		}
		return cloneRun(run)
	}
	putTrigger := func(trigger *proto.BoundWorkflowEventTrigger) *proto.BoundWorkflowEventTrigger {
		trigger = cloneTrigger(trigger)
		if old := state.Triggers[trigger.GetId()]; old != nil {
			for _, key := range matchKeys(targetOwnerKey(old.GetTarget()), old.GetMatch()) {
				state.TriggerKeys[key] = removeString(state.TriggerKeys[key], old.GetId())
			}
		}
		state.Triggers[trigger.GetId()] = trigger
		for _, key := range matchKeys(targetOwnerKey(trigger.GetTarget()), trigger.GetMatch()) {
			state.TriggerKeys[key] = appendUnique(state.TriggerKeys[key], trigger.GetId())
		}
		return cloneTrigger(trigger)
	}
	putRef := func(ref *proto.WorkflowExecutionReference) *proto.WorkflowExecutionReference {
		ref = cloneExecutionReference(ref)
		if old := state.Refs[ref.GetId()]; old != nil {
			state.SubjectRefs[old.GetSubjectId()] = removeString(state.SubjectRefs[old.GetSubjectId()], old.GetId())
		}
		state.Refs[ref.GetId()] = ref
		state.SubjectRefs[ref.GetSubjectId()] = appendUnique(state.SubjectRefs[ref.GetSubjectId()], ref.GetId())
		return cloneExecutionReference(ref)
	}

	_ = workflow.SetUpdateHandler(ctx, updatePutRun, func(ctx workflow.Context, run *proto.BoundWorkflowRun) (*proto.BoundWorkflowRun, error) {
		return putRun(run), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetRun, func(ctx workflow.Context, id string) (*proto.BoundWorkflowRun, error) {
		run := state.Runs[strings.TrimSpace(id)]
		if run == nil {
			return &proto.BoundWorkflowRun{}, nil
		}
		return cloneRun(run), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateListRuns, func(ctx workflow.Context) (*proto.ListWorkflowProviderRunsResponse, error) {
		out := make([]*proto.BoundWorkflowRun, 0, len(state.Runs))
		for _, run := range state.Runs {
			out = append(out, cloneRun(run))
		}
		return &proto.ListWorkflowProviderRunsResponse{Runs: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePutWorkflowKey, func(ctx workflow.Context, key string, run *proto.BoundWorkflowRun) (*proto.BoundWorkflowRun, error) {
		key = strings.TrimSpace(key)
		if key != "" && run != nil {
			run = cloneRun(run)
			run.WorkflowKey = key
		}
		return putRun(run), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetWorkflowKey, func(ctx workflow.Context, key string) (*proto.BoundWorkflowRun, error) {
		id := state.WorkflowKeys[strings.TrimSpace(key)]
		run := state.Runs[id]
		if run == nil {
			return &proto.BoundWorkflowRun{}, nil
		}
		return cloneRun(run), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePutIdempotency, func(ctx workflow.Context, ownerKey, key string, record *proto.SignalWorkflowRunResponse) (*proto.SignalWorkflowRunResponse, error) {
		id := ownerKey + "\x00" + key
		state.Idempotency[id] = cloneSignalResponse(record)
		return cloneSignalResponse(record), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetIdempotency, func(ctx workflow.Context, ownerKey, key string) (*proto.SignalWorkflowRunResponse, error) {
		record := state.Idempotency[ownerKey+"\x00"+key]
		if record == nil {
			return &proto.SignalWorkflowRunResponse{}, nil
		}
		return cloneSignalResponse(record), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePutSchedule, func(ctx workflow.Context, schedule *proto.BoundWorkflowSchedule) (*proto.BoundWorkflowSchedule, error) {
		schedule = cloneSchedule(schedule)
		state.Schedules[schedule.GetId()] = schedule
		return cloneSchedule(schedule), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetSchedule, func(ctx workflow.Context, id string) (*proto.BoundWorkflowSchedule, error) {
		schedule := state.Schedules[strings.TrimSpace(id)]
		if schedule == nil {
			return &proto.BoundWorkflowSchedule{}, nil
		}
		return cloneSchedule(schedule), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateListSchedules, func(ctx workflow.Context) (*proto.ListWorkflowProviderSchedulesResponse, error) {
		out := make([]*proto.BoundWorkflowSchedule, 0, len(state.Schedules))
		for _, schedule := range state.Schedules {
			out = append(out, cloneSchedule(schedule))
		}
		return &proto.ListWorkflowProviderSchedulesResponse{Schedules: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateDeleteSchedule, func(ctx workflow.Context, id string) (bool, error) {
		delete(state.Schedules, strings.TrimSpace(id))
		return true, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePutTrigger, func(ctx workflow.Context, trigger *proto.BoundWorkflowEventTrigger) (*proto.BoundWorkflowEventTrigger, error) {
		return putTrigger(trigger), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetTrigger, func(ctx workflow.Context, id string) (*proto.BoundWorkflowEventTrigger, error) {
		trigger := state.Triggers[strings.TrimSpace(id)]
		if trigger == nil {
			return &proto.BoundWorkflowEventTrigger{}, nil
		}
		return cloneTrigger(trigger), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateListTriggers, func(ctx workflow.Context) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
		out := make([]*proto.BoundWorkflowEventTrigger, 0, len(state.Triggers))
		for _, trigger := range state.Triggers {
			out = append(out, cloneTrigger(trigger))
		}
		return &proto.ListWorkflowProviderEventTriggersResponse{Triggers: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateMatchTriggers, func(ctx workflow.Context, key string) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
		var out []*proto.BoundWorkflowEventTrigger
		for _, id := range state.TriggerKeys[strings.TrimSpace(key)] {
			if trigger := state.Triggers[id]; trigger != nil {
				out = append(out, cloneTrigger(trigger))
			}
		}
		return &proto.ListWorkflowProviderEventTriggersResponse{Triggers: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateDeleteTrigger, func(ctx workflow.Context, id string) (bool, error) {
		id = strings.TrimSpace(id)
		if old := state.Triggers[id]; old != nil {
			for _, key := range matchKeys(targetOwnerKey(old.GetTarget()), old.GetMatch()) {
				state.TriggerKeys[key] = removeString(state.TriggerKeys[key], id)
			}
		}
		delete(state.Triggers, id)
		return true, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePutRef, func(ctx workflow.Context, ref *proto.WorkflowExecutionReference) (*proto.WorkflowExecutionReference, error) {
		return putRef(ref), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateGetRef, func(ctx workflow.Context, id string) (*proto.WorkflowExecutionReference, error) {
		ref := state.Refs[strings.TrimSpace(id)]
		if ref == nil {
			return &proto.WorkflowExecutionReference{}, nil
		}
		return cloneExecutionReference(ref), nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateListRefs, func(ctx workflow.Context) (*proto.ListWorkflowExecutionReferencesResponse, error) {
		out := make([]*proto.WorkflowExecutionReference, 0, len(state.Refs))
		for _, ref := range state.Refs {
			out = append(out, cloneExecutionReference(ref))
		}
		return &proto.ListWorkflowExecutionReferencesResponse{References: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateListRefsBySubject, func(ctx workflow.Context, subjectID string) (*proto.ListWorkflowExecutionReferencesResponse, error) {
		var out []*proto.WorkflowExecutionReference
		for _, id := range state.SubjectRefs[strings.TrimSpace(subjectID)] {
			if ref := state.Refs[id]; ref != nil {
				out = append(out, cloneExecutionReference(ref))
			}
		}
		return &proto.ListWorkflowExecutionReferencesResponse{References: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updateDeleteRef, func(ctx workflow.Context, id string) (bool, error) {
		id = strings.TrimSpace(id)
		if old := state.Refs[id]; old != nil {
			state.SubjectRefs[old.GetSubjectId()] = removeString(state.SubjectRefs[old.GetSubjectId()], id)
		}
		delete(state.Refs, id)
		return true, nil
	})

	putRunCh := workflow.GetSignalChannel(ctx, signalIndexPutRun)
	for {
		var run *proto.BoundWorkflowRun
		putRunCh.Receive(ctx, &run)
		if run != nil {
			putRun(run)
		}
	}
}

func gestaltRunWorkflow(ctx workflow.Context, input runWorkflowOptions, target *proto.BoundWorkflowTarget, trigger *proto.WorkflowRunTrigger, createdBy *proto.WorkflowActor) (*proto.BoundWorkflowRun, error) {
	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UTC()
	if input.ScheduleID != "" {
		trigger = scheduleTrigger(input.ScheduleID, now)
	}
	publicID := publicRunID(info.WorkflowExecution.ID, info.WorkflowExecution.RunID)
	state := &proto.BoundWorkflowRun{
		Id:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       cloneTarget(target),
		Trigger:      cloneRunTrigger(trigger),
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    cloneActor(createdBy),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	pendingSignals := make([]*proto.WorkflowSignal, 0)
	nextSignalSequence := int64(1)
	signalCount := 0

	if err := workflow.SetQueryHandler(ctx, queryRunState, func() (*proto.BoundWorkflowRun, error) {
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, error) {
		if workflowRunTerminal(state.GetStatus()) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.GetId(), state.GetStatus().String())
		}
		signal = cloneSignal(signal)
		if strings.TrimSpace(signal.GetId()) == "" {
			signal.Id = "signal:" + hashID(state.GetId(), signal.GetName(), fmt.Sprintf("%d", nextSignalSequence), signal.GetIdempotencyKey())
		}
		if signal.GetSequence() <= 0 {
			signal.Sequence = nextSignalSequence
		}
		if signal.GetSequence() >= nextSignalSequence {
			nextSignalSequence = signal.GetSequence() + 1
		}
		pendingSignals = append(pendingSignals, signal)
		signalCount++
		resp := &proto.SignalWorkflowRunResponse{
			Run:         cloneRun(state),
			Signal:      cloneSignal(signal),
			StartedRun:  signalCount == 1 && state.GetStartedAt() == nil,
			WorkflowKey: strings.TrimSpace(state.GetWorkflowKey()),
		}
		return resp, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateCancelRun, func(ctx workflow.Context, reason string) (*proto.BoundWorkflowRun, error) {
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
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}

	if err := signalRunIndex(ctx, input, state); err != nil {
		return nil, err
	}
	if input.RequireSignal {
		_ = workflow.Await(ctx, func() bool { return len(pendingSignals) > 0 || workflowRunTerminal(state.GetStatus()) })
	}
	for !workflowRunTerminal(state.GetStatus()) {
		startedAt := workflow.Now(ctx).UTC()
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
		state.StartedAt = timestamppb.New(startedAt)
		state.CompletedAt = nil
		state.StatusMessage = ""
		if err := signalRunIndex(ctx, input, state); err != nil {
			return nil, err
		}
		batch := pendingSignals
		pendingSignals = nil
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		invokeReq := &proto.InvokeWorkflowOperationRequest{
			Target:       cloneTarget(state.GetTarget()),
			RunId:        state.GetId(),
			Trigger:      cloneRunTrigger(state.GetTrigger()),
			CreatedBy:    cloneActor(state.GetCreatedBy()),
			ExecutionRef: strings.TrimSpace(state.GetExecutionRef()),
			Signals:      cloneSignals(batch),
		}
		var resp proto.InvokeWorkflowOperationResponse
		invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeOperation, invokeReq).Get(activityCtx, &resp)
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
			if state.GetWorkflowKey() != "" && len(pendingSignals) > 0 {
				state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				state.CompletedAt = nil
				_ = signalRunIndex(ctx, input, state)
				continue
			}
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			state.StatusMessage = ""
		}
		break
	}
	if err := signalRunIndex(ctx, input, state); err != nil {
		return nil, err
	}
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	return cloneRun(state), nil
}

type workflowActivities struct {
	host workflowHost
}

func (a *workflowActivities) InvokeOperation(ctx context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	return a.host.InvokeOperation(ctx, req)
}

func signalRunIndex(ctx workflow.Context, input runWorkflowOptions, run *proto.BoundWorkflowRun) error {
	keys := []string{run.GetId()}
	if workflowKey := strings.TrimSpace(run.GetWorkflowKey()); workflowKey != "" {
		keys = append(keys, "workflow-key:"+workflowKey)
	}
	seen := map[int]struct{}{}
	for _, key := range keys {
		shard := shardFor(key, input.IndexShardCount)
		if _, ok := seen[shard]; ok {
			continue
		}
		seen[shard] = struct{}{}
		if err := workflow.SignalExternalWorkflow(ctx, indexWorkflowID(input.ScopeID, shard), "", signalIndexPutRun, cloneRun(run)).Get(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

func appendUnique(values []string, value string) []string {
	value = strings.TrimSpace(value)
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func removeString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	out := values[:0]
	for _, existing := range values {
		if existing != value {
			out = append(out, existing)
		}
	}
	return out
}

func cloneRunTrigger(trigger *proto.WorkflowRunTrigger) *proto.WorkflowRunTrigger {
	if trigger == nil {
		return nil
	}
	return gproto.Clone(trigger).(*proto.WorkflowRunTrigger)
}

func cloneSignals(signals []*proto.WorkflowSignal) []*proto.WorkflowSignal {
	if len(signals) == 0 {
		return nil
	}
	out := make([]*proto.WorkflowSignal, 0, len(signals))
	for _, signal := range signals {
		out = append(out, cloneSignal(signal))
	}
	return out
}
