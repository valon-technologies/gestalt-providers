package temporal

import (
	"context"
	"fmt"
	"strings"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.temporal.io/sdk/workflow"
	gproto "google.golang.org/protobuf/proto"
)

const (
	queryRunState = "gestalt.run_state"
	queryLaneRun  = "gestalt.workflow_key.active_run"

	updateAddSignal         = "gestalt.add_signal"
	updateCancelRun         = "gestalt.cancel_run"
	updateClaimRun          = "gestalt.claim_run"
	updateEnsureScope       = "gestalt.ensure_scope"
	updatePutRun            = "gestalt.index.put_run"
	updateGetRun            = "gestalt.index.get_run"
	updateListRuns          = "gestalt.index.list_runs"
	updatePruneRuns         = "gestalt.index.prune_runs"
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

	signalIndexPutRun  = "gestalt.index.signal_put_run"
	signalIndexCompact = "gestalt.index.signal_compact"
)

type scopeMetadata struct {
	ScopeID         string `json:"scope_id"`
	IndexShardCount int    `json:"index_shard_count"`
}

type indexInput struct {
	ScopeID  string         `json:"scope_id"`
	Shard    int            `json:"shard"`
	Snapshot *indexSnapshot `json:"snapshot,omitempty"`
}

type indexState struct {
	Runs        map[string]*proto.BoundWorkflowRun
	Schedules   map[string]*proto.BoundWorkflowSchedule
	Triggers    map[string]*proto.BoundWorkflowEventTrigger
	TriggerKeys map[string][]string
	Refs        map[string]*proto.WorkflowExecutionReference
	SubjectRefs map[string][]string
}

type indexSnapshot struct {
	Runs        map[string][]byte   `json:"runs,omitempty"`
	Schedules   map[string][]byte   `json:"schedules,omitempty"`
	Triggers    map[string][]byte   `json:"triggers,omitempty"`
	TriggerKeys map[string][]string `json:"trigger_keys,omitempty"`
	Refs        map[string][]byte   `json:"refs,omitempty"`
	SubjectRefs map[string][]string `json:"subject_refs,omitempty"`
}

func newIndexState() *indexState {
	return &indexState{
		Runs:        map[string]*proto.BoundWorkflowRun{},
		Schedules:   map[string]*proto.BoundWorkflowSchedule{},
		Triggers:    map[string]*proto.BoundWorkflowEventTrigger{},
		TriggerKeys: map[string][]string{},
		Refs:        map[string]*proto.WorkflowExecutionReference{},
		SubjectRefs: map[string][]string{},
	}
}

func indexStateFromInput(input indexInput) (*indexState, error) {
	if input.Snapshot == nil {
		return newIndexState(), nil
	}
	state := newIndexState()
	var err error
	if state.Runs, err = unmarshalRunMap(input.Snapshot.Runs); err != nil {
		return nil, err
	}
	if state.Schedules, err = unmarshalScheduleMap(input.Snapshot.Schedules); err != nil {
		return nil, err
	}
	if state.Triggers, err = unmarshalTriggerMap(input.Snapshot.Triggers); err != nil {
		return nil, err
	}
	if input.Snapshot.TriggerKeys != nil {
		state.TriggerKeys = input.Snapshot.TriggerKeys
	}
	if state.Refs, err = unmarshalExecutionReferenceMap(input.Snapshot.Refs); err != nil {
		return nil, err
	}
	if input.Snapshot.SubjectRefs != nil {
		state.SubjectRefs = input.Snapshot.SubjectRefs
	}
	return state, nil
}

func indexSnapshotFromState(state *indexState) (*indexSnapshot, error) {
	if state == nil {
		state = newIndexState()
	}
	runs, err := marshalProtoMap(state.Runs)
	if err != nil {
		return nil, err
	}
	schedules, err := marshalProtoMap(state.Schedules)
	if err != nil {
		return nil, err
	}
	triggers, err := marshalProtoMap(state.Triggers)
	if err != nil {
		return nil, err
	}
	refs, err := marshalProtoMap(state.Refs)
	if err != nil {
		return nil, err
	}
	return &indexSnapshot{
		Runs:        runs,
		Schedules:   schedules,
		Triggers:    triggers,
		TriggerKeys: cloneStringSliceMap(state.TriggerKeys),
		Refs:        refs,
		SubjectRefs: cloneStringSliceMap(state.SubjectRefs),
	}, nil
}

func ensureIndexStateMaps(state *indexState) *indexState {
	if state == nil {
		return newIndexState()
	}
	if state.Runs == nil {
		state.Runs = map[string]*proto.BoundWorkflowRun{}
	}
	if state.Schedules == nil {
		state.Schedules = map[string]*proto.BoundWorkflowSchedule{}
	}
	if state.Triggers == nil {
		state.Triggers = map[string]*proto.BoundWorkflowEventTrigger{}
	}
	if state.TriggerKeys == nil {
		state.TriggerKeys = map[string][]string{}
	}
	if state.Refs == nil {
		state.Refs = map[string]*proto.WorkflowExecutionReference{}
	}
	if state.SubjectRefs == nil {
		state.SubjectRefs = map[string][]string{}
	}
	return state
}

func marshalProtoMap[T gproto.Message](items map[string]T) (map[string][]byte, error) {
	out := make(map[string][]byte, len(items))
	for key, item := range items {
		msg := gproto.Message(item)
		if msg == nil {
			continue
		}
		data, err := gproto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return nil, err
		}
		out[key] = data
	}
	return out, nil
}

func unmarshalRunMap(items map[string][]byte) (map[string]*proto.BoundWorkflowRun, error) {
	out := make(map[string]*proto.BoundWorkflowRun, len(items))
	for key, data := range items {
		var value proto.BoundWorkflowRun
		if err := gproto.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		out[key] = &value
	}
	return out, nil
}

func unmarshalScheduleMap(items map[string][]byte) (map[string]*proto.BoundWorkflowSchedule, error) {
	out := make(map[string]*proto.BoundWorkflowSchedule, len(items))
	for key, data := range items {
		var value proto.BoundWorkflowSchedule
		if err := gproto.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		out[key] = &value
	}
	return out, nil
}

func unmarshalTriggerMap(items map[string][]byte) (map[string]*proto.BoundWorkflowEventTrigger, error) {
	out := make(map[string]*proto.BoundWorkflowEventTrigger, len(items))
	for key, data := range items {
		var value proto.BoundWorkflowEventTrigger
		if err := gproto.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		out[key] = &value
	}
	return out, nil
}

func unmarshalExecutionReferenceMap(items map[string][]byte) (map[string]*proto.WorkflowExecutionReference, error) {
	out := make(map[string]*proto.WorkflowExecutionReference, len(items))
	for key, data := range items {
		var value proto.WorkflowExecutionReference
		if err := gproto.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		out[key] = &value
	}
	return out, nil
}

func cloneStringSliceMap(items map[string][]string) map[string][]string {
	out := make(map[string][]string, len(items))
	for key, values := range items {
		out[key] = append([]string(nil), values...)
	}
	return out
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
	state, err := indexStateFromInput(input)
	if err != nil {
		return err
	}
	state = ensureIndexStateMaps(state)
	putRun := func(run *proto.BoundWorkflowRun) *proto.BoundWorkflowRun {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			return nil
		}
		if existing := state.Runs[run.GetId()]; existing != nil && workflowRunTerminal(existing.GetStatus()) && !workflowRunTerminal(run.GetStatus()) {
			return cloneRun(existing)
		}
		state.Runs[run.GetId()] = run
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
	_ = workflow.SetQueryHandler(ctx, updateGetRun, func(id string) (*proto.BoundWorkflowRun, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateListRuns, func() (*proto.ListWorkflowProviderRunsResponse, error) {
		out := make([]*proto.BoundWorkflowRun, 0, len(state.Runs))
		for _, run := range state.Runs {
			out = append(out, cloneRun(run))
		}
		return &proto.ListWorkflowProviderRunsResponse{Runs: out}, nil
	})
	_ = workflow.SetUpdateHandler(ctx, updatePruneRuns, func(ctx workflow.Context, shardCount int) (int, error) {
		if shardCount <= 0 {
			return 0, nil
		}
		removed := 0
		for id := range state.Runs {
			if shardFor(id, shardCount) != input.Shard {
				delete(state.Runs, id)
				removed++
			}
		}
		return removed, nil
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
	_ = workflow.SetQueryHandler(ctx, updateGetSchedule, func(id string) (*proto.BoundWorkflowSchedule, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateListSchedules, func() (*proto.ListWorkflowProviderSchedulesResponse, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateGetTrigger, func(id string) (*proto.BoundWorkflowEventTrigger, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateListTriggers, func() (*proto.ListWorkflowProviderEventTriggersResponse, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateMatchTriggers, func(key string) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateGetRef, func(id string) (*proto.WorkflowExecutionReference, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateListRefs, func() (*proto.ListWorkflowExecutionReferencesResponse, error) {
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
	_ = workflow.SetQueryHandler(ctx, updateListRefsBySubject, func(subjectID string) (*proto.ListWorkflowExecutionReferencesResponse, error) {
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
	compactCh := workflow.GetSignalChannel(ctx, signalIndexCompact)
	for {
		compact := false
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(putRunCh, func(c workflow.ReceiveChannel, more bool) {
			var run *proto.BoundWorkflowRun
			c.Receive(ctx, &run)
			if run != nil {
				putRun(run)
			}
		})
		selector.AddReceive(compactCh, func(c workflow.ReceiveChannel, more bool) {
			var reason string
			c.Receive(ctx, &reason)
			compact = true
		})
		selector.Select(ctx)
		if compact || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			snapshot, err := indexSnapshotFromState(state)
			if err != nil {
				return err
			}
			return workflow.NewContinueAsNewError(ctx, indexWorkflow, indexInput{ScopeID: input.ScopeID, Shard: input.Shard, Snapshot: snapshot})
		}
	}
}

type workflowActivities struct {
	host  workflowHost
	state *workflowStateStore
}

func (a *workflowActivities) InvokeOperation(ctx context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	return a.host.InvokeOperation(ctx, req)
}

func (a *workflowActivities) ProjectRun(ctx context.Context, run *proto.BoundWorkflowRun) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, run)
}

func signalRunIndex(ctx workflow.Context, scopeID string, indexShardCount int, run *proto.BoundWorkflowRun) error {
	shard := shardFor(run.GetId(), indexShardCount)
	return workflow.SignalExternalWorkflow(ctx, indexWorkflowID(scopeID, shard), "", signalIndexPutRun, cloneRun(run)).Get(ctx, nil)
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
