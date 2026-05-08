package temporal

import (
	"context"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (b *temporalBackend) runV4Input(ownerKey, executionRef, workflowKey string, target *proto.BoundWorkflowTarget, trigger *proto.WorkflowRunTrigger, createdBy *proto.WorkflowActor, initialSignal *proto.WorkflowSignal, requireSignal bool) runWorkflowV4Input {
	return runWorkflowV4Input{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		TaskQueue:                     b.cfg.TaskQueue,
		WorkflowRunTimeoutNS:          b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeoutNS:         b.cfg.WorkflowTaskTimeout,
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		ExecutionRef:                  strings.TrimSpace(executionRef),
		WorkflowKey:                   strings.TrimSpace(workflowKey),
		OwnerKey:                      strings.TrimSpace(ownerKey),
		TargetPayload:                 protoPayload(target),
		TriggerPayload:                protoPayload(trigger),
		CreatedByPayload:              protoPayload(createdBy),
		InitialSignalPayload:          protoPayload(initialSignal),
		RequireSignal:                 requireSignal,
	}
}

func (b *temporalBackend) executeRunV4(ctx context.Context, workflowID string, input runWorkflowV4Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*proto.BoundWorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startV3WorkflowOptions(workflowID, conflict, reuse), gestaltRunWorkflowV4, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal v4 workflow: %v", err)
	}
	now := time.Now().UTC()
	if input.ScheduleID != "" {
		input.TriggerPayload = protoPayload(scheduleTrigger(input.ScheduleID, now))
	}
	publicID := encodeV3RunHandle(v3RunHandle{
		Kind:             runHandleKindV3,
		RunWorkflowID:    run.GetID(),
		RunTemporalRunID: run.GetRunID(),
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	out := &proto.BoundWorkflowRun{
		Id:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       targetFromPayload(input.TargetPayload),
		Trigger:      triggerFromPayload(input.TriggerPayload),
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    actorFromPayload(input.CreatedByPayload),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	if b.state != nil {
		_ = b.state.putRun(ctx, out)
	}
	return out, nil
}

func (b *temporalBackend) getRunProjection(ctx context.Context, runID string) (*proto.BoundWorkflowRun, bool, error) {
	if b.state == nil {
		return nil, false, nil
	}
	return b.state.getRun(ctx, runID)
}

func (b *temporalBackend) listRunProjections(ctx context.Context) ([]*proto.BoundWorkflowRun, error) {
	if b.state == nil {
		return nil, nil
	}
	return b.state.listRuns(ctx)
}

func mergeRuns(primary, legacy []*proto.BoundWorkflowRun) []*proto.BoundWorkflowRun {
	if len(primary) == 0 {
		return cloneRuns(legacy)
	}
	out := make([]*proto.BoundWorkflowRun, 0, len(primary)+len(legacy))
	seen := make(map[string]struct{}, len(primary)+len(legacy))
	for _, run := range primary {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			continue
		}
		seen[run.GetId()] = struct{}{}
		out = append(out, run)
	}
	for _, run := range legacy {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			continue
		}
		if _, ok := seen[run.GetId()]; ok {
			continue
		}
		out = append(out, run)
	}
	sortRuns(out)
	return out
}

func cloneRuns(runs []*proto.BoundWorkflowRun) []*proto.BoundWorkflowRun {
	out := make([]*proto.BoundWorkflowRun, 0, len(runs))
	for _, run := range runs {
		if cloned := cloneRun(run); cloned != nil {
			out = append(out, cloned)
		}
	}
	sortRuns(out)
	return out
}
