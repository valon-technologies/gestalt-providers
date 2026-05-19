package temporal

import (
	"context"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const workflowStepsPlanFormatVersion = "temporal-workflow-steps-v1"

func (b *temporalBackend) CompileWorkflowTarget(_ context.Context, req *gestalt.CompileWorkflowTargetRequest) (*gestalt.CompileWorkflowTargetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.NormalizedTarget)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(target.Target.Steps) == 0 {
		return &gestalt.CompileWorkflowTargetResponse{
			AcceptedTargetDigest: strings.TrimSpace(req.TargetDigest),
			Unsupported: []gestalt.WorkflowUnsupportedFeature{{
				Feature: "target",
				Reason:  "Temporal provider-owned plans support target.steps only",
			}},
		}, nil
	}
	targetDigest := strings.TrimSpace(req.TargetDigest)
	planDigest := hashID(
		workflowStepsPlanFormatVersion,
		strings.TrimSpace(req.WorkflowSemanticsVersion),
		strings.TrimSpace(req.TargetCanonicalizationVersion),
		targetDigest,
		valueHashID(target.Target),
	)
	return &gestalt.CompileWorkflowTargetResponse{
		AcceptedTargetDigest:      targetDigest,
		ProviderPlanID:            "temporal-step-plan:" + planDigest,
		ProviderPlanDigest:        planDigest,
		ProviderPlanFormatVersion: workflowStepsPlanFormatVersion,
	}, nil
}

func (b *temporalBackend) FinalizeWorkflowBinding(ctx context.Context, req *gestalt.FinalizeWorkflowBindingRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "workflow binding finalization request is required")
	}
	switch req.Decision {
	case gestalt.WorkflowBindingFinalizationDecisionActivate:
		return b.activateWorkflowBinding(ctx, req.PlanBinding)
	case gestalt.WorkflowBindingFinalizationDecisionAbort:
		return b.abortWorkflowBinding(ctx, req.PlanBinding, req.Reason)
	default:
		return status.Error(codes.InvalidArgument, "workflow binding finalization decision is required")
	}
}

func (b *temporalBackend) activateWorkflowBinding(ctx context.Context, binding *gestalt.WorkflowPlanBinding) error {
	binding = clonePlanBindingInput(binding)
	if binding == nil || strings.TrimSpace(binding.ID) == "" {
		return nil
	}
	prepared, found, err := b.state.getPlanBinding(ctx, binding.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "load workflow plan binding: %v", err)
	}
	if !found || prepared == nil {
		return status.Errorf(codes.NotFound, "workflow plan binding %q not found", binding.ID)
	}
	if !planBindingMatches(prepared.Binding, binding) {
		return status.Error(codes.InvalidArgument, "workflow plan binding does not match prepared binding")
	}
	if prepared.RunID != "" {
		if err := b.activatePreparedRun(ctx, prepared); err != nil {
			return err
		}
	}
	if prepared.Schedule != nil {
		prepared.Schedule.UpdatedAt = time.Now().UTC()
		if err := b.upsertTemporalScheduleWithBinding(ctx, prepared.Schedule, prepared.Binding); err != nil {
			return err
		}
		if err := b.state.putSchedule(ctx, prepared.Schedule); err != nil {
			_ = b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(prepared.Schedule.ID)).Delete(ctx)
			return status.Errorf(codes.Internal, "store activated workflow schedule: %v", err)
		}
	}
	if prepared.Trigger != nil {
		prepared.Trigger.UpdatedAt = time.Now().UTC()
		if err := b.state.putTrigger(ctx, prepared.Trigger); err != nil {
			return status.Errorf(codes.Internal, "store activated workflow event trigger: %v", err)
		}
		if err := b.state.putActiveTriggerBinding(ctx, prepared.Trigger.ID, prepared.Binding); err != nil {
			_, _ = b.state.deleteTrigger(ctx, prepared.Trigger.ID)
			return status.Errorf(codes.Internal, "store activated workflow trigger binding: %v", err)
		}
	}
	if err := b.state.deletePlanBinding(ctx, binding.ID); err != nil {
		return status.Errorf(codes.Internal, "delete workflow plan binding: %v", err)
	}
	return nil
}

func (b *temporalBackend) abortWorkflowBinding(ctx context.Context, binding *gestalt.WorkflowPlanBinding, reason string) error {
	if binding == nil || strings.TrimSpace(binding.ID) == "" {
		return nil
	}
	prepared, found, err := b.state.getPlanBinding(ctx, binding.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "load workflow plan binding: %v", err)
	}
	if !found || prepared == nil {
		return nil
	}
	if !planBindingMatches(prepared.Binding, binding) {
		return status.Error(codes.InvalidArgument, "workflow plan binding does not match prepared binding")
	}
	if prepared.RunID != "" {
		if err := b.abortPreparedRun(ctx, prepared, reason); err != nil {
			return err
		}
	}
	if err := b.state.deletePlanBinding(ctx, binding.ID); err != nil {
		return status.Errorf(codes.Internal, "delete workflow plan binding: %v", err)
	}
	return nil
}

func (b *temporalBackend) activatePreparedRun(ctx context.Context, prepared *workflowPlanBindingProjection) error {
	handle, err := decodeTemporalRunHandle(prepared.RunID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     "activate-binding:" + hashID(prepared.Binding.ID),
		UpdateName:   updateActivateBinding,
		Args:         []any{*prepared.Binding},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		if isNotFoundLike(err) {
			return nil
		}
		return mapTemporalWorkflowCallError("activate temporal workflow binding", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		return mapTemporalWorkflowCallError("temporal workflow activation update", err)
	}
	return nil
}

func (b *temporalBackend) abortPreparedRun(ctx context.Context, prepared *workflowPlanBindingProjection, reason string) error {
	handle, err := decodeTemporalRunHandle(prepared.RunID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "workflow plan binding aborted"
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     "abort-binding:" + hashID(prepared.Binding.ID, reason),
		UpdateName:   updateAbortBinding,
		Args:         []any{*prepared.Binding, reason},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		if isNotFoundLike(err) {
			return nil
		}
		return mapTemporalWorkflowCallError("abort temporal workflow binding", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		if strings.Contains(err.Error(), "failed_precondition") {
			return nil
		}
		return mapTemporalWorkflowCallError("temporal workflow abort update", err)
	}
	return nil
}

func clonePlanBindingInput(binding *gestalt.WorkflowPlanBinding) *gestalt.WorkflowPlanBinding {
	if binding == nil {
		return nil
	}
	out := *binding
	out.ID = strings.TrimSpace(out.ID)
	out.ExecutionRef = strings.TrimSpace(out.ExecutionRef)
	out.ExecutionRefSeal = strings.TrimSpace(out.ExecutionRefSeal)
	out.TargetDigest = strings.TrimSpace(out.TargetDigest)
	out.ProviderPlanID = strings.TrimSpace(out.ProviderPlanID)
	out.ProviderPlanDigest = strings.TrimSpace(out.ProviderPlanDigest)
	out.WorkflowSemanticsVersion = strings.TrimSpace(out.WorkflowSemanticsVersion)
	out.IdempotencyKey = strings.TrimSpace(out.IdempotencyKey)
	return &out
}

func isStepTargetInput(target *gestalt.BoundWorkflowTarget) bool {
	return target != nil && len(target.Steps) > 0
}

func planBindingTargetDigest(binding *gestalt.WorkflowPlanBinding) string {
	if binding == nil {
		return ""
	}
	return strings.TrimSpace(binding.TargetDigest)
}

func planBindingProviderPlanDigest(binding *gestalt.WorkflowPlanBinding) string {
	if binding == nil {
		return ""
	}
	return strings.TrimSpace(binding.ProviderPlanDigest)
}

func (b *temporalBackend) putPreparedRunBinding(ctx context.Context, binding *gestalt.WorkflowPlanBinding, run *gestalt.BoundWorkflowRun) error {
	binding = clonePlanBindingInput(binding)
	if binding == nil || strings.TrimSpace(binding.ID) == "" || run == nil || !isStepTargetInput(run.Target) {
		return nil
	}
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := b.state.putPlanBinding(ctx, &workflowPlanBindingProjection{
		Binding:            binding,
		RunID:              strings.TrimSpace(run.ID),
		TemporalWorkflowID: handle.RunWorkflowID,
		TemporalRunID:      handle.RunTemporalRunID,
	}); err != nil {
		return status.Errorf(codes.Internal, "store workflow plan binding: %v", err)
	}
	return nil
}

func (b *temporalBackend) cleanupPreparedRunAfterBindingStoreFailure(ctx context.Context, run *gestalt.BoundWorkflowRun, cause error) error {
	if run != nil {
		b.terminateUnclaimedRunV4(ctx, run)
		if strings.TrimSpace(run.WorkflowKey) != "" {
			if _, err := b.state.clearWorkflowKeyRun(ctx, run.WorkflowKey, run.ID); err != nil && cause != nil {
				return status.Errorf(codes.Internal, "%v; additionally failed to clear prepared workflow key claim: %v", cause, err)
			}
		}
	}
	return cause
}

func (b *temporalBackend) putPreparedScheduleBinding(ctx context.Context, binding *gestalt.WorkflowPlanBinding, schedule *gestalt.BoundWorkflowSchedule) error {
	binding = clonePlanBindingInput(binding)
	if binding == nil || strings.TrimSpace(binding.ID) == "" || schedule == nil || !isStepTargetInput(schedule.Target) {
		return nil
	}
	if err := b.state.putPlanBinding(ctx, &workflowPlanBindingProjection{
		Binding:  binding,
		Schedule: cloneScheduleInput(schedule),
	}); err != nil {
		return status.Errorf(codes.Internal, "store workflow schedule plan binding: %v", err)
	}
	return nil
}

func (b *temporalBackend) putPreparedTriggerBinding(ctx context.Context, binding *gestalt.WorkflowPlanBinding, trigger *gestalt.BoundWorkflowEventTrigger) error {
	binding = clonePlanBindingInput(binding)
	if binding == nil || strings.TrimSpace(binding.ID) == "" || trigger == nil || !isStepTargetInput(trigger.Target) {
		return nil
	}
	if err := b.state.putPlanBinding(ctx, &workflowPlanBindingProjection{
		Binding: binding,
		Trigger: cloneTriggerInput(trigger),
	}); err != nil {
		return status.Errorf(codes.Internal, "store workflow trigger plan binding: %v", err)
	}
	return nil
}

func cloneScheduleInput(schedule *gestalt.BoundWorkflowSchedule) *gestalt.BoundWorkflowSchedule {
	if schedule == nil {
		return nil
	}
	out := *schedule
	out.ID = strings.TrimSpace(out.ID)
	out.Cron = strings.TrimSpace(out.Cron)
	out.Timezone = strings.TrimSpace(out.Timezone)
	out.ExecutionRef = strings.TrimSpace(out.ExecutionRef)
	out.CreatedBy = cloneActorInput(out.CreatedBy)
	if out.NextRunAt != nil {
		next := out.NextRunAt.UTC()
		out.NextRunAt = &next
	}
	return &out
}

func cloneTriggerInput(trigger *gestalt.BoundWorkflowEventTrigger) *gestalt.BoundWorkflowEventTrigger {
	if trigger == nil {
		return nil
	}
	out := *trigger
	out.ID = strings.TrimSpace(out.ID)
	out.ExecutionRef = strings.TrimSpace(out.ExecutionRef)
	out.CreatedBy = cloneActorInput(out.CreatedBy)
	if trigger.Match != nil {
		match := *trigger.Match
		match.Type = strings.TrimSpace(match.Type)
		match.Source = strings.TrimSpace(match.Source)
		match.Subject = strings.TrimSpace(match.Subject)
		out.Match = &match
	}
	return &out
}
