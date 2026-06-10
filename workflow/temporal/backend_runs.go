package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *temporalBackend) startKeyedRun(ctx context.Context, start workflowRunStartSnapshot, workflowKey, key, fingerprint string) (*gestalt.WorkflowRun, error) {
	now := time.Now().UTC()
	key = strings.TrimSpace(key)
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	temporalWorkflowID := workflowKeyRunWorkflowID(b.cfg.ScopeID, workflowKey)
	if key != "" {
		entry, existing, err := b.state.reserveRunIdempotency(ctx, start.OwnerKey, key, fingerprint, defaultIdempotencyRetention, now)
		if err != nil {
			var conflict *runIdempotencyConflictError
			if errors.As(err, &conflict) {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
			if status.Code(err) == codes.Aborted {
				return nil, status.Errorf(codes.Aborted, "reserve workflow run idempotency: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "reserve workflow run idempotency: %v", err)
		}
		if existing && entry != nil && entry.Status == "completed" {
			if run := workflowRunFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	input := b.runInput(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, workflowKey, start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, false)
	input.RunAs = cloneSubjectInput(start.RunAs)
	run, err := b.executeRun(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	if err != nil {
		if isAlreadyStarted(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run", workflowKey)
		}
		return nil, err
	}
	if key != "" {
		if err := b.state.completeRunIdempotency(ctx, start.OwnerKey, key, fingerprint, run, defaultIdempotencyRetention, time.Now().UTC()); err != nil {
			var conflict *runIdempotencyConflictError
			if errors.As(err, &conflict) {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
			return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
		}
	}
	return run, nil
}

func (b *temporalBackend) startUnkeyedRun(ctx context.Context, start workflowRunStartSnapshot, key, fingerprint string) (*gestalt.WorkflowRun, error) {
	now := time.Now().UTC()
	entry, existing, err := b.state.reserveRunIdempotency(ctx, start.OwnerKey, key, fingerprint, defaultIdempotencyRetention, now)
	if err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Errorf(codes.Aborted, "reserve workflow run idempotency: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "reserve workflow run idempotency: %v", err)
	}
	if existing && entry != nil && entry.Status == "completed" {
		if run := workflowRunFromPayload(entry.RunPayload); run != nil {
			return run, nil
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "temporal-run-idempotent", start.OwnerKey, key)
	input := b.runInput(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, "", start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, false)
	input.RunAs = cloneSubjectInput(start.RunAs)
	run, err := b.executeRun(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	if err := b.state.completeRunIdempotency(ctx, start.OwnerKey, key, fingerprint, run, defaultIdempotencyRetention, time.Now().UTC()); err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
	}
	return run, nil
}

func (b *temporalBackend) signalOrStartRun(ctx context.Context, start workflowRunStartSnapshot, workflowKey string, signal *gestalt.WorkflowSignal, updateID string) (*gestalt.SignalWorkflowRunResponse, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	return b.startSignalWorkflowRun(ctx, start, workflowKey, signal, updateID)
}

func (b *temporalBackend) startSignalWorkflowRun(ctx context.Context, start workflowRunStartSnapshot, workflowKey string, signal *gestalt.WorkflowSignal, updateID string) (*gestalt.SignalWorkflowRunResponse, error) {
	temporalWorkflowID := workflowKeyRunWorkflowID(b.cfg.ScopeID, workflowKey)
	input := b.runInput(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, workflowKey, start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, true)
	input.RunAs = cloneSubjectInput(start.RunAs)
	startOperation := b.client.NewWithStartWorkflowOperation(
		b.startWorkflowOptionsWithVisibility(temporalWorkflowID, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, input),
		TemporalRun,
		input,
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOperation,
		UpdateOptions: client.UpdateWorkflowOptions{
			WorkflowID:   temporalWorkflowID,
			UpdateID:     updateID,
			UpdateName:   updateAddSignal,
			Args:         []any{*signal},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, mapTemporalWorkflowCallError("signal-or-start temporal workflow", err)
	}
	var out gestalt.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	startedRun, err := startOperation.Get(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve signal-or-start temporal workflow: %v", err)
	}
	run := out.Run
	if run == nil {
		run = b.pendingRunFromWorkflowRun(startedRun, input)
	}
	out.Run = cloneRunInput(run)
	if out.Signal == nil {
		out.Signal = signalInputForStartedRun(run, signal)
	}
	out.WorkflowKey = strings.TrimSpace(workflowKey)
	return cloneSignalResponseInput(&out), nil
}

func mapTemporalWorkflowCallError(prefix string, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case isNotFoundLike(err):
		return status.Error(codes.NotFound, msg)
	case strings.Contains(msg, "failed_precondition"):
		return status.Error(codes.FailedPrecondition, msg)
	case strings.Contains(msg, "invalid_argument"):
		return status.Error(codes.InvalidArgument, msg)
	default:
		return status.Errorf(codes.Internal, "%s: %v", prefix, err)
	}
}

func (b *temporalBackend) runInput(ownerKey, definitionID string, definitionGeneration int64, workflowKey string, target *gestalt.BoundWorkflowTarget, input map[string]any, trigger *gestalt.WorkflowRunTrigger, createdBySubjectID string, requireSignal bool) runWorkflowInput {
	return runWorkflowInput{
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		ScopeID:                       b.cfg.ScopeID,
		ProviderName:                  b.providerName,
		DefinitionID:                  strings.TrimSpace(definitionID),
		DefinitionGeneration:          definitionGeneration,
		WorkflowKey:                   strings.TrimSpace(workflowKey),
		OwnerKey:                      strings.TrimSpace(ownerKey),
		Target:                        cloneBoundWorkflowTargetInput(target),
		Input:                         cloneMapInput(input),
		Trigger:                       trigger,
		CreatedBySubjectID:            createdBySubjectID,
		RequireSignal:                 requireSignal,
	}
}

func (b *temporalBackend) executeRun(ctx context.Context, workflowID string, input runWorkflowInput, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*gestalt.WorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startWorkflowOptionsWithVisibility(workflowID, conflict, reuse, input), TemporalRun, input)
	if err != nil {
		if isAlreadyStarted(err) {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "start temporal workflow: %v", err)
	}
	return b.pendingRunFromWorkflowRun(run, input), nil
}

func workflowKeyRunWorkflowID(scopeID, workflowKey string) string {
	return workflowID(scopeID, "workflow-key", hashID(workflowKey))
}

func (b *temporalBackend) pendingRunFromWorkflowRun(run client.WorkflowRun, input runWorkflowInput) *gestalt.WorkflowRun {
	now := time.Now().UTC()
	publicID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    run.GetID(),
		RunTemporalRunID: run.GetRunID(),
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	out := &gestalt.WorkflowRun{
		ID:                   publicID,
		Status:               gestalt.WorkflowRunStatusValuePending,
		Target:               input.targetInput(),
		Trigger:              input.triggerInput(now),
		CreatedAt:            now,
		CreatedBySubjectID:   input.createdByInput(),
		WorkflowKey:          strings.TrimSpace(input.WorkflowKey),
		DefinitionID:         strings.TrimSpace(input.DefinitionID),
		DefinitionGeneration: input.DefinitionGeneration,
		ProviderName:         b.providerName,
		RunAs:                cloneSubjectInput(input.RunAs),
		Input:                cloneMapInput(input.Input),
	}
	return out
}

func (b *temporalBackend) startWorkflowOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.runStartOptions(workflowID, conflict, reuse)
	opts.WorkflowIDReusePolicy = reuse
	return opts
}

func (b *temporalBackend) startWorkflowOptionsWithVisibility(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy, input runWorkflowInput) client.StartWorkflowOptions {
	opts := b.startWorkflowOptions(workflowID, conflict, reuse)
	opts.TypedSearchAttributes = workflowRunSearchAttributesFromInput(input, gestalt.WorkflowRunStatusValuePending)
	if ownerKey := strings.TrimSpace(input.OwnerKey); ownerKey != "" {
		opts.Memo = map[string]any{memoKeyOwnerKey: ownerKey}
	}
	return opts
}
