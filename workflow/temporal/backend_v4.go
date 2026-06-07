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

func (b *temporalBackend) startKeyedRunV4(ctx context.Context, start workflowRunStartSnapshot, workflowKey, key, fingerprint string) (*gestalt.WorkflowRun, error) {
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
			if run := runProjectionFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	input := b.runV4Input(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, workflowKey, start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, false)
	input.RunAs = cloneSubjectInput(start.RunAs)
	run, err := b.executeRunV5(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
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

func (b *temporalBackend) startUnkeyedRunV4(ctx context.Context, start workflowRunStartSnapshot, key, fingerprint string) (*gestalt.WorkflowRun, error) {
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
		if run := runProjectionFromPayload(entry.RunPayload); run != nil {
			return run, nil
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "manual-v5", start.OwnerKey, key)
	input := b.runV4Input(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, "", start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, false)
	input.RunAs = cloneSubjectInput(start.RunAs)
	run, err := b.executeRunV5(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
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

func (b *temporalBackend) signalOrStartRunV4(ctx context.Context, start workflowRunStartSnapshot, workflowKey string, signal *gestalt.WorkflowSignal, updateID string) (*gestalt.SignalWorkflowRunResponse, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	return b.startSignalWorkflowRunV4(ctx, start, workflowKey, signal, updateID)
}

func (b *temporalBackend) startSignalWorkflowRunV4(ctx context.Context, start workflowRunStartSnapshot, workflowKey string, signal *gestalt.WorkflowSignal, updateID string) (*gestalt.SignalWorkflowRunResponse, error) {
	temporalWorkflowID := workflowKeyRunWorkflowID(b.cfg.ScopeID, workflowKey)
	input := b.runV4Input(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, workflowKey, start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, true)
	input.RunAs = cloneSubjectInput(start.RunAs)
	startOperation := b.client.NewWithStartWorkflowOperation(
		b.startWorkflowOptionsV5(temporalWorkflowID, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, input),
		gestaltRunWorkflowV5,
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

func (b *temporalBackend) runV4Input(ownerKey, definitionID string, definitionGeneration int64, workflowKey string, target *gestalt.BoundWorkflowTarget, input map[string]any, trigger *gestalt.WorkflowRunTrigger, createdBySubjectID string, requireSignal bool) runWorkflowV4Input {
	return runWorkflowV4Input{
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

func (b *temporalBackend) executeRunV5(ctx context.Context, workflowID string, input runWorkflowV4Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*gestalt.WorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startWorkflowOptionsV5(workflowID, conflict, reuse, input), gestaltRunWorkflowV5, input)
	if err != nil {
		if isAlreadyStarted(err) {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "start temporal v5 workflow: %v", err)
	}
	return b.pendingRunFromWorkflowRun(run, input), nil
}

func workflowKeyRunWorkflowID(scopeID, workflowKey string) string {
	return workflowID(scopeID, "workflow-key-v5", hashID(workflowKey))
}

func (b *temporalBackend) pendingRunFromWorkflowRun(run client.WorkflowRun, input runWorkflowV4Input) *gestalt.WorkflowRun {
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

func (b *temporalBackend) startWorkflowOptionsV5(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy, input runWorkflowV4Input) client.StartWorkflowOptions {
	opts := b.startWorkflowOptions(workflowID, conflict, reuse)
	opts.TypedSearchAttributes = workflowRunSearchAttributesFromInput(input, gestalt.WorkflowRunStatusValuePending)
	return opts
}
