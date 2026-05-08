package temporal

import (
	"context"
	"strings"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *temporalBackend) putScheduleIndex(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putSchedule(ctx, schedule)
}

func (b *temporalBackend) getScheduleIndex(ctx context.Context, id string) (*proto.BoundWorkflowSchedule, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getSchedule(ctx, id)
}

func (b *temporalBackend) listSchedulesIndex(ctx context.Context) ([]*proto.BoundWorkflowSchedule, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.listSchedules(ctx)
}

func (b *temporalBackend) deleteScheduleIndex(ctx context.Context, id string) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.deleteSchedule(ctx, id)
}

func (b *temporalBackend) putTriggerIndex(ctx context.Context, trigger *proto.BoundWorkflowEventTrigger) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putTrigger(ctx, trigger)
}

func (b *temporalBackend) getTriggerIndex(ctx context.Context, id string) (*proto.BoundWorkflowEventTrigger, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getTrigger(ctx, id)
}

func (b *temporalBackend) listTriggersIndex(ctx context.Context) ([]*proto.BoundWorkflowEventTrigger, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.listTriggers(ctx)
}

func (b *temporalBackend) matchTriggersIndex(ctx context.Context, ownerKey string, event *proto.WorkflowEvent) ([]*proto.BoundWorkflowEventTrigger, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.matchTriggers(ctx, ownerKey, event)
}

func (b *temporalBackend) deleteTriggerIndex(ctx context.Context, id string) (bool, error) {
	if b.state == nil {
		return false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.deleteTrigger(ctx, id)
}

func (b *temporalBackend) putExecutionRefIndex(ctx context.Context, ref *proto.WorkflowExecutionReference) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putExecutionRef(ctx, ref)
}

func (b *temporalBackend) getExecutionRefIndex(ctx context.Context, id string) (*proto.WorkflowExecutionReference, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getExecutionRef(ctx, id)
}

func (b *temporalBackend) listExecutionRefsIndex(ctx context.Context, subjectID string) ([]*proto.WorkflowExecutionReference, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.listExecutionRefs(ctx, subjectID)
}

func isNotFoundLike(err error) bool {
	if err == nil {
		return false
	}
	return isNotFound(err) || strings.Contains(strings.ToLower(err.Error()), "not found")
}
