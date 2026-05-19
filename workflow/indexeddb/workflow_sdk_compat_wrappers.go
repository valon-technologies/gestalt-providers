package indexeddb

import proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"

func NewBoundWorkflowTarget(input BoundWorkflowTarget) (*proto.BoundWorkflowTarget, error) {
	return boundWorkflowTargetToProto(input)
}

func NewBoundWorkflowTargetFromTarget(value *proto.BoundWorkflowTarget) (*proto.BoundWorkflowTarget, error) {
	return cloneBoundWorkflowTargetProto(value)
}

func BoundWorkflowTargetFromTarget(value *proto.BoundWorkflowTarget) BoundWorkflowTarget {
	return boundWorkflowTargetFromProto(value)
}

func NewWorkflowActor(input WorkflowActor) *proto.WorkflowActor {
	return workflowActorToProto(input)
}

func WorkflowActorFromActor(value *proto.WorkflowActor) WorkflowActor {
	return workflowActorFromProto(value)
}

func NewWorkflowSignal(input WorkflowSignal) (*proto.WorkflowSignal, error) {
	return workflowSignalToProto(input)
}

func NewWorkflowSignalFromSignal(value *proto.WorkflowSignal) (*proto.WorkflowSignal, error) {
	return cloneWorkflowSignalProto(value)
}

func WorkflowSignalFromSignal(value *proto.WorkflowSignal) WorkflowSignal {
	return workflowSignalFromProto(value)
}

func NewWorkflowEvent(input WorkflowEvent) (*proto.WorkflowEvent, error) {
	return workflowEventToProto(input)
}

func NewWorkflowEventFromEvent(value *proto.WorkflowEvent) (*proto.WorkflowEvent, error) {
	return cloneWorkflowEventProto(value)
}

func WorkflowEventFromEvent(value *proto.WorkflowEvent) WorkflowEvent {
	return workflowEventFromProto(value)
}

func NewWorkflowOutputDelivery(input WorkflowOutputDelivery) (*proto.WorkflowOutputDelivery, error) {
	return workflowOutputDeliveryToProto(input)
}

func NewWorkflowExecutionReference(input WorkflowExecutionReference) (*proto.WorkflowExecutionReference, error) {
	return workflowExecutionReferenceToProto(input)
}

func NewWorkflowExecutionReferenceFromReference(value *proto.WorkflowExecutionReference) (*proto.WorkflowExecutionReference, error) {
	return cloneWorkflowExecutionReferenceProto(value)
}

func WorkflowExecutionReferenceFromReference(value *proto.WorkflowExecutionReference) (WorkflowExecutionReference, error) {
	return workflowExecutionReferenceFromProto(value)
}

func NewBoundWorkflowRun(input BoundWorkflowRun) (*proto.BoundWorkflowRun, error) {
	return boundWorkflowRunToProto(input)
}

func BoundWorkflowRunFromRun(value *proto.BoundWorkflowRun) (BoundWorkflowRun, error) {
	return boundWorkflowRunFromProto(value)
}

func NewBoundWorkflowSchedule(input BoundWorkflowSchedule) (*proto.BoundWorkflowSchedule, error) {
	return boundWorkflowScheduleToProto(input)
}

func BoundWorkflowScheduleFromSchedule(value *proto.BoundWorkflowSchedule) (BoundWorkflowSchedule, error) {
	return boundWorkflowScheduleFromProto(value)
}

func NewBoundWorkflowEventTrigger(input BoundWorkflowEventTrigger) (*proto.BoundWorkflowEventTrigger, error) {
	return boundWorkflowEventTriggerToProto(input)
}

func BoundWorkflowEventTriggerFromTrigger(value *proto.BoundWorkflowEventTrigger) (BoundWorkflowEventTrigger, error) {
	return boundWorkflowEventTriggerFromProto(value)
}
