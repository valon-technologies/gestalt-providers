package temporal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion            = "0.0.1-alpha.1"
	defaultHostPort            = "127.0.0.1:7233"
	defaultNamespace           = "default"
	defaultTimezone            = "UTC"
	defaultSpecVersion         = "1.0"
	defaultActivityTimeout     = 2 * time.Minute
	defaultRunTimeout          = 15 * time.Minute
	defaultListPageSize        = 200
	scheduleContinueAsNewEvery = 500
	memoRecordKey              = "gestalt.record"

	runWorkflowName          = "gestalt.workflow.temporal.run"
	scheduleWorkflowName     = "gestalt.workflow.temporal.schedule"
	eventTriggerWorkflowName = "gestalt.workflow.temporal.event_trigger"
	invokeActivityName       = "gestalt.workflow.temporal.invoke_operation"

	runCancelSignalName         = "set_run_cancel_reason"
	scheduleApplySignalName     = "apply_schedule"
	eventTriggerApplySignalName = "apply_event_trigger"
	definitionDeleteSignalName  = "delete_definition"

	triggerKindManual   = "manual"
	triggerKindSchedule = "schedule"
	triggerKindEvent    = "event"
)

type config struct {
	HostPort        string        `yaml:"hostPort"`
	Namespace       string        `yaml:"namespace"`
	TaskQueue       string        `yaml:"taskQueue"`
	ActivityTimeout time.Duration `yaml:"activityTimeout"`
	RunTimeout      time.Duration `yaml:"runTimeout"`
}

type Provider struct {
	proto.UnimplementedWorkflowProviderServer

	mu sync.RWMutex

	name            string
	executionPrefix string
	cfg             config
	client          client.Client
	worker          worker.Worker
	eventTriggers   map[string]eventTriggerSnapshot
	deletedTriggers map[string]eventTriggerDeleteMarker
}

type scopedTarget struct {
	PluginName string
	Operation  string
	Input      map[string]any
}

type actorSnapshot struct {
	SubjectID   string `json:"subject_id,omitempty"`
	SubjectKind string `json:"subject_kind,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	AuthSource  string `json:"auth_source,omitempty"`
}

type targetSnapshot struct {
	PluginName string         `json:"plugin_name"`
	Operation  string         `json:"operation"`
	Input      map[string]any `json:"input,omitempty"`
}

type eventSnapshot struct {
	ID              string         `json:"id,omitempty"`
	Source          string         `json:"source,omitempty"`
	SpecVersion     string         `json:"spec_version,omitempty"`
	Type            string         `json:"type,omitempty"`
	Subject         string         `json:"subject,omitempty"`
	Time            *time.Time     `json:"time,omitempty"`
	Datacontenttype string         `json:"datacontenttype,omitempty"`
	Data            map[string]any `json:"data,omitempty"`
	Extensions      map[string]any `json:"extensions,omitempty"`
}

type runTriggerSnapshot struct {
	Kind           string         `json:"kind"`
	ScheduleID     string         `json:"schedule_id,omitempty"`
	ScheduledFor   *time.Time     `json:"scheduled_for,omitempty"`
	EventTriggerID string         `json:"event_trigger_id,omitempty"`
	Event          *eventSnapshot `json:"event,omitempty"`
}

type runSnapshot struct {
	ID            string                  `json:"id"`
	PluginName    string                  `json:"plugin_name"`
	Status        proto.WorkflowRunStatus `json:"status"`
	Target        targetSnapshot          `json:"target"`
	Trigger       runTriggerSnapshot      `json:"trigger"`
	CreatedAt     time.Time               `json:"created_at"`
	StartedAt     *time.Time              `json:"started_at,omitempty"`
	CompletedAt   *time.Time              `json:"completed_at,omitempty"`
	StatusMessage string                  `json:"status_message,omitempty"`
	ResultBody    string                  `json:"result_body,omitempty"`
	CreatedBy     *actorSnapshot          `json:"created_by,omitempty"`
}

type scheduleSnapshot struct {
	ID         string         `json:"id"`
	PluginName string         `json:"plugin_name"`
	Cron       string         `json:"cron"`
	Timezone   string         `json:"timezone"`
	Target     targetSnapshot `json:"target"`
	Paused     bool           `json:"paused"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	NextRunAt  *time.Time     `json:"next_run_at,omitempty"`
	CreatedBy  *actorSnapshot `json:"created_by,omitempty"`
}

type eventTriggerSnapshot struct {
	ID           string         `json:"id"`
	PluginName   string         `json:"plugin_name"`
	MatchType    string         `json:"match_type"`
	MatchSource  string         `json:"match_source,omitempty"`
	MatchSubject string         `json:"match_subject,omitempty"`
	Target       targetSnapshot `json:"target"`
	Paused       bool           `json:"paused"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	CreatedBy    *actorSnapshot `json:"created_by,omitempty"`
	InstanceID   string         `json:"instance_id,omitempty"`
	Revision     int64          `json:"revision,omitempty"`
}

type runWorkflowConfig struct {
	ProviderName    string        `json:"provider_name"`
	Namespace       string        `json:"namespace"`
	TaskQueue       string        `json:"task_queue"`
	ExecutionPrefix string        `json:"execution_prefix"`
	ActivityTimeout time.Duration `json:"activity_timeout"`
	RunTimeout      time.Duration `json:"run_timeout"`
}

type runWorkflowInput struct {
	Run    runSnapshot       `json:"run"`
	Config runWorkflowConfig `json:"config"`
}

type scheduleWorkflowInput struct {
	Record scheduleSnapshot  `json:"record"`
	Config runWorkflowConfig `json:"config"`
	Steps  int               `json:"steps"`
}

type scheduleApplySignal struct {
	Record scheduleSnapshot  `json:"record"`
	Config runWorkflowConfig `json:"config"`
}

type eventTriggerWorkflowInput struct {
	Record eventTriggerSnapshot `json:"record"`
}

type eventTriggerApplySignal struct {
	Record eventTriggerSnapshot `json:"record"`
}

type invokeActivityInput struct {
	Run        runSnapshot    `json:"run"`
	Provider   string         `json:"provider"`
	Namespace  string         `json:"namespace"`
	WorkflowID string         `json:"workflow_id"`
	RunID      string         `json:"run_id"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type invokeActivityResult struct {
	Status int32  `json:"status"`
	Body   string `json:"body"`
}

type runCancelSignal struct {
	Reason string `json:"reason,omitempty"`
}

type eventTriggerDeleteMarker struct {
	InstanceID string `json:"instance_id,omitempty"`
	Revision   int64  `json:"revision,omitempty"`
}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	if err := p.Close(); err != nil {
		return err
	}

	cfg, err := decodeConfig(name, raw)
	if err != nil {
		return fmt.Errorf("temporal workflow: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	temporalClient, err := client.DialContext(dialCtx, client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return fmt.Errorf("temporal workflow: connect temporal: %w", err)
	}

	w := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(runExecutionWorkflow, workflow.RegisterOptions{Name: runWorkflowName})
	w.RegisterWorkflowWithOptions(scheduleDefinitionWorkflow, workflow.RegisterOptions{Name: scheduleWorkflowName})
	w.RegisterWorkflowWithOptions(eventTriggerDefinitionWorkflow, workflow.RegisterOptions{Name: eventTriggerWorkflowName})
	w.RegisterActivityWithOptions(invokeWorkflowOperationActivity, activity.RegisterOptions{Name: invokeActivityName})
	if err := w.Start(); err != nil {
		temporalClient.Close()
		return fmt.Errorf("temporal workflow: start worker: %w", err)
	}

	p.mu.Lock()
	p.name = strings.TrimSpace(name)
	p.executionPrefix = executionPrefixForName(p.name)
	p.cfg = cfg
	p.client = temporalClient
	p.worker = w
	p.eventTriggers = map[string]eventTriggerSnapshot{}
	p.deletedTriggers = map[string]eventTriggerDeleteMarker{}
	p.mu.Unlock()
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	p.mu.RLock()
	name := p.name
	p.mu.RUnlock()
	if name == "" {
		name = "temporal"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindWorkflow,
		Name:        name,
		DisplayName: "Temporal Workflow",
		Description: "Workflow provider backed by Temporal workflow executions.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	temporalClient, _, _, err := p.state()
	if err != nil {
		return err
	}
	_, err = temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		PageSize: defaultListPageSize,
		Query:    fmt.Sprintf("WorkflowType = %q", runWorkflowName),
	})
	if err != nil {
		return fmt.Errorf("temporal workflow: list workflows: %w", err)
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	w := p.worker
	temporalClient := p.client
	p.name = ""
	p.executionPrefix = ""
	p.cfg = config{}
	p.client = nil
	p.worker = nil
	p.eventTriggers = nil
	p.deletedTriggers = nil
	p.mu.Unlock()

	if w != nil {
		w.Stop()
	}
	if temporalClient != nil {
		temporalClient.Close()
	}
	return nil
}

func (p *Provider) StartRun(ctx context.Context, req *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeScopedTarget(req.GetPluginName(), req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	temporalClient, cfg, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	key := strings.TrimSpace(req.GetIdempotencyKey())
	runID := uuid.NewString()
	if key != "" {
		runID = idempotentManualRunID(target.PluginName, key)
		if existing, found, err := p.getRunSnapshot(ctx, temporalClient, prefix, target.PluginName, runID); err != nil {
			return nil, status.Errorf(codes.Internal, "lookup idempotent run: %v", err)
		} else if found {
			return existing.toProto()
		}
	}

	now := time.Now().UTC()
	run := runSnapshot{
		ID:         runID,
		PluginName: target.PluginName,
		Status:     proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target: targetSnapshot{
			PluginName: target.PluginName,
			Operation:  target.Operation,
			Input:      cloneMap(target.Input),
		},
		Trigger:   runTriggerSnapshot{Kind: triggerKindManual},
		CreatedAt: now,
		CreatedBy: actorSnapshotFromProto(req.GetCreatedBy()),
	}
	if _, _, err := startRunExecution(ctx, temporalClient, cfg, prefix, run); err != nil {
		return nil, status.Errorf(codes.Internal, "start run: %v", err)
	}
	if key != "" {
		persisted, found, err := p.waitForRunStatus(ctx, temporalClient, prefix, target.PluginName, runID, 2*time.Second, func(runSnapshot) bool {
			return true
		})
		if err != nil {
			return nil, waitStatusError("load started run", err)
		}
		if found {
			return persisted.toProto()
		}
	}
	return run.toProto()
}

func (p *Provider) GetRun(ctx context.Context, req *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	runID := strings.TrimSpace(req.GetRunId())
	if pluginName == "" || runID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and run_id are required")
	}

	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := p.getRunSnapshot(ctx, temporalClient, prefix, pluginName, runID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	return run.toProto()
}

func (p *Provider) ListRuns(ctx context.Context, req *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	if pluginName == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name is required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	infos, err := listWorkflowInfos(ctx, temporalClient, fmt.Sprintf("WorkflowType = %q", runWorkflowName))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}
	runs := make([]runSnapshot, 0, len(infos))
	for _, info := range infos {
		if !strings.HasPrefix(info.GetExecution().GetWorkflowId(), runWorkflowPrefix(prefix)) {
			continue
		}
		run, err := runSnapshotFromExecutionInfo(info)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode run: %v", err)
		}
		if run.PluginName != pluginName {
			continue
		}
		runs = append(runs, run)
	}
	slices.SortFunc(runs, func(a, b runSnapshot) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})

	resp := &proto.ListWorkflowProviderRunsResponse{Runs: make([]*proto.BoundWorkflowRun, 0, len(runs))}
	for _, run := range runs {
		pbRun, err := run.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build run response: %v", err)
		}
		resp.Runs = append(resp.Runs, pbRun)
	}
	return resp, nil
}

func (p *Provider) CancelRun(ctx context.Context, req *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	runID := strings.TrimSpace(req.GetRunId())
	if pluginName == "" || runID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and run_id are required")
	}

	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := p.getRunSnapshot(ctx, temporalClient, prefix, pluginName, runID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	switch run.Status {
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED:
		return nil, status.Errorf(codes.FailedPrecondition, "workflow run %q is already complete", runID)
	}

	if reason := strings.TrimSpace(req.GetReason()); reason != "" {
		if err := temporalClient.SignalWorkflow(ctx, runWorkflowID(prefix, runID), "", runCancelSignalName, runCancelSignal{Reason: reason}); err != nil && !isNotFound(err) {
			return nil, status.Errorf(codes.Internal, "set cancel reason: %v", err)
		}
	}
	if err := temporalClient.CancelWorkflow(ctx, runWorkflowID(prefix, runID), ""); err != nil {
		return nil, status.Errorf(codes.Internal, "cancel workflow: %v", err)
	}
	canceled, found, err := p.waitForRunStatus(ctx, temporalClient, prefix, pluginName, runID, 10*time.Second, func(value runSnapshot) bool {
		return value.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
	})
	if err != nil {
		return nil, waitStatusError("wait for cancellation", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	return canceled.toProto()
}

func (p *Provider) UpsertSchedule(ctx context.Context, req *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeScopedTarget(req.GetPluginName(), req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if scheduleID == "" {
		scheduleID = uuid.NewString()
	}
	cronSpec := strings.TrimSpace(req.GetCron())
	if cronSpec == "" {
		return nil, status.Error(codes.InvalidArgument, "cron is required")
	}
	location, timezone, err := parseTimezone(req.GetTimezone())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	parser := cronParser()
	if _, err := parser.Parse(cronSpec); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cron: %v", err)
	}

	temporalClient, cfg, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	existing, found, err := p.getOpenScheduleSnapshot(ctx, temporalClient, prefix, scheduleID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if found && existing.PluginName != target.PluginName {
		return nil, status.Errorf(codes.AlreadyExists, "workflow schedule %q is already owned by plugin %q", scheduleID, existing.PluginName)
	}

	now := time.Now().UTC()
	next, err := nextCronTime(parser, cronSpec, location, now)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cron: %v", err)
	}
	record := scheduleSnapshot{
		ID:         scheduleID,
		PluginName: target.PluginName,
		Cron:       cronSpec,
		Timezone:   timezone,
		Target: targetSnapshot{
			PluginName: target.PluginName,
			Operation:  target.Operation,
			Input:      cloneMap(target.Input),
		},
		Paused:    req.GetPaused(),
		CreatedAt: now,
		UpdatedAt: now,
		NextRunAt: next,
		CreatedBy: actorSnapshotFromProto(req.GetRequestedBy()),
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = cloneActorSnapshot(existing.CreatedBy)
	}

	if found {
		if err := temporalClient.SignalWorkflow(ctx, scheduleWorkflowID(prefix, scheduleID), "", scheduleApplySignalName, scheduleApplySignal{
			Record: record,
			Config: runWorkflowConfigFromProvider(p.name, prefix, cfg),
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "update schedule: %v", err)
		}
	} else if err := startScheduleDefinition(ctx, temporalClient, prefix, cfg, record, p.name); err != nil {
		return nil, status.Errorf(codes.Internal, "create schedule: %v", err)
	}

	value, found, err := p.waitForScheduleSnapshot(ctx, temporalClient, prefix, scheduleID, 5*time.Second, func(value scheduleSnapshot) bool {
		return value.UpdatedAt.Equal(record.UpdatedAt)
	})
	if err != nil {
		return nil, waitStatusError("wait for schedule", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	return value.toProto()
}

func (p *Provider) GetSchedule(ctx context.Context, req *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if pluginName == "" || scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and schedule_id are required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	value, found, err := p.getOpenScheduleSnapshot(ctx, temporalClient, prefix, scheduleID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get schedule: %v", err)
	}
	if !found || value.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	return value.toProto()
}

func (p *Provider) ListSchedules(ctx context.Context, req *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	if pluginName == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name is required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	infos, err := listWorkflowInfos(ctx, temporalClient, fmt.Sprintf("WorkflowType = %q AND CloseTime IS NULL", scheduleWorkflowName))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
	}

	values := make([]scheduleSnapshot, 0, len(infos))
	for _, info := range infos {
		if !strings.HasPrefix(info.GetExecution().GetWorkflowId(), scheduleWorkflowPrefix(prefix)) {
			continue
		}
		value, err := decodeMemoRecord[scheduleSnapshot](info.GetMemo())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode schedule: %v", err)
		}
		if value.PluginName != pluginName {
			continue
		}
		values = append(values, value)
	}
	slices.SortFunc(values, func(a, b scheduleSnapshot) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})

	resp := &proto.ListWorkflowProviderSchedulesResponse{Schedules: make([]*proto.BoundWorkflowSchedule, 0, len(values))}
	for _, value := range values {
		pbValue, err := value.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
		}
		resp.Schedules = append(resp.Schedules, pbValue)
	}
	return resp, nil
}

func (p *Provider) DeleteSchedule(ctx context.Context, req *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if pluginName == "" || scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and schedule_id are required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	value, found, err := p.getOpenScheduleSnapshot(ctx, temporalClient, prefix, scheduleID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found || value.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	if err := temporalClient.SignalWorkflow(ctx, scheduleWorkflowID(prefix, scheduleID), "", definitionDeleteSignalName, "deleted"); err != nil {
		return nil, status.Errorf(codes.Internal, "delete schedule: %v", err)
	}
	if err := waitForWorkflowClosed(ctx, temporalClient, scheduleWorkflowID(prefix, scheduleID), 5*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "delete schedule: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) PauseSchedule(ctx context.Context, req *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, strings.TrimSpace(req.GetPluginName()), strings.TrimSpace(req.GetScheduleId()), true)
}

func (p *Provider) ResumeSchedule(ctx context.Context, req *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, strings.TrimSpace(req.GetPluginName()), strings.TrimSpace(req.GetScheduleId()), false)
}

func (p *Provider) UpsertEventTrigger(ctx context.Context, req *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeScopedTarget(req.GetPluginName(), req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if triggerID == "" {
		triggerID = uuid.NewString()
	}
	matchType := strings.TrimSpace(req.GetMatch().GetType())
	if matchType == "" {
		return nil, status.Error(codes.InvalidArgument, "match.type is required")
	}

	temporalClient, cfg, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	existing, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if found && existing.PluginName != target.PluginName {
		return nil, status.Errorf(codes.AlreadyExists, "workflow event trigger %q is already owned by plugin %q", triggerID, existing.PluginName)
	}

	now := time.Now().UTC()
	record := eventTriggerSnapshot{
		ID:           triggerID,
		PluginName:   target.PluginName,
		MatchType:    matchType,
		MatchSource:  strings.TrimSpace(req.GetMatch().GetSource()),
		MatchSubject: strings.TrimSpace(req.GetMatch().GetSubject()),
		Target: targetSnapshot{
			PluginName: target.PluginName,
			Operation:  target.Operation,
			Input:      cloneMap(target.Input),
		},
		Paused:     req.GetPaused(),
		CreatedAt:  now,
		UpdatedAt:  now,
		CreatedBy:  actorSnapshotFromProto(req.GetRequestedBy()),
		InstanceID: uuid.NewString(),
		Revision:   1,
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = cloneActorSnapshot(existing.CreatedBy)
		record.InstanceID = existing.InstanceID
		if record.InstanceID == "" {
			record.InstanceID = uuid.NewString()
		}
		record.Revision = existing.Revision + 1
	}

	if found {
		if err := temporalClient.SignalWorkflow(ctx, eventTriggerWorkflowID(prefix, triggerID), "", eventTriggerApplySignalName, eventTriggerApplySignal{Record: record}); err != nil {
			return nil, status.Errorf(codes.Internal, "update event trigger: %v", err)
		}
	} else if err := startEventTriggerDefinition(ctx, temporalClient, prefix, cfg, record); err != nil {
		return nil, status.Errorf(codes.Internal, "create event trigger: %v", err)
	}

	value, found, err := p.waitForEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID, 5*time.Second, func(value eventTriggerSnapshot) bool {
		return value.UpdatedAt.Equal(record.UpdatedAt)
	})
	if err != nil {
		return nil, waitStatusError("wait for event trigger", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	p.rememberEventTrigger(value)
	return value.toProto()
}

func (p *Provider) GetEventTrigger(ctx context.Context, req *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if pluginName == "" || triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and trigger_id are required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	value, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get event trigger: %v", err)
	}
	if !found || value.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	return value.toProto()
}

func (p *Provider) ListEventTriggers(ctx context.Context, req *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	if pluginName == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name is required")
	}
	values, err := p.listEventTriggerSnapshots(ctx, pluginName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	slices.SortFunc(values, func(a, b eventTriggerSnapshot) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})

	resp := &proto.ListWorkflowProviderEventTriggersResponse{Triggers: make([]*proto.BoundWorkflowEventTrigger, 0, len(values))}
	for _, value := range values {
		pbValue, err := value.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
		}
		resp.Triggers = append(resp.Triggers, pbValue)
	}
	return resp, nil
}

func (p *Provider) DeleteEventTrigger(ctx context.Context, req *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if pluginName == "" || triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and trigger_id are required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	value, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found || value.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	if err := temporalClient.SignalWorkflow(ctx, eventTriggerWorkflowID(prefix, triggerID), "", definitionDeleteSignalName, "deleted"); err != nil {
		return nil, status.Errorf(codes.Internal, "delete event trigger: %v", err)
	}
	if err := waitForWorkflowClosed(ctx, temporalClient, eventTriggerWorkflowID(prefix, triggerID), 5*time.Second); err != nil {
		return nil, status.Errorf(codes.Internal, "delete event trigger: %v", err)
	}
	p.rememberDeletedEventTrigger(triggerID, value)
	return &emptypb.Empty{}, nil
}

func (p *Provider) PauseEventTrigger(ctx context.Context, req *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, strings.TrimSpace(req.GetPluginName()), strings.TrimSpace(req.GetTriggerId()), true)
}

func (p *Provider) ResumeEventTrigger(ctx context.Context, req *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, strings.TrimSpace(req.GetPluginName()), strings.TrimSpace(req.GetTriggerId()), false)
}

func (p *Provider) PublishEvent(ctx context.Context, req *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	if pluginName == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name is required")
	}

	temporalClient, cfg, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	event, err := normalizeWorkflowEvent(req.GetEvent(), time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	eventValue := eventSnapshotFromProto(event)

	triggers, err := p.listEventTriggerSnapshots(ctx, pluginName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	for _, trigger := range triggers {
		if trigger.PluginName != pluginName || trigger.Paused || !eventMatchesTrigger(event, trigger) {
			continue
		}

		runID := uuid.NewString()
		if strings.TrimSpace(event.GetId()) != "" {
			triggerInstanceID := trigger.InstanceID
			if strings.TrimSpace(triggerInstanceID) == "" {
				triggerInstanceID = trigger.ID
			}
			runID = eventRunID(triggerInstanceID, event.GetSource(), event.GetId())
		}
		run := runSnapshot{
			ID:         runID,
			PluginName: pluginName,
			Status:     proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:     trigger.Target,
			Trigger: runTriggerSnapshot{
				Kind:           triggerKindEvent,
				EventTriggerID: trigger.ID,
				Event:          &eventValue,
			},
			CreatedAt: time.Now().UTC(),
			CreatedBy: cloneActorSnapshot(trigger.CreatedBy),
		}
		if _, _, err := startRunExecution(ctx, temporalClient, cfg, prefix, run); err != nil {
			return nil, status.Errorf(codes.Internal, "start event run: %v", err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) updateSchedulePaused(ctx context.Context, pluginName, scheduleID string, paused bool) (*proto.BoundWorkflowSchedule, error) {
	if pluginName == "" || scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and schedule_id are required")
	}
	temporalClient, cfg, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := p.getOpenScheduleSnapshot(ctx, temporalClient, prefix, scheduleID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found || record.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	record.Paused = paused
	record.UpdatedAt = time.Now().UTC()
	if !paused {
		location, _, err := parseTimezone(record.Timezone)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parse schedule timezone: %v", err)
		}
		next, err := nextCronTime(cronParser(), record.Cron, location, record.UpdatedAt)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parse schedule cron: %v", err)
		}
		record.NextRunAt = next
	}
	if err := temporalClient.SignalWorkflow(ctx, scheduleWorkflowID(prefix, scheduleID), "", scheduleApplySignalName, scheduleApplySignal{
		Record: record,
		Config: runWorkflowConfigFromProvider(p.name, prefix, cfg),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "update schedule: %v", err)
	}
	value, found, err := p.waitForScheduleSnapshot(ctx, temporalClient, prefix, scheduleID, 5*time.Second, func(value scheduleSnapshot) bool {
		return value.UpdatedAt.Equal(record.UpdatedAt)
	})
	if err != nil {
		return nil, waitStatusError("wait for schedule", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	return value.toProto()
}

func (p *Provider) updateEventTriggerPaused(ctx context.Context, pluginName, triggerID string, paused bool) (*proto.BoundWorkflowEventTrigger, error) {
	if pluginName == "" || triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name and trigger_id are required")
	}
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found || record.PluginName != pluginName {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	record.Paused = paused
	record.UpdatedAt = time.Now().UTC()
	if err := temporalClient.SignalWorkflow(ctx, eventTriggerWorkflowID(prefix, triggerID), "", eventTriggerApplySignalName, eventTriggerApplySignal{Record: record}); err != nil {
		return nil, status.Errorf(codes.Internal, "update event trigger: %v", err)
	}
	value, found, err := p.waitForEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID, 5*time.Second, func(value eventTriggerSnapshot) bool {
		return value.UpdatedAt.Equal(record.UpdatedAt)
	})
	if err != nil {
		return nil, waitStatusError("wait for event trigger", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	p.rememberEventTrigger(value)
	return value.toProto()
}

func (p *Provider) state() (client.Client, config, string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.client == nil {
		return nil, config{}, "", errors.New("temporal workflow: provider is not configured")
	}
	return p.client, p.cfg, p.executionPrefix, nil
}

func (p *Provider) listEventTriggerSnapshots(ctx context.Context, pluginName string) ([]eventTriggerSnapshot, error) {
	temporalClient, _, prefix, err := p.state()
	if err != nil {
		return nil, err
	}
	infos, err := listWorkflowInfos(ctx, temporalClient, fmt.Sprintf("WorkflowType = %q AND CloseTime IS NULL", eventTriggerWorkflowName))
	if err != nil {
		return nil, err
	}
	valuesByID := make(map[string]eventTriggerSnapshot, len(infos))
	for _, info := range infos {
		if !strings.HasPrefix(info.GetExecution().GetWorkflowId(), eventTriggerWorkflowPrefix(prefix)) {
			continue
		}
		value, err := decodeMemoRecord[eventTriggerSnapshot](info.GetMemo())
		if err != nil {
			return nil, err
		}
		valuesByID[value.ID] = value
	}

	p.mu.RLock()
	localUpdates := make(map[string]eventTriggerSnapshot, len(p.eventTriggers))
	for id, value := range p.eventTriggers {
		localUpdates[id] = value
	}
	localDeletes := make(map[string]eventTriggerDeleteMarker, len(p.deletedTriggers))
	for id, marker := range p.deletedTriggers {
		localDeletes[id] = marker
	}
	p.mu.RUnlock()

	for id, value := range localUpdates {
		current, ok := valuesByID[id]
		if !ok {
			verified, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, id)
			if err != nil {
				return nil, err
			}
			if found {
				current = verified
				ok = true
				valuesByID[id] = verified
			} else {
				p.forgetEventTriggerOverlay(id)
				continue
			}
		}
		deleted, hasDeleted := localDeletes[id]
		if shouldPreferLocalEventTrigger(current, value, deleted, hasDeleted) {
			valuesByID[id] = value
		}
	}

	for id, marker := range localDeletes {
		current, ok := valuesByID[id]
		if ok && shouldSuppressDeletedEventTrigger(current, marker) {
			delete(valuesByID, id)
		}
	}

	out := make([]eventTriggerSnapshot, 0, len(valuesByID))
	for _, value := range valuesByID {
		if pluginName != "" && value.PluginName != pluginName {
			continue
		}
		out = append(out, value)
	}
	return out, nil
}

func (p *Provider) rememberEventTrigger(value eventTriggerSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.eventTriggers == nil {
		p.eventTriggers = map[string]eventTriggerSnapshot{}
	}
	p.eventTriggers[value.ID] = value
	if marker, ok := p.deletedTriggers[value.ID]; ok && marker.InstanceID == value.InstanceID {
		delete(p.deletedTriggers, value.ID)
	}
}

func (p *Provider) rememberDeletedEventTrigger(id string, value eventTriggerSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.eventTriggers, id)
	if p.deletedTriggers == nil {
		p.deletedTriggers = map[string]eventTriggerDeleteMarker{}
	}
	p.deletedTriggers[id] = eventTriggerDeleteMarker{
		InstanceID: value.InstanceID,
		Revision:   value.Revision,
	}
}

func (p *Provider) forgetEventTriggerOverlay(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.eventTriggers, id)
}

func (p *Provider) getRunSnapshot(ctx context.Context, temporalClient client.Client, prefix, pluginName, runID string) (runSnapshot, bool, error) {
	resp, err := temporalClient.DescribeWorkflowExecution(ctx, runWorkflowID(prefix, runID), "")
	if err != nil {
		if isNotFound(err) {
			return runSnapshot{}, false, nil
		}
		return runSnapshot{}, false, err
	}
	run, err := runSnapshotFromExecutionInfo(resp.GetWorkflowExecutionInfo())
	if err != nil {
		return runSnapshot{}, false, err
	}
	if run.PluginName != pluginName {
		return runSnapshot{}, false, nil
	}
	return run, true, nil
}

func (p *Provider) getOpenScheduleSnapshot(ctx context.Context, temporalClient client.Client, prefix, scheduleID string) (scheduleSnapshot, bool, error) {
	resp, err := temporalClient.DescribeWorkflowExecution(ctx, scheduleWorkflowID(prefix, scheduleID), "")
	if err != nil {
		if isNotFound(err) {
			return scheduleSnapshot{}, false, nil
		}
		return scheduleSnapshot{}, false, err
	}
	info := resp.GetWorkflowExecutionInfo()
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return scheduleSnapshot{}, false, nil
	}
	value, err := decodeMemoRecord[scheduleSnapshot](info.GetMemo())
	if err != nil {
		return scheduleSnapshot{}, false, err
	}
	return value, true, nil
}

func (p *Provider) getOpenEventTriggerSnapshot(ctx context.Context, temporalClient client.Client, prefix, triggerID string) (eventTriggerSnapshot, bool, error) {
	resp, err := temporalClient.DescribeWorkflowExecution(ctx, eventTriggerWorkflowID(prefix, triggerID), "")
	if err != nil {
		if isNotFound(err) {
			return eventTriggerSnapshot{}, false, nil
		}
		return eventTriggerSnapshot{}, false, err
	}
	info := resp.GetWorkflowExecutionInfo()
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return eventTriggerSnapshot{}, false, nil
	}
	value, err := decodeMemoRecord[eventTriggerSnapshot](info.GetMemo())
	if err != nil {
		return eventTriggerSnapshot{}, false, err
	}
	return value, true, nil
}

func (p *Provider) waitForRunStatus(ctx context.Context, temporalClient client.Client, prefix, pluginName, runID string, timeout time.Duration, predicate func(runSnapshot) bool) (runSnapshot, bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		value, found, err := p.getRunSnapshot(ctx, temporalClient, prefix, pluginName, runID)
		if err != nil {
			return runSnapshot{}, false, err
		}
		if found && predicate(value) {
			return value, true, nil
		}
		if time.Now().After(deadline) {
			return value, found, context.DeadlineExceeded
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return runSnapshot{}, false, err
		}
	}
}

func (p *Provider) waitForScheduleSnapshot(ctx context.Context, temporalClient client.Client, prefix, scheduleID string, timeout time.Duration, predicate func(scheduleSnapshot) bool) (scheduleSnapshot, bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		value, found, err := p.getOpenScheduleSnapshot(ctx, temporalClient, prefix, scheduleID)
		if err != nil {
			return scheduleSnapshot{}, false, err
		}
		if found && predicate(value) {
			return value, true, nil
		}
		if time.Now().After(deadline) {
			return value, found, context.DeadlineExceeded
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return scheduleSnapshot{}, false, err
		}
	}
}

func (p *Provider) waitForEventTriggerSnapshot(ctx context.Context, temporalClient client.Client, prefix, triggerID string, timeout time.Duration, predicate func(eventTriggerSnapshot) bool) (eventTriggerSnapshot, bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		value, found, err := p.getOpenEventTriggerSnapshot(ctx, temporalClient, prefix, triggerID)
		if err != nil {
			return eventTriggerSnapshot{}, false, err
		}
		if found && predicate(value) {
			return value, true, nil
		}
		if time.Now().After(deadline) {
			return value, found, context.DeadlineExceeded
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return eventTriggerSnapshot{}, false, err
		}
	}
}

func startRunExecution(ctx context.Context, temporalClient client.Client, cfg config, prefix string, run runSnapshot) (client.WorkflowRun, bool, error) {
	options := client.StartWorkflowOptions{
		ID:                                       runWorkflowID(prefix, run.ID),
		TaskQueue:                                cfg.TaskQueue,
		Memo:                                     memoRecord(run),
		WorkflowExecutionTimeout:                 cfg.RunTimeout,
		WorkflowRunTimeout:                       cfg.RunTimeout,
		RetryPolicy:                              &sdktemporal.RetryPolicy{MaximumAttempts: 1},
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
	wfRun, err := temporalClient.ExecuteWorkflow(ctx, options, runWorkflowName, runWorkflowInput{
		Run:    run,
		Config: runWorkflowConfig{ProviderName: "", Namespace: cfg.Namespace, TaskQueue: cfg.TaskQueue, ExecutionPrefix: prefix, ActivityTimeout: cfg.ActivityTimeout, RunTimeout: cfg.RunTimeout},
	})
	if err != nil {
		if isWorkflowAlreadyStarted(err) {
			return temporalClient.GetWorkflow(ctx, runWorkflowID(prefix, run.ID), ""), true, nil
		}
		return nil, false, err
	}
	return wfRun, false, nil
}

func waitStatusError(detail string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Errorf(codes.DeadlineExceeded, "%s: timed out waiting for workflow state to update", detail)
	}
	return status.Errorf(codes.Internal, "%s: %v", detail, err)
}

func shouldPreferLocalEventTrigger(visible, local eventTriggerSnapshot, deleted eventTriggerDeleteMarker, hasDeleted bool) bool {
	if visible.InstanceID == "" || local.InstanceID == "" {
		return !local.UpdatedAt.Before(visible.UpdatedAt)
	}
	if visible.InstanceID != local.InstanceID {
		return hasDeleted && visible.InstanceID == deleted.InstanceID
	}
	return visible.Revision <= local.Revision
}

func shouldSuppressDeletedEventTrigger(visible eventTriggerSnapshot, deleted eventTriggerDeleteMarker) bool {
	if visible.InstanceID == "" || deleted.InstanceID == "" {
		return false
	}
	return visible.InstanceID == deleted.InstanceID && visible.Revision <= deleted.Revision
}

func startScheduleDefinition(ctx context.Context, temporalClient client.Client, prefix string, cfg config, record scheduleSnapshot, providerName string) error {
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       scheduleWorkflowID(prefix, record.ID),
		TaskQueue:                                cfg.TaskQueue,
		Memo:                                     memoRecord(record),
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, scheduleWorkflowName, scheduleWorkflowInput{
		Record: record,
		Config: runWorkflowConfigFromProvider(providerName, prefix, cfg),
	})
	if err != nil && !isWorkflowAlreadyStarted(err) {
		return err
	}
	return nil
}

func startEventTriggerDefinition(ctx context.Context, temporalClient client.Client, prefix string, cfg config, record eventTriggerSnapshot) error {
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       eventTriggerWorkflowID(prefix, record.ID),
		TaskQueue:                                cfg.TaskQueue,
		Memo:                                     memoRecord(record),
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, eventTriggerWorkflowName, eventTriggerWorkflowInput{Record: record})
	if err != nil && !isWorkflowAlreadyStarted(err) {
		return err
	}
	return nil
}

func runWorkflowConfigFromProvider(providerName, prefix string, cfg config) runWorkflowConfig {
	return runWorkflowConfig{
		ProviderName:    providerName,
		Namespace:       cfg.Namespace,
		TaskQueue:       cfg.TaskQueue,
		ExecutionPrefix: prefix,
		ActivityTimeout: cfg.ActivityTimeout,
		RunTimeout:      cfg.RunTimeout,
	}
}

func listWorkflowInfos(ctx context.Context, temporalClient client.Client, query string) ([]*workflowpb.WorkflowExecutionInfo, error) {
	var (
		out       []*workflowpb.WorkflowExecutionInfo
		nextToken []byte
	)
	for {
		resp, err := temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      defaultListPageSize,
			NextPageToken: nextToken,
			Query:         query,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetExecutions()...)
		if len(resp.GetNextPageToken()) == 0 {
			return out, nil
		}
		nextToken = resp.GetNextPageToken()
	}
}

func waitForWorkflowClosed(ctx context.Context, temporalClient client.Client, workflowID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		if resp.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		if err := sleepContext(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
}

func decodeConfig(name string, raw map[string]any) (config, error) {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return config{}, fmt.Errorf("marshal config: %w", err)
	}
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	if strings.TrimSpace(cfg.HostPort) == "" {
		cfg.HostPort = defaultHostPort
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		cfg.Namespace = defaultNamespace
	}
	if cfg.ActivityTimeout <= 0 {
		cfg.ActivityTimeout = defaultActivityTimeout
	}
	if cfg.RunTimeout <= 0 {
		cfg.RunTimeout = defaultRunTimeout
	}
	if strings.TrimSpace(cfg.TaskQueue) == "" {
		cfg.TaskQueue = "gestalt-workflow-temporal-" + providerIdentitySegment(name)
	}
	return cfg, nil
}

func runExecutionWorkflow(ctx workflow.Context, input runWorkflowInput) error {
	state := input.Run
	now := workflow.Now(ctx).UTC()
	state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	state.StartedAt = timePtr(now)
	state.StatusMessage = ""
	if err := upsertWorkflowMemo(ctx, state); err != nil {
		return err
	}

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: input.Config.ActivityTimeout,
		RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: 1},
		WaitForCancellation: true,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	cancelReasonCh := workflow.GetSignalChannel(ctx, runCancelSignalName)
	cancelReason := "workflow run canceled"

	info := workflow.GetInfo(ctx)
	activityInput := invokeActivityInput{
		Run:        state,
		Provider:   input.Config.ProviderName,
		Namespace:  input.Config.Namespace,
		WorkflowID: info.WorkflowExecution.ID,
		RunID:      info.WorkflowExecution.RunID,
		Metadata: map[string]any{
			"temporal": map[string]any{
				"namespace":  input.Config.Namespace,
				"workflowId": info.WorkflowExecution.ID,
				"runId":      info.WorkflowExecution.RunID,
			},
		},
	}

	var (
		result       invokeActivityResult
		err          error
		activityDone bool
	)
	activityFuture := workflow.ExecuteActivity(ctx, invokeActivityName, activityInput)
	for !activityDone {
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(activityFuture, func(f workflow.Future) {
			err = f.Get(ctx, &result)
			activityDone = true
		})
		selector.AddReceive(cancelReasonCh, func(c workflow.ReceiveChannel, _ bool) {
			var signal runCancelSignal
			c.Receive(ctx, &signal)
			if reason := strings.TrimSpace(signal.Reason); reason != "" {
				cancelReason = reason
			}
		})
		selector.Select(ctx)
	}
	drainRunCancelReasonSignal(cancelReasonCh, &cancelReason)

	completedAt := workflow.Now(ctx).UTC()
	state.CompletedAt = timePtr(completedAt)
	switch {
	case err != nil && sdktemporal.IsCanceledError(err):
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
		state.StatusMessage = cancelReason
	case err != nil:
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
		state.StatusMessage = err.Error()
	default:
		state.ResultBody = result.Body
		if result.Status >= http.StatusBadRequest {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			state.StatusMessage = fmt.Sprintf("workflow operation returned status %d", result.Status)
		} else {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			state.StatusMessage = ""
		}
	}
	if memoErr := upsertWorkflowMemo(ctx, state); memoErr != nil {
		return memoErr
	}
	switch state.Status {
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED:
		return sdktemporal.NewCanceledError(state.StatusMessage)
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED:
		return errors.New(state.StatusMessage)
	default:
		return nil
	}
}

func drainRunCancelReasonSignal(ch workflow.ReceiveChannel, cancelReason *string) {
	for {
		var signal runCancelSignal
		if !ch.ReceiveAsync(&signal) {
			return
		}
		if reason := strings.TrimSpace(signal.Reason); reason != "" {
			*cancelReason = reason
		}
	}
}

// Schedule definitions keep their durable state in memo and emit child run workflows on due cron boundaries.
func scheduleDefinitionWorkflow(ctx workflow.Context, input scheduleWorkflowInput) error {
	state := input.Record
	runCfg := input.Config
	steps := input.Steps
	if err := upsertWorkflowMemo(ctx, state); err != nil {
		return err
	}

	applyCh := workflow.GetSignalChannel(ctx, scheduleApplySignalName)
	deleteCh := workflow.GetSignalChannel(ctx, definitionDeleteSignalName)
	for {
		if state.Paused || state.NextRunAt == nil {
			var (
				loopErr error
				deleted bool
			)
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(applyCh, func(c workflow.ReceiveChannel, _ bool) {
				var signal scheduleApplySignal
				c.Receive(ctx, &signal)
				state = signal.Record
				runCfg = signal.Config
				loopErr = upsertWorkflowMemo(ctx, state)
			})
			selector.AddReceive(deleteCh, func(c workflow.ReceiveChannel, _ bool) {
				var reason string
				c.Receive(ctx, &reason)
				deleted = true
			})
			selector.Select(ctx)
			if loopErr != nil {
				return loopErr
			}
			if deleted {
				return nil
			}
			steps++
			if steps >= scheduleContinueAsNewEvery {
				return workflow.NewContinueAsNewError(ctx, scheduleWorkflowName, scheduleWorkflowInput{
					Record: state,
					Config: runCfg,
				})
			}
			continue
		}

		delay := state.NextRunAt.Sub(workflow.Now(ctx).UTC())
		if delay < 0 {
			delay = 0
		}
		timer := workflow.NewTimer(ctx, delay)
		var (
			loopErr error
			applied bool
			deleted bool
		)
		selector := workflow.NewSelector(ctx)
		selector.AddFuture(timer, func(workflow.Future) {})
		selector.AddReceive(applyCh, func(c workflow.ReceiveChannel, _ bool) {
			var signal scheduleApplySignal
			c.Receive(ctx, &signal)
			state = signal.Record
			runCfg = signal.Config
			applied = true
			loopErr = upsertWorkflowMemo(ctx, state)
		})
		selector.AddReceive(deleteCh, func(c workflow.ReceiveChannel, _ bool) {
			var reason string
			c.Receive(ctx, &reason)
			deleted = true
		})
		selector.Select(ctx)
		if loopErr != nil {
			return loopErr
		}
		if deleted {
			return nil
		}
		if applied {
			continue
		}

		location, err := time.LoadLocation(state.Timezone)
		if err != nil {
			return err
		}
		latestDue, nextRun, err := collapseCron(cronParser(), state.Cron, location, *state.NextRunAt, workflow.Now(ctx).UTC())
		if err != nil {
			return err
		}
		run := runSnapshot{
			ID:         scheduleRunID(state.ID, latestDue),
			PluginName: state.PluginName,
			Status:     proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:     state.Target,
			Trigger: runTriggerSnapshot{
				Kind:         triggerKindSchedule,
				ScheduleID:   state.ID,
				ScheduledFor: timePtr(latestDue),
			},
			CreatedAt: workflow.Now(ctx).UTC(),
			CreatedBy: cloneActorSnapshot(state.CreatedBy),
		}
		if err := startChildRunWorkflow(ctx, runCfg, run); err != nil && !sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
			return err
		}

		state.NextRunAt = timePtr(nextRun)
		state.UpdatedAt = workflow.Now(ctx).UTC()
		if err := upsertWorkflowMemo(ctx, state); err != nil {
			return err
		}
		steps++
		if steps >= scheduleContinueAsNewEvery {
			return workflow.NewContinueAsNewError(ctx, scheduleWorkflowName, scheduleWorkflowInput{
				Record: state,
				Config: runCfg,
			})
		}
	}
}

func eventTriggerDefinitionWorkflow(ctx workflow.Context, input eventTriggerWorkflowInput) error {
	state := input.Record
	if err := upsertWorkflowMemo(ctx, state); err != nil {
		return err
	}

	applyCh := workflow.GetSignalChannel(ctx, eventTriggerApplySignalName)
	deleteCh := workflow.GetSignalChannel(ctx, definitionDeleteSignalName)
	for {
		var (
			loopErr error
			deleted bool
		)
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(applyCh, func(c workflow.ReceiveChannel, _ bool) {
			var signal eventTriggerApplySignal
			c.Receive(ctx, &signal)
			state = signal.Record
			loopErr = upsertWorkflowMemo(ctx, state)
		})
		selector.AddReceive(deleteCh, func(c workflow.ReceiveChannel, _ bool) {
			var reason string
			c.Receive(ctx, &reason)
			deleted = true
		})
		selector.Select(ctx)
		if loopErr != nil {
			return loopErr
		}
		if deleted {
			return nil
		}
	}
}

func startChildRunWorkflow(ctx workflow.Context, cfg runWorkflowConfig, run runSnapshot) error {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:               runWorkflowID(cfg.ExecutionPrefix, run.ID),
		TaskQueue:                cfg.TaskQueue,
		Memo:                     memoRecord(run),
		WorkflowExecutionTimeout: cfg.RunTimeout,
		WorkflowRunTimeout:       cfg.RunTimeout,
		RetryPolicy:              &sdktemporal.RetryPolicy{MaximumAttempts: 1},
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	})
	return workflow.ExecuteChildWorkflow(childCtx, runWorkflowName, runWorkflowInput{Run: run, Config: cfg}).GetChildWorkflowExecution().Get(childCtx, nil)
}

func invokeWorkflowOperationActivity(ctx context.Context, input invokeActivityInput) (*invokeActivityResult, error) {
	host, err := gestalt.WorkflowHost()
	if err != nil {
		return nil, err
	}
	defer func() { _ = host.Close() }()

	req := &proto.InvokeWorkflowOperationRequest{
		Target:     input.Run.Target.toProto(),
		RunId:      input.Run.ID,
		Trigger:    input.Run.Trigger.toProto(),
		PluginName: input.Run.PluginName,
		CreatedBy:  input.Run.CreatedBy.toProto(),
	}
	if len(input.Metadata) > 0 {
		value, err := structpb.NewStruct(input.Metadata)
		if err != nil {
			return nil, err
		}
		req.Metadata = value
	}

	type invokeResult struct {
		response *proto.InvokeWorkflowOperationResponse
		err      error
	}

	invokeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	doneCh := make(chan invokeResult, 1)
	go func() {
		resp, err := host.InvokeOperation(invokeCtx, req)
		doneCh <- invokeResult{response: resp, err: err}
	}()

	heartbeat := time.NewTicker(500 * time.Millisecond)
	defer heartbeat.Stop()

	for {
		select {
		case result := <-doneCh:
			if result.err != nil {
				if ctx.Err() != nil {
					return nil, sdktemporal.NewCanceledError("workflow run canceled")
				}
				if st, ok := status.FromError(result.err); ok && st.Code() == codes.Canceled {
					return nil, sdktemporal.NewCanceledError("workflow run canceled")
				}
				return nil, result.err
			}
			return &invokeActivityResult{
				Status: result.response.GetStatus(),
				Body:   result.response.GetBody(),
			}, nil
		case <-heartbeat.C:
			activity.RecordHeartbeat(ctx)
		case <-ctx.Done():
			cancel()
			return nil, sdktemporal.NewCanceledError("workflow run canceled")
		}
	}
}

func executionPrefixForName(name string) string {
	return "gestalt-workflow-temporal/" + providerIdentitySegment(name)
}

func providerIdentitySegment(name string) string {
	name = strings.TrimSpace(name)
	slug := sanitizeIDSegment(name)
	sum := sha256.Sum256([]byte(name))
	return slug + "-" + hex.EncodeToString(sum[:8])
}

func runWorkflowPrefix(prefix string) string {
	return prefix + "/run/"
}

func scheduleWorkflowPrefix(prefix string) string {
	return prefix + "/schedule/"
}

func eventTriggerWorkflowPrefix(prefix string) string {
	return prefix + "/event-trigger/"
}

func runWorkflowID(prefix, runID string) string {
	return runWorkflowPrefix(prefix) + runID
}

func scheduleWorkflowID(prefix, scheduleID string) string {
	return scheduleWorkflowPrefix(prefix) + scheduleID
}

func eventTriggerWorkflowID(prefix, triggerID string) string {
	return eventTriggerWorkflowPrefix(prefix) + triggerID
}

func sanitizeIDSegment(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "default"
	}
	return out
}

func normalizeScopedTarget(pluginName string, target *proto.BoundWorkflowTarget) (scopedTarget, error) {
	pluginName = strings.TrimSpace(pluginName)
	if pluginName == "" {
		return scopedTarget{}, errors.New("plugin_name is required")
	}
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	targetPlugin := strings.TrimSpace(target.GetPluginName())
	if targetPlugin == "" {
		targetPlugin = pluginName
	}
	if targetPlugin != pluginName {
		return scopedTarget{}, fmt.Errorf("target.plugin_name %q is outside scoped plugin %q", targetPlugin, pluginName)
	}
	operation := strings.TrimSpace(target.GetOperation())
	if operation == "" {
		return scopedTarget{}, errors.New("target.operation is required")
	}
	return scopedTarget{
		PluginName: pluginName,
		Operation:  operation,
		Input:      cloneStructMap(target.GetInput()),
	}, nil
}

func normalizeWorkflowEvent(event *proto.WorkflowEvent, now time.Time) (*proto.WorkflowEvent, error) {
	if event == nil {
		return nil, errors.New("event is required")
	}
	eventType := strings.TrimSpace(event.GetType())
	if eventType == "" {
		return nil, errors.New("event.type is required")
	}
	normalized := &proto.WorkflowEvent{
		Id:              strings.TrimSpace(event.GetId()),
		Source:          strings.TrimSpace(event.GetSource()),
		SpecVersion:     strings.TrimSpace(event.GetSpecVersion()),
		Type:            eventType,
		Subject:         strings.TrimSpace(event.GetSubject()),
		Datacontenttype: strings.TrimSpace(event.GetDatacontenttype()),
		Data:            cloneStruct(event.GetData()),
		Extensions:      cloneExtensions(event.GetExtensions()),
	}
	if normalized.Id == "" {
		normalized.Id = uuid.NewString()
	}
	if normalized.SpecVersion == "" {
		normalized.SpecVersion = defaultSpecVersion
	}
	if ts := event.GetTime(); ts != nil && ts.IsValid() {
		normalized.Time = timestamppb.New(ts.AsTime().UTC())
	} else {
		normalized.Time = timestamppb.New(now.UTC())
	}
	return normalized, nil
}

func parseTimezone(raw string) (*time.Location, string, error) {
	timezone := strings.TrimSpace(raw)
	if timezone == "" {
		timezone = defaultTimezone
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, "", fmt.Errorf("invalid timezone %q", timezone)
	}
	return location, timezone, nil
}

func cronParser() cron.Parser {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
}

func nextCronTime(parser cron.Parser, spec string, location *time.Location, now time.Time) (*time.Time, error) {
	schedule, err := parser.Parse(spec)
	if err != nil {
		return nil, err
	}
	next := schedule.Next(now.In(location))
	return timePtr(next.UTC()), nil
}

func collapseCron(parser cron.Parser, spec string, location *time.Location, start, now time.Time) (time.Time, time.Time, error) {
	schedule, err := parser.Parse(spec)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	latestDue := start.In(location)
	current := now.In(location)
	for {
		next := schedule.Next(latestDue)
		if next.After(current) {
			return latestDue.UTC(), next.UTC(), nil
		}
		latestDue = next
	}
}

func eventMatchesTrigger(event *proto.WorkflowEvent, trigger eventTriggerSnapshot) bool {
	if event == nil {
		return false
	}
	if strings.TrimSpace(event.GetType()) != trigger.MatchType {
		return false
	}
	if trigger.MatchSource != "" && strings.TrimSpace(event.GetSource()) != trigger.MatchSource {
		return false
	}
	if trigger.MatchSubject != "" && strings.TrimSpace(event.GetSubject()) != trigger.MatchSubject {
		return false
	}
	return true
}

func idempotentManualRunID(pluginName, key string) string {
	sum := sha256.Sum256([]byte(pluginName + "\x00" + key))
	return "manual:" + hex.EncodeToString(sum[:16])
}

func eventRunID(triggerInstanceID, eventSource, eventID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(triggerInstanceID) + "\x00" + strings.TrimSpace(eventSource) + "\x00" + strings.TrimSpace(eventID)))
	return "event:" + hex.EncodeToString(sum[:16])
}

func scheduleRunID(scheduleID string, scheduledFor time.Time) string {
	return "schedule:" + scheduleID + ":" + scheduledFor.UTC().Format(time.RFC3339Nano)
}

func memoRecord(value any) map[string]any {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return map[string]any{memoRecordKey: string(encoded)}
}

func upsertWorkflowMemo(ctx workflow.Context, value any) error {
	return workflow.UpsertMemo(ctx, memoRecord(value))
}

func decodeMemoRecord[T any](memo *commonpb.Memo) (T, error) {
	var zero T
	if memo == nil || memo.GetFields() == nil {
		return zero, errors.New("missing memo")
	}
	payload, ok := memo.GetFields()[memoRecordKey]
	if !ok {
		return zero, fmt.Errorf("memo key %q is missing", memoRecordKey)
	}
	var raw string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &raw); err != nil {
		return zero, err
	}
	var value T
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return zero, err
	}
	return value, nil
}

func runSnapshotFromExecutionInfo(info *workflowpb.WorkflowExecutionInfo) (runSnapshot, error) {
	if info == nil {
		return runSnapshot{}, errors.New("workflow execution info is required")
	}
	value, err := decodeMemoRecord[runSnapshot](info.GetMemo())
	if err != nil {
		return runSnapshot{}, err
	}
	if value.CreatedAt.IsZero() && info.GetStartTime() != nil {
		value.CreatedAt = info.GetStartTime().AsTime().UTC()
	}
	if value.StartedAt == nil && info.GetExecutionTime() != nil {
		value.StartedAt = timePtr(info.GetExecutionTime().AsTime().UTC())
	}
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		if value.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			value.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		if value.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING || value.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
			value.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		value.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
		if value.StatusMessage == "" {
			value.StatusMessage = "workflow run canceled"
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		value.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
		if value.StatusMessage == "" {
			value.StatusMessage = "workflow execution failed"
		}
	}
	if value.CompletedAt == nil && info.GetCloseTime() != nil && info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		value.CompletedAt = timePtr(info.GetCloseTime().AsTime().UTC())
	}
	return value, nil
}

func (s targetSnapshot) toProto() *proto.BoundWorkflowTarget {
	var input *structpb.Struct
	if s.Input != nil {
		input, _ = structpb.NewStruct(cloneMap(s.Input))
	}
	return &proto.BoundWorkflowTarget{
		PluginName: s.PluginName,
		Operation:  s.Operation,
		Input:      input,
	}
}

func (s *actorSnapshot) toProto() *proto.WorkflowActor {
	if s == nil {
		return nil
	}
	return &proto.WorkflowActor{
		SubjectId:   s.SubjectID,
		SubjectKind: s.SubjectKind,
		DisplayName: s.DisplayName,
		AuthSource:  s.AuthSource,
	}
}

func (s runTriggerSnapshot) toProto() *proto.WorkflowRunTrigger {
	switch s.Kind {
	case triggerKindSchedule:
		return &proto.WorkflowRunTrigger{
			Kind: &proto.WorkflowRunTrigger_Schedule{
				Schedule: &proto.WorkflowScheduleTrigger{
					ScheduleId:   s.ScheduleID,
					ScheduledFor: timestampPtr(s.ScheduledFor),
				},
			},
		}
	case triggerKindEvent:
		var event *proto.WorkflowEvent
		if s.Event != nil {
			event = s.Event.toProto()
		}
		return &proto.WorkflowRunTrigger{
			Kind: &proto.WorkflowRunTrigger_Event{
				Event: &proto.WorkflowEventTriggerInvocation{
					TriggerId: s.EventTriggerID,
					Event:     event,
				},
			},
		}
	default:
		return &proto.WorkflowRunTrigger{
			Kind: &proto.WorkflowRunTrigger_Manual{
				Manual: &proto.WorkflowManualTrigger{},
			},
		}
	}
}

func (s eventSnapshot) toProto() *proto.WorkflowEvent {
	var (
		data       *structpb.Struct
		extensions map[string]*structpb.Value
	)
	if s.Data != nil {
		data, _ = structpb.NewStruct(cloneMap(s.Data))
	}
	if len(s.Extensions) > 0 {
		extensions = make(map[string]*structpb.Value, len(s.Extensions))
		for key, value := range s.Extensions {
			pbValue, err := structpb.NewValue(cloneAny(value))
			if err != nil {
				continue
			}
			extensions[key] = pbValue
		}
	}
	return &proto.WorkflowEvent{
		Id:              s.ID,
		Source:          s.Source,
		SpecVersion:     s.SpecVersion,
		Type:            s.Type,
		Subject:         s.Subject,
		Time:            timestampPtr(s.Time),
		Datacontenttype: s.Datacontenttype,
		Data:            data,
		Extensions:      extensions,
	}
}

func eventSnapshotFromProto(event *proto.WorkflowEvent) eventSnapshot {
	if event == nil {
		return eventSnapshot{}
	}
	var at *time.Time
	if event.GetTime() != nil && event.GetTime().IsValid() {
		at = timePtr(event.GetTime().AsTime().UTC())
	}
	out := eventSnapshot{
		ID:              event.GetId(),
		Source:          event.GetSource(),
		SpecVersion:     event.GetSpecVersion(),
		Type:            event.GetType(),
		Subject:         event.GetSubject(),
		Time:            at,
		Datacontenttype: event.GetDatacontenttype(),
		Data:            cloneStructMap(event.GetData()),
	}
	if len(event.GetExtensions()) > 0 {
		out.Extensions = make(map[string]any, len(event.GetExtensions()))
		for key, value := range event.GetExtensions() {
			out.Extensions[key] = cloneAny(value.AsInterface())
		}
	}
	return out
}

func (r runSnapshot) toProto() (*proto.BoundWorkflowRun, error) {
	return &proto.BoundWorkflowRun{
		Id:            r.ID,
		Status:        r.Status,
		Target:        r.Target.toProto(),
		Trigger:       r.Trigger.toProto(),
		CreatedAt:     timestamppb.New(r.CreatedAt),
		StartedAt:     timestampPtr(r.StartedAt),
		CompletedAt:   timestampPtr(r.CompletedAt),
		StatusMessage: r.StatusMessage,
		ResultBody:    r.ResultBody,
		CreatedBy:     r.CreatedBy.toProto(),
	}, nil
}

func (r scheduleSnapshot) toProto() (*proto.BoundWorkflowSchedule, error) {
	return &proto.BoundWorkflowSchedule{
		Id:        r.ID,
		Cron:      r.Cron,
		Timezone:  r.Timezone,
		Target:    r.Target.toProto(),
		Paused:    r.Paused,
		CreatedAt: timestamppb.New(r.CreatedAt),
		UpdatedAt: timestamppb.New(r.UpdatedAt),
		NextRunAt: timestampPtr(r.NextRunAt),
		CreatedBy: r.CreatedBy.toProto(),
	}, nil
}

func (r eventTriggerSnapshot) toProto() (*proto.BoundWorkflowEventTrigger, error) {
	return &proto.BoundWorkflowEventTrigger{
		Id: r.ID,
		Match: &proto.WorkflowEventMatch{
			Type:    r.MatchType,
			Source:  r.MatchSource,
			Subject: r.MatchSubject,
		},
		Target:    r.Target.toProto(),
		Paused:    r.Paused,
		CreatedAt: timestamppb.New(r.CreatedAt),
		UpdatedAt: timestamppb.New(r.UpdatedAt),
		CreatedBy: r.CreatedBy.toProto(),
	}, nil
}

func actorSnapshotFromProto(actor *proto.WorkflowActor) *actorSnapshot {
	if actor == nil {
		return nil
	}
	return &actorSnapshot{
		SubjectID:   actor.GetSubjectId(),
		SubjectKind: actor.GetSubjectKind(),
		DisplayName: actor.GetDisplayName(),
		AuthSource:  actor.GetAuthSource(),
	}
}

func cloneActorSnapshot(actor *actorSnapshot) *actorSnapshot {
	if actor == nil {
		return nil
	}
	value := *actor
	return &value
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = cloneAny(item)
	}
	return out
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneAny(item)
		}
		return out
	default:
		return typed
	}
}

func cloneStruct(value *structpb.Struct) *structpb.Struct {
	if value == nil {
		return nil
	}
	cloned, _ := structpb.NewStruct(value.AsMap())
	return cloned
}

func cloneStructMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return cloneMap(value.AsMap())
}

func cloneExtensions(value map[string]*structpb.Value) map[string]*structpb.Value {
	if len(value) == 0 {
		return nil
	}
	out := make(map[string]*structpb.Value, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func timestampPtr(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(value.UTC())
}

func timePtr(value time.Time) *time.Time {
	copied := value.UTC()
	return &copied
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNotFound(err error) bool {
	var target *serviceerror.NotFound
	return errors.As(err, &target)
}

func isWorkflowAlreadyStarted(err error) bool {
	return sdktemporal.IsWorkflowExecutionAlreadyStartedError(err)
}
