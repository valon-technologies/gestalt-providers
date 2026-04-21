package indexeddb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion     = "0.0.1-alpha.1"
	defaultPollInterval = time.Second

	storeSchedules     = "schedules"
	storeEventTriggers = "event_triggers"
	storeRuns          = "runs"
	storeIdempotency   = "idempotency"

	triggerKindManual   = "manual"
	triggerKindSchedule = "schedule"
	triggerKindEvent    = "event"

	columnTypeString = 0
	columnTypeInt    = 1
	columnTypeBool   = 3
	columnTypeTime   = 4
	columnTypeJSON   = 6

	defaultSpecVersion = "1.0"
	defaultTimezone    = "UTC"
)

type config struct {
	PollInterval time.Duration `yaml:"pollInterval"`
}

type Provider struct {
	proto.UnimplementedWorkflowProviderServer

	mu sync.Mutex

	name              string
	cfg               config
	db                *gestalt.IndexedDBClient
	adminConn         *grpc.ClientConn
	admin             proto.IndexedDBClient
	host              *gestalt.WorkflowHostClient
	scheduleStore     *gestalt.ObjectStoreClient
	eventTriggerStore *gestalt.ObjectStoreClient
	runStore          *gestalt.ObjectStoreClient
	idempotencyStore  *gestalt.ObjectStoreClient
	pollCancel        context.CancelFunc
	pollDone          chan struct{}
	wake              chan struct{}

	now func() time.Time
}

type workflowScheduleRecord struct {
	ID           string
	PluginName   string
	Cron         string
	Timezone     string
	Operation    string
	Input        map[string]any
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	NextRunAt    *time.Time
	CreatedBy    *proto.WorkflowActor
	ExecutionRef string
}

type workflowEventTriggerRecord struct {
	ID           string
	PluginName   string
	MatchType    string
	MatchSource  string
	MatchSubject string
	Operation    string
	Input        map[string]any
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CreatedBy    *proto.WorkflowActor
}

type workflowRunRecord struct {
	ID                    string
	PluginName            string
	Status                proto.WorkflowRunStatus
	Operation             string
	Input                 map[string]any
	TriggerKind           string
	TriggerScheduleID     string
	TriggerScheduledFor   *time.Time
	TriggerEventTriggerID string
	TriggerEvent          *proto.WorkflowEvent
	CreatedAt             time.Time
	StartedAt             *time.Time
	CompletedAt           *time.Time
	StatusMessage         string
	ResultBody            string
	CreatedBy             *proto.WorkflowActor
	ExecutionRef          string
}

type workflowIdempotencyRecord struct {
	ID             string
	PluginName     string
	IdempotencyKey string
	RunID          string
	CreatedAt      time.Time
}

type scopedTarget struct {
	PluginName string
	Operation  string
	Input      map[string]any
}

func New() *Provider {
	return &Provider{now: time.Now}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	if err := p.Close(); err != nil {
		return err
	}

	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("indexeddb workflow: %w", err)
	}

	db, err := gestalt.IndexedDB()
	if err != nil {
		return fmt.Errorf("indexeddb workflow: connect indexeddb: %w", err)
	}

	adminConn, admin, err := dialIndexedDBAdmin()
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("indexeddb workflow: connect indexeddb admin: %w", err)
	}

	host, err := gestalt.WorkflowHost()
	if err != nil {
		_ = adminConn.Close()
		_ = db.Close()
		return fmt.Errorf("indexeddb workflow: connect workflow host: %w", err)
	}

	cleanup := func() {
		_ = host.Close()
		_ = adminConn.Close()
		_ = db.Close()
	}

	if err := ensureWorkflowStores(ctx, admin); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: ensure stores: %w", err)
	}

	runStore := db.ObjectStore(storeRuns)
	if err := markStaleRunningRunsFailed(ctx, runStore, p.clock().UTC()); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: recover stale runs: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.db = db
	p.adminConn = adminConn
	p.admin = admin
	p.host = host
	p.scheduleStore = db.ObjectStore(storeSchedules)
	p.eventTriggerStore = db.ObjectStore(storeEventTriggers)
	p.runStore = runStore
	p.idempotencyStore = db.ObjectStore(storeIdempotency)
	p.wake = make(chan struct{}, 1)
	p.pollDone = make(chan struct{})

	loopCtx, cancel := context.WithCancel(context.Background())
	p.pollCancel = cancel
	go p.pollLoop(loopCtx, p.cfg.PollInterval, p.pollDone, p.wake)
	p.signalWorkerLocked()
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if name == "" {
		name = "indexeddb"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindWorkflow,
		Name:        name,
		DisplayName: "IndexedDB Workflow",
		Description: "Workflow provider backed by the IndexedDB primitive.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	p.mu.Lock()
	store := p.runStore
	p.mu.Unlock()
	if store == nil {
		return errors.New("indexeddb workflow: provider is not configured")
	}
	_, err := store.Count(ctx, nil)
	if err != nil {
		return fmt.Errorf("indexeddb workflow: count runs: %w", err)
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	cancel := p.pollCancel
	done := p.pollDone
	host := p.host
	db := p.db
	adminConn := p.adminConn

	p.name = ""
	p.cfg = config{}
	p.db = nil
	p.adminConn = nil
	p.admin = nil
	p.host = nil
	p.scheduleStore = nil
	p.eventTriggerStore = nil
	p.runStore = nil
	p.idempotencyStore = nil
	p.pollCancel = nil
	p.pollDone = nil
	p.wake = nil
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}

	var errs []error
	if host != nil {
		if err := host.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if adminConn != nil {
		if err := adminConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if db != nil {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *Provider) StartRun(ctx context.Context, req *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	target, err := normalizeScopedTarget("", req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	actor := cloneActor(req.GetCreatedBy())
	key := strings.TrimSpace(req.GetIdempotencyKey())

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	if key != "" {
		existing, found, err := loadIdempotencyRecord(ctx, state.idempotencyStore, target.PluginName, key)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotency key: %v", err)
		}
		if found {
			run, found, err := loadRunRecord(ctx, state.runStore, target.PluginName, existing.RunID)
			if err != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load idempotent run: %v", err)
			}
			if found {
				resp, err := run.toProto()
				p.mu.Unlock()
				if err != nil {
					return nil, status.Errorf(codes.Internal, "build run response: %v", err)
				}
				return resp, nil
			}
		}
	}

	now := p.clock().UTC()
	runID := uuid.NewString()
	if key != "" {
		// Deterministic IDs let retries re-adopt the same run if the idempotency
		// mapping write is lost after the run row has already been stored.
		runID = idempotentManualRunID(target.PluginName, key)
		run, found, err := loadRunRecord(ctx, state.runStore, target.PluginName, runID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotent run by deterministic id: %v", err)
		}
		if found {
			_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.PluginName, key, run.ID, now)
			resp, err := run.toProto()
			p.mu.Unlock()
			if err != nil {
				return nil, status.Errorf(codes.Internal, "build run response: %v", err)
			}
			return resp, nil
		}
	}
	run := workflowRunRecord{
		ID:           runID,
		PluginName:   target.PluginName,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Operation:    target.Operation,
		Input:        cloneMap(target.Input),
		TriggerKind:  triggerKindManual,
		CreatedAt:    now,
		CreatedBy:    actor,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
		if key != "" && errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := loadRunRecord(ctx, state.runStore, target.PluginName, run.ID)
			if loadErr != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load existing idempotent run: %v", loadErr)
			}
			if found {
				_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.PluginName, key, existing.ID, now)
				resp, err := existing.toProto()
				p.mu.Unlock()
				if err != nil {
					return nil, status.Errorf(codes.Internal, "build run response: %v", err)
				}
				return resp, nil
			}
		}
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "create run: %v", err)
	}
	if key != "" {
		_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.PluginName, key, run.ID, now)
	}
	resp, err := run.toProto()
	p.signalWorkerLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetRun(ctx context.Context, req *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""
	runID := strings.TrimSpace(req.GetRunId())
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, pluginName, runID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	resp, err := run.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListRuns(ctx context.Context, req *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	runs, err := listRunRecords(ctx, state.runStore, pluginName)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}
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
	pluginName := ""
	runID := strings.TrimSpace(req.GetRunId())
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	reason := strings.TrimSpace(req.GetReason())
	if reason == "" {
		reason = "canceled"
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, pluginName, runID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "workflow run %q is %s; only pending runs can be canceled", runID, run.Status.String())
	}
	now := p.clock().UTC()
	run.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
	run.CompletedAt = &now
	run.StatusMessage = reason
	if err := state.runStore.Put(ctx, run.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "cancel run: %v", err)
	}
	resp, err := run.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) UpsertSchedule(ctx context.Context, req *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeScopedTarget("", req.GetTarget())
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
	requestedBy := cloneActor(req.GetRequestedBy())

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	existing, found, err := loadScheduleRecord(ctx, state.scheduleStore, target.PluginName, scheduleID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found {
		other, otherFound, err := loadScheduleRecord(ctx, state.scheduleStore, "", scheduleID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "check schedule id collision: %v", err)
		}
		if otherFound && other.PluginName != target.PluginName {
			p.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "workflow schedule %q is already owned by plugin %q", scheduleID, other.PluginName)
		}
	}

	now := p.clock().UTC()
	record := workflowScheduleRecord{
		ID:           scheduleID,
		PluginName:   target.PluginName,
		Cron:         cronSpec,
		Timezone:     timezone,
		Operation:    target.Operation,
		Input:        cloneMap(target.Input),
		Paused:       req.GetPaused(),
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = cloneActor(existing.CreatedBy)
	}
	next, err := nextCronTime(parser, cronSpec, location, now)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "invalid cron: %v", err)
	}
	record.NextRunAt = next
	if err := state.scheduleStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "upsert schedule: %v", err)
	}
	resp, err := record.toProto()
	p.signalWorkerLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetSchedule(ctx context.Context, req *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadScheduleRecord(ctx, state.scheduleStore, pluginName, scheduleID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get schedule: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	resp, err := record.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListSchedules(ctx context.Context, req *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listScheduleRecords(ctx, state.scheduleStore, pluginName)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
	}
	resp := &proto.ListWorkflowProviderSchedulesResponse{Schedules: make([]*proto.BoundWorkflowSchedule, 0, len(records))}
	for _, record := range records {
		pbSchedule, err := record.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
		}
		resp.Schedules = append(resp.Schedules, pbSchedule)
	}
	return resp, nil
}

func (p *Provider) DeleteSchedule(ctx context.Context, req *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	_, found, err := loadScheduleRecord(ctx, state.scheduleStore, pluginName, scheduleID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	if err := state.scheduleStore.Delete(ctx, scheduleID); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "delete schedule: %v", err)
	}
	p.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) PauseSchedule(ctx context.Context, req *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, "", strings.TrimSpace(req.GetScheduleId()), true)
}

func (p *Provider) ResumeSchedule(ctx context.Context, req *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, "", strings.TrimSpace(req.GetScheduleId()), false)
}

func (p *Provider) UpsertEventTrigger(ctx context.Context, req *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeScopedTarget("", req.GetTarget())
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
	matchSource := strings.TrimSpace(req.GetMatch().GetSource())
	matchSubject := strings.TrimSpace(req.GetMatch().GetSubject())
	requestedBy := cloneActor(req.GetRequestedBy())

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	existing, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, target.PluginName, triggerID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found {
		other, otherFound, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, "", triggerID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "check event trigger id collision: %v", err)
		}
		if otherFound && other.PluginName != target.PluginName {
			p.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "workflow event trigger %q is already owned by plugin %q", triggerID, other.PluginName)
		}
	}
	now := p.clock().UTC()
	record := workflowEventTriggerRecord{
		ID:           triggerID,
		PluginName:   target.PluginName,
		MatchType:    matchType,
		MatchSource:  matchSource,
		MatchSubject: matchSubject,
		Operation:    target.Operation,
		Input:        cloneMap(target.Input),
		Paused:       req.GetPaused(),
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = cloneActor(existing.CreatedBy)
	}
	if err := state.eventTriggerStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "upsert event trigger: %v", err)
	}
	resp, err := record.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetEventTrigger(ctx context.Context, req *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, pluginName, triggerID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get event trigger: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	resp, err := record.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListEventTriggers(ctx context.Context, req *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listEventTriggerRecords(ctx, state.eventTriggerStore, pluginName)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	resp := &proto.ListWorkflowProviderEventTriggersResponse{Triggers: make([]*proto.BoundWorkflowEventTrigger, 0, len(records))}
	for _, record := range records {
		pbTrigger, err := record.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
		}
		resp.Triggers = append(resp.Triggers, pbTrigger)
	}
	return resp, nil
}

func (p *Provider) DeleteEventTrigger(ctx context.Context, req *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := ""
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	_, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, pluginName, triggerID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	if err := state.eventTriggerStore.Delete(ctx, triggerID); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "delete event trigger: %v", err)
	}
	p.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) PauseEventTrigger(ctx context.Context, req *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, "", strings.TrimSpace(req.GetTriggerId()), true)
}

func (p *Provider) ResumeEventTrigger(ctx context.Context, req *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, "", strings.TrimSpace(req.GetTriggerId()), false)
}

func (p *Provider) PublishEvent(ctx context.Context, req *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	if pluginName == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin_name is required")
	}
	event, err := normalizeWorkflowEvent(req.GetEvent(), p.clock())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	triggers, err := listEventTriggerRecords(ctx, state.eventTriggerStore, pluginName)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	now := p.clock().UTC()
	enqueued := false
	for _, trigger := range triggers {
		if trigger.Paused || !eventMatchesTrigger(event, trigger) {
			continue
		}
		runID := uuid.NewString()
		if strings.TrimSpace(event.GetId()) != "" {
			runID = eventRunID(trigger.ID, event.GetSource(), event.GetId())
		}
		run := workflowRunRecord{
			ID:                    runID,
			PluginName:            pluginName,
			Status:                proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Operation:             trigger.Operation,
			Input:                 cloneMap(trigger.Input),
			TriggerKind:           triggerKindEvent,
			TriggerEventTriggerID: trigger.ID,
			TriggerEvent:          cloneEvent(event),
			CreatedAt:             now,
			CreatedBy:             cloneActor(trigger.CreatedBy),
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			if errors.Is(err, gestalt.ErrAlreadyExists) {
				continue
			}
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "enqueue workflow run: %v", err)
		}
		enqueued = true
	}
	if enqueued {
		p.signalWorkerLocked()
	}
	p.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) updateSchedulePaused(ctx context.Context, pluginName, scheduleID string, paused bool) (*proto.BoundWorkflowSchedule, error) {
	if scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadScheduleRecord(ctx, state.scheduleStore, pluginName, scheduleID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	record.Paused = paused
	record.UpdatedAt = p.clock().UTC()
	if !paused {
		location, _, err := parseTimezone(record.Timezone)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "parse schedule timezone: %v", err)
		}
		next, err := nextCronTime(cronParser(), record.Cron, location, record.UpdatedAt)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "parse schedule cron: %v", err)
		}
		record.NextRunAt = next
		p.signalWorkerLocked()
	}
	if err := state.scheduleStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "update schedule: %v", err)
	}
	resp, err := record.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) updateEventTriggerPaused(ctx context.Context, pluginName, triggerID string, paused bool) (*proto.BoundWorkflowEventTrigger, error) {
	if triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, pluginName, triggerID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	record.Paused = paused
	record.UpdatedAt = p.clock().UTC()
	if err := state.eventTriggerStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "update event trigger: %v", err)
	}
	resp, err := record.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
}

func (p *Provider) pollLoop(ctx context.Context, pollInterval time.Duration, done chan struct{}, wake <-chan struct{}) {
	defer close(done)
	_ = p.tick(ctx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		if ctx.Err() != nil {
			return
		}
		_ = p.tick(ctx)
	}
}

func (p *Provider) tick(ctx context.Context) error {
	if err := p.enqueueDueSchedules(ctx); err != nil {
		return err
	}
	for {
		processed, err := p.processNextPendingRun(ctx)
		if err != nil {
			return err
		}
		if !processed {
			return nil
		}
	}
}

func (p *Provider) enqueueDueSchedules(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, err := p.requireConfiguredLocked()
	if err != nil {
		return err
	}
	records, err := listScheduleRecords(ctx, state.scheduleStore, "")
	if err != nil {
		return err
	}

	now := p.clock().UTC()
	parser := cronParser()
	enqueued := false

	for _, schedule := range records {
		if schedule.Paused || schedule.NextRunAt == nil || schedule.NextRunAt.After(now) {
			continue
		}
		location, _, err := parseTimezone(schedule.Timezone)
		if err != nil {
			return fmt.Errorf("schedule %q timezone: %w", schedule.ID, err)
		}
		latestDue, nextRun, err := collapseCron(parser, schedule.Cron, location, *schedule.NextRunAt, now)
		if err != nil {
			return fmt.Errorf("schedule %q cron: %w", schedule.ID, err)
		}
		run := workflowRunRecord{
			ID:                  scheduleRunID(schedule.ID, latestDue),
			PluginName:          schedule.PluginName,
			Status:              proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Operation:           schedule.Operation,
			Input:               cloneMap(schedule.Input),
			TriggerKind:         triggerKindSchedule,
			TriggerScheduleID:   schedule.ID,
			TriggerScheduledFor: timePtr(latestDue),
			CreatedAt:           now,
			CreatedBy:           cloneActor(schedule.CreatedBy),
			ExecutionRef:        schedule.ExecutionRef,
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			if !errors.Is(err, gestalt.ErrAlreadyExists) {
				return fmt.Errorf("schedule %q enqueue: %w", schedule.ID, err)
			}
		}
		schedule.NextRunAt = timePtr(nextRun)
		schedule.UpdatedAt = now
		if err := state.scheduleStore.Put(ctx, schedule.toRecord()); err != nil {
			return fmt.Errorf("schedule %q advance: %w", schedule.ID, err)
		}
		enqueued = true
	}
	if enqueued {
		p.signalWorkerLocked()
	}
	return nil
}

func (p *Provider) processNextPendingRun(ctx context.Context) (bool, error) {
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return false, err
	}
	pending, found, err := nextPendingRun(ctx, state.runStore)
	if err != nil {
		p.mu.Unlock()
		return false, err
	}
	if !found {
		p.mu.Unlock()
		return false, nil
	}

	now := p.clock().UTC()
	pending.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	pending.StartedAt = &now
	pending.StatusMessage = ""
	if err := state.runStore.Put(ctx, pending.toRecord()); err != nil {
		p.mu.Unlock()
		return false, err
	}
	host := state.host
	p.mu.Unlock()

	resp, invokeErr := host.InvokeOperation(ctx, &proto.InvokeWorkflowOperationRequest{
		Target:       pending.targetProto(),
		RunId:        pending.ID,
		Trigger:      pending.triggerProto(),
		CreatedBy:    cloneActor(pending.CreatedBy),
		ExecutionRef: pending.ExecutionRef,
	})

	p.mu.Lock()
	defer p.mu.Unlock()
	state, err = p.requireConfiguredLocked()
	if err != nil {
		return true, err
	}
	current, found, err := loadRunRecord(ctx, state.runStore, pending.PluginName, pending.ID)
	if err != nil {
		return true, err
	}
	if !found {
		return true, nil
	}

	completedAt := p.clock().UTC()
	current.CompletedAt = &completedAt
	if invokeErr != nil {
		current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
		current.StatusMessage = invokeErr.Error()
	} else {
		current.ResultBody = resp.GetBody()
		if resp.GetStatus() >= http.StatusBadRequest {
			current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			current.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
		} else {
			current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			current.StatusMessage = ""
		}
	}
	if err := state.runStore.Put(ctx, current.toRecord()); err != nil {
		return true, err
	}
	return true, nil
}

func (p *Provider) requireConfiguredLocked() (*configuredState, error) {
	if p.runStore == nil || p.scheduleStore == nil || p.eventTriggerStore == nil || p.idempotencyStore == nil || p.host == nil {
		return nil, errors.New("indexeddb workflow: provider is not configured")
	}
	return &configuredState{
		host:              p.host,
		scheduleStore:     p.scheduleStore,
		eventTriggerStore: p.eventTriggerStore,
		runStore:          p.runStore,
		idempotencyStore:  p.idempotencyStore,
	}, nil
}

func (p *Provider) signalWorkerLocked() {
	if p.wake == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Provider) clock() time.Time {
	if p.now == nil {
		return time.Now()
	}
	return p.now()
}

type configuredState struct {
	host              *gestalt.WorkflowHostClient
	scheduleStore     *gestalt.ObjectStoreClient
	eventTriggerStore *gestalt.ObjectStoreClient
	runStore          *gestalt.ObjectStoreClient
	idempotencyStore  *gestalt.ObjectStoreClient
}

func decodeConfig(raw map[string]any) (config, error) {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return config{}, fmt.Errorf("marshal config: %w", err)
	}
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	return cfg, nil
}

func dialIndexedDBAdmin() (*grpc.ClientConn, proto.IndexedDBClient, error) {
	socketPath := os.Getenv(gestalt.EnvIndexedDBSocket)
	if socketPath == "" {
		return nil, nil, fmt.Errorf("%s is not set", gestalt.EnvIndexedDBSocket)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "unix:"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, proto.NewIndexedDBClient(conn), nil
}

func ensureWorkflowStores(ctx context.Context, admin proto.IndexedDBClient) error {
	for _, def := range workflowStoreSchemas() {
		_, err := admin.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
			Name:   def.name,
			Schema: def.schema,
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			return err
		}
	}
	return nil
}

type storeSchemaDef struct {
	name   string
	schema *proto.ObjectStoreSchema
}

func workflowStoreSchemas() []storeSchemaDef {
	return []storeSchemaDef{
		{
			name:   storeSchedules,
			schema: &proto.ObjectStoreSchema{},
		},
		{
			name:   storeEventTriggers,
			schema: &proto.ObjectStoreSchema{},
		},
		{
			name:   storeRuns,
			schema: &proto.ObjectStoreSchema{},
		},
		{
			name:   storeIdempotency,
			schema: &proto.ObjectStoreSchema{},
		},
	}
}

func markStaleRunningRunsFailed(ctx context.Context, store *gestalt.ObjectStoreClient, now time.Time) error {
	runs, err := store.GetAll(ctx, nil)
	if err != nil {
		return err
	}
	for _, record := range runs {
		run, err := runRecordFromRecord(record)
		if err != nil {
			return err
		}
		if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
			continue
		}
		run.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
		run.CompletedAt = &now
		run.StatusMessage = "workflow provider restarted while run was in progress"
		if err := store.Put(ctx, run.toRecord()); err != nil {
			return err
		}
	}
	return nil
}

func normalizeScopedTarget(pluginName string, target *proto.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	pluginName = strings.TrimSpace(pluginName)
	targetPlugin := strings.TrimSpace(target.GetPluginName())
	if pluginName == "" {
		pluginName = targetPlugin
	}
	if pluginName == "" {
		return scopedTarget{}, errors.New("plugin_name is required")
	}
	if targetPlugin != "" && targetPlugin != pluginName {
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
	return timePtr(next), nil
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

func nextPendingRun(ctx context.Context, store *gestalt.ObjectStoreClient) (workflowRunRecord, bool, error) {
	runs, err := listRunRecords(ctx, store, "")
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	for _, run := range runs {
		if run.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			return run, true, nil
		}
	}
	return workflowRunRecord{}, false, nil
}

func loadScheduleRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, scheduleID string) (workflowScheduleRecord, bool, error) {
	record, err := store.Get(ctx, scheduleID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowScheduleRecord{}, false, nil
		}
		return workflowScheduleRecord{}, false, err
	}
	schedule, err := scheduleRecordFromRecord(record)
	if err != nil {
		return workflowScheduleRecord{}, false, err
	}
	if pluginName != "" && schedule.PluginName != pluginName {
		return workflowScheduleRecord{}, false, nil
	}
	return schedule, true, nil
}

func listScheduleRecords(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName string) ([]workflowScheduleRecord, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]workflowScheduleRecord, 0, len(records))
	for _, record := range records {
		schedule, err := scheduleRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if pluginName != "" && schedule.PluginName != pluginName {
			continue
		}
		out = append(out, schedule)
	}
	slices.SortFunc(out, func(a, b workflowScheduleRecord) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func loadEventTriggerRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, triggerID string) (workflowEventTriggerRecord, bool, error) {
	record, err := store.Get(ctx, triggerID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowEventTriggerRecord{}, false, nil
		}
		return workflowEventTriggerRecord{}, false, err
	}
	trigger, err := eventTriggerRecordFromRecord(record)
	if err != nil {
		return workflowEventTriggerRecord{}, false, err
	}
	if pluginName != "" && trigger.PluginName != pluginName {
		return workflowEventTriggerRecord{}, false, nil
	}
	return trigger, true, nil
}

func listEventTriggerRecords(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName string) ([]workflowEventTriggerRecord, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]workflowEventTriggerRecord, 0, len(records))
	for _, record := range records {
		trigger, err := eventTriggerRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if pluginName != "" && trigger.PluginName != pluginName {
			continue
		}
		out = append(out, trigger)
	}
	slices.SortFunc(out, func(a, b workflowEventTriggerRecord) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func loadRunRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, runID string) (workflowRunRecord, bool, error) {
	record, err := store.Get(ctx, runID)
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	run, err := runRecordFromRecord(record)
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	if pluginName != "" && run.PluginName != pluginName {
		return workflowRunRecord{}, false, nil
	}
	return run, true, nil
}

func listRunRecords(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName string) ([]workflowRunRecord, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]workflowRunRecord, 0, len(records))
	for _, record := range records {
		run, err := runRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if pluginName != "" && run.PluginName != pluginName {
			continue
		}
		out = append(out, run)
	}
	slices.SortFunc(out, func(a, b workflowRunRecord) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func loadIdempotencyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, key string) (workflowIdempotencyRecord, bool, error) {
	record, err := store.Get(ctx, idempotencyID(pluginName, key))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowIdempotencyRecord{}, false, nil
		}
		return workflowIdempotencyRecord{}, false, err
	}
	value, err := idempotencyRecordFromRecord(record)
	if err != nil {
		return workflowIdempotencyRecord{}, false, err
	}
	if value.PluginName != pluginName {
		return workflowIdempotencyRecord{}, false, nil
	}
	return value, true, nil
}

func storeIdempotencyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, key, runID string, createdAt time.Time) error {
	record := workflowIdempotencyRecord{
		ID:             idempotencyID(pluginName, key),
		PluginName:     pluginName,
		IdempotencyKey: key,
		RunID:          runID,
		CreatedAt:      createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func eventMatchesTrigger(event *proto.WorkflowEvent, trigger workflowEventTriggerRecord) bool {
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

func idempotencyID(pluginName, key string) string {
	return pluginName + ":" + key
}

func idempotentManualRunID(pluginName, key string) string {
	sum := sha256.Sum256([]byte(pluginName + "\x00" + key))
	return "manual:" + hex.EncodeToString(sum[:16])
}

func eventRunID(triggerID, eventSource, eventID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(triggerID) + "\x00" + strings.TrimSpace(eventSource) + "\x00" + strings.TrimSpace(eventID)))
	return "event:" + hex.EncodeToString(sum[:16])
}

func scheduleRunID(scheduleID string, scheduledFor time.Time) string {
	return "schedule:" + scheduleID + ":" + scheduledFor.UTC().Format(time.RFC3339Nano)
}

func cloneStructMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	return cloneMap(value.AsMap())
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	out := maps.Clone(value)
	for key, item := range out {
		if nested, ok := item.(map[string]any); ok {
			out[key] = cloneMap(nested)
		}
	}
	return out
}

func cloneStruct(value *structpb.Struct) *structpb.Struct {
	if value == nil {
		return nil
	}
	cloned, _ := structpb.NewStruct(value.AsMap())
	return cloned
}

func cloneActor(actor *proto.WorkflowActor) *proto.WorkflowActor {
	if actor == nil {
		return nil
	}
	return &proto.WorkflowActor{
		SubjectId:   actor.GetSubjectId(),
		SubjectKind: actor.GetSubjectKind(),
		DisplayName: actor.GetDisplayName(),
		AuthSource:  actor.GetAuthSource(),
	}
}

func cloneEvent(event *proto.WorkflowEvent) *proto.WorkflowEvent {
	if event == nil {
		return nil
	}
	return &proto.WorkflowEvent{
		Id:              event.GetId(),
		Source:          event.GetSource(),
		SpecVersion:     event.GetSpecVersion(),
		Type:            event.GetType(),
		Subject:         event.GetSubject(),
		Time:            cloneTimestamp(event.GetTime()),
		Datacontenttype: event.GetDatacontenttype(),
		Data:            cloneStruct(event.GetData()),
		Extensions:      cloneExtensions(event.GetExtensions()),
	}
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return timestamppb.New(ts.AsTime())
}

func cloneExtensions(values map[string]*structpb.Value) map[string]*structpb.Value {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]*structpb.Value, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func timePtr(value time.Time) *time.Time {
	ts := value.UTC()
	return &ts
}

func actorToMap(actor *proto.WorkflowActor) map[string]any {
	if actor == nil {
		return nil
	}
	return map[string]any{
		"subject_id":   actor.GetSubjectId(),
		"subject_kind": actor.GetSubjectKind(),
		"display_name": actor.GetDisplayName(),
		"auth_source":  actor.GetAuthSource(),
	}
}

func actorFromAny(value any) *proto.WorkflowActor {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	return &proto.WorkflowActor{
		SubjectId:   stringField(data, "subject_id"),
		SubjectKind: stringField(data, "subject_kind"),
		DisplayName: stringField(data, "display_name"),
		AuthSource:  stringField(data, "auth_source"),
	}
}

func eventToMap(event *proto.WorkflowEvent) map[string]any {
	if event == nil {
		return nil
	}
	value := map[string]any{
		"id":              event.GetId(),
		"source":          event.GetSource(),
		"spec_version":    event.GetSpecVersion(),
		"type":            event.GetType(),
		"subject":         event.GetSubject(),
		"datacontenttype": event.GetDatacontenttype(),
		"data":            cloneStructMap(event.GetData()),
		"extensions":      extensionsToMap(event.GetExtensions()),
	}
	if ts := event.GetTime(); ts != nil && ts.IsValid() {
		value["time"] = ts.AsTime().UTC().Format(time.RFC3339Nano)
	}
	return value
}

func eventFromAny(value any) *proto.WorkflowEvent {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	event := &proto.WorkflowEvent{
		Id:              stringField(data, "id"),
		Source:          stringField(data, "source"),
		SpecVersion:     stringField(data, "spec_version"),
		Type:            stringField(data, "type"),
		Subject:         stringField(data, "subject"),
		Datacontenttype: stringField(data, "datacontenttype"),
		Data:            structFromAny(data["data"]),
		Extensions:      mapToExtensions(anyMap(data["extensions"])),
	}
	if raw := stringField(data, "time"); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			event.Time = timestamppb.New(parsed.UTC())
		}
	}
	return event
}

func structFromAny(value any) *structpb.Struct {
	if value == nil {
		return nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	out, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return out
}

func extensionsToMap(values map[string]*structpb.Value) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		if value == nil {
			out[key] = nil
			continue
		}
		out[key] = value.AsInterface()
	}
	return out
}

func mapToExtensions(values map[string]any) map[string]*structpb.Value {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]*structpb.Value, len(values))
	for key, value := range values {
		pbValue, err := structpb.NewValue(value)
		if err != nil {
			continue
		}
		out[key] = pbValue
	}
	return out
}

func anyMap(value any) map[string]any {
	out, _ := value.(map[string]any)
	return out
}

func stringField(value map[string]any, key string) string {
	raw, ok := value[key]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func boolField(value map[string]any, key string) bool {
	raw, ok := value[key]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}

func intField(value map[string]any, key string) int64 {
	raw, ok := value[key]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func timeField(value map[string]any, key string) *time.Time {
	raw, ok := value[key]
	if !ok || raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case time.Time:
		return timePtr(v)
	default:
		return nil
	}
}

func (r workflowScheduleRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":            r.ID,
		"plugin_name":   r.PluginName,
		"cron":          r.Cron,
		"timezone":      r.Timezone,
		"operation":     r.Operation,
		"input":         cloneMap(r.Input),
		"paused":        r.Paused,
		"created_at":    r.CreatedAt.UTC(),
		"updated_at":    r.UpdatedAt.UTC(),
		"created_by":    actorToMap(r.CreatedBy),
		"execution_ref": r.ExecutionRef,
	}
	if r.NextRunAt != nil {
		record["next_run_at"] = r.NextRunAt.UTC()
	} else {
		record["next_run_at"] = nil
	}
	return record
}

func scheduleRecordFromRecord(record gestalt.Record) (workflowScheduleRecord, error) {
	value := map[string]any(record)
	out := workflowScheduleRecord{
		ID:           stringField(value, "id"),
		PluginName:   stringField(value, "plugin_name"),
		Cron:         stringField(value, "cron"),
		Timezone:     stringField(value, "timezone"),
		Operation:    stringField(value, "operation"),
		Input:        anyMap(value["input"]),
		Paused:       boolField(value, "paused"),
		CreatedBy:    actorFromAny(value["created_by"]),
		ExecutionRef: stringField(value, "execution_ref"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := timeField(value, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	out.NextRunAt = timeField(value, "next_run_at")
	return out, nil
}

func (r workflowScheduleRecord) toProto() (*proto.BoundWorkflowSchedule, error) {
	return &proto.BoundWorkflowSchedule{
		Id:       r.ID,
		Cron:     r.Cron,
		Timezone: r.Timezone,
		Target: &proto.BoundWorkflowTarget{
			PluginName: r.PluginName,
			Operation:  r.Operation,
			Input:      structFromAny(r.Input),
		},
		Paused:       r.Paused,
		CreatedAt:    timestamppb.New(r.CreatedAt),
		UpdatedAt:    timestamppb.New(r.UpdatedAt),
		NextRunAt:    timeToProto(r.NextRunAt),
		CreatedBy:    cloneActor(r.CreatedBy),
		ExecutionRef: r.ExecutionRef,
	}, nil
}

func (r workflowEventTriggerRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":            r.ID,
		"plugin_name":   r.PluginName,
		"match_type":    r.MatchType,
		"match_source":  r.MatchSource,
		"match_subject": r.MatchSubject,
		"operation":     r.Operation,
		"input":         cloneMap(r.Input),
		"paused":        r.Paused,
		"created_at":    r.CreatedAt.UTC(),
		"updated_at":    r.UpdatedAt.UTC(),
		"created_by":    actorToMap(r.CreatedBy),
	}
}

func eventTriggerRecordFromRecord(record gestalt.Record) (workflowEventTriggerRecord, error) {
	value := map[string]any(record)
	out := workflowEventTriggerRecord{
		ID:           stringField(value, "id"),
		PluginName:   stringField(value, "plugin_name"),
		MatchType:    stringField(value, "match_type"),
		MatchSource:  stringField(value, "match_source"),
		MatchSubject: stringField(value, "match_subject"),
		Operation:    stringField(value, "operation"),
		Input:        anyMap(value["input"]),
		Paused:       boolField(value, "paused"),
		CreatedBy:    actorFromAny(value["created_by"]),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := timeField(value, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	return out, nil
}

func (r workflowEventTriggerRecord) toProto() (*proto.BoundWorkflowEventTrigger, error) {
	return &proto.BoundWorkflowEventTrigger{
		Id: r.ID,
		Match: &proto.WorkflowEventMatch{
			Type:    r.MatchType,
			Source:  r.MatchSource,
			Subject: r.MatchSubject,
		},
		Target: &proto.BoundWorkflowTarget{
			PluginName: r.PluginName,
			Operation:  r.Operation,
			Input:      structFromAny(r.Input),
		},
		Paused:    r.Paused,
		CreatedAt: timestamppb.New(r.CreatedAt),
		UpdatedAt: timestamppb.New(r.UpdatedAt),
		CreatedBy: cloneActor(r.CreatedBy),
	}, nil
}

func (r workflowRunRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":                       r.ID,
		"plugin_name":              r.PluginName,
		"status":                   int64(r.Status),
		"operation":                r.Operation,
		"input":                    cloneMap(r.Input),
		"trigger_kind":             r.TriggerKind,
		"trigger_schedule_id":      r.TriggerScheduleID,
		"trigger_event_trigger_id": r.TriggerEventTriggerID,
		"trigger_event":            eventToMap(r.TriggerEvent),
		"created_at":               r.CreatedAt.UTC(),
		"status_message":           r.StatusMessage,
		"result_body":              r.ResultBody,
		"created_by":               actorToMap(r.CreatedBy),
		"execution_ref":            r.ExecutionRef,
	}
	if r.TriggerScheduledFor != nil {
		record["trigger_scheduled_for"] = r.TriggerScheduledFor.UTC()
	} else {
		record["trigger_scheduled_for"] = nil
	}
	if r.StartedAt != nil {
		record["started_at"] = r.StartedAt.UTC()
	} else {
		record["started_at"] = nil
	}
	if r.CompletedAt != nil {
		record["completed_at"] = r.CompletedAt.UTC()
	} else {
		record["completed_at"] = nil
	}
	return record
}

func runRecordFromRecord(record gestalt.Record) (workflowRunRecord, error) {
	value := map[string]any(record)
	out := workflowRunRecord{
		ID:                    stringField(value, "id"),
		PluginName:            stringField(value, "plugin_name"),
		Status:                proto.WorkflowRunStatus(intField(value, "status")),
		Operation:             stringField(value, "operation"),
		Input:                 anyMap(value["input"]),
		TriggerKind:           stringField(value, "trigger_kind"),
		TriggerScheduleID:     stringField(value, "trigger_schedule_id"),
		TriggerEventTriggerID: stringField(value, "trigger_event_trigger_id"),
		TriggerEvent:          eventFromAny(value["trigger_event"]),
		StatusMessage:         stringField(value, "status_message"),
		ResultBody:            stringField(value, "result_body"),
		CreatedBy:             actorFromAny(value["created_by"]),
		ExecutionRef:          stringField(value, "execution_ref"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	out.TriggerScheduledFor = timeField(value, "trigger_scheduled_for")
	out.StartedAt = timeField(value, "started_at")
	out.CompletedAt = timeField(value, "completed_at")
	return out, nil
}

func (r workflowRunRecord) toProto() (*proto.BoundWorkflowRun, error) {
	return &proto.BoundWorkflowRun{
		Id:            r.ID,
		Status:        r.Status,
		Target:        r.targetProto(),
		Trigger:       r.triggerProto(),
		CreatedAt:     timestamppb.New(r.CreatedAt),
		StartedAt:     timeToProto(r.StartedAt),
		CompletedAt:   timeToProto(r.CompletedAt),
		StatusMessage: r.StatusMessage,
		ResultBody:    r.ResultBody,
		CreatedBy:     cloneActor(r.CreatedBy),
		ExecutionRef:  r.ExecutionRef,
	}, nil
}

func (r workflowRunRecord) targetProto() *proto.BoundWorkflowTarget {
	return &proto.BoundWorkflowTarget{
		PluginName: r.PluginName,
		Operation:  r.Operation,
		Input:      structFromAny(r.Input),
	}
}

func (r workflowRunRecord) triggerProto() *proto.WorkflowRunTrigger {
	switch r.TriggerKind {
	case triggerKindSchedule:
		return &proto.WorkflowRunTrigger{
			Kind: &proto.WorkflowRunTrigger_Schedule{
				Schedule: &proto.WorkflowScheduleTrigger{
					ScheduleId:   r.TriggerScheduleID,
					ScheduledFor: timeToProto(r.TriggerScheduledFor),
				},
			},
		}
	case triggerKindEvent:
		return &proto.WorkflowRunTrigger{
			Kind: &proto.WorkflowRunTrigger_Event{
				Event: &proto.WorkflowEventTriggerInvocation{
					TriggerId: r.TriggerEventTriggerID,
					Event:     cloneEvent(r.TriggerEvent),
				},
			},
		}
	default:
		return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Manual{Manual: &proto.WorkflowManualTrigger{}}}
	}
}

func (r workflowIdempotencyRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":              r.ID,
		"plugin_name":     r.PluginName,
		"idempotency_key": r.IdempotencyKey,
		"run_id":          r.RunID,
		"created_at":      r.CreatedAt.UTC(),
	}
}

func idempotencyRecordFromRecord(record gestalt.Record) (workflowIdempotencyRecord, error) {
	value := map[string]any(record)
	out := workflowIdempotencyRecord{
		ID:             stringField(value, "id"),
		PluginName:     stringField(value, "plugin_name"),
		IdempotencyKey: stringField(value, "idempotency_key"),
		RunID:          stringField(value, "run_id"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	return out, nil
}

func timeToProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(value.UTC())
}

var _ gestalt.WorkflowProvider = (*Provider)(nil)
