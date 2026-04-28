package indexeddb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion     = "0.0.1-alpha.12"
	defaultPollInterval = time.Second

	storeSchedules     = "schedules"
	storeEventTriggers = "event_triggers"
	storeRuns          = "runs"
	storeIdempotency   = "idempotency"
	storeExecutionRefs = "execution_refs"
	storeWorkflowKeys  = "workflow_keys"
	storeSignals       = "workflow_signals"

	triggerKindManual   = "manual"
	triggerKindSchedule = "schedule"
	triggerKindEvent    = "event"

	gestaltInputKey              = "_gestalt"
	eventRunPermissionsKey       = "eventRunPermissions"
	configManagedWorkflowSubject = "system:config"
	configManagedWorkflowAuth    = "config"
	configManagedWorkflowKind    = "system"

	signalStatePending   = "pending"
	signalStateClaimed   = "claimed"
	signalStateDelivered = "delivered"
	signalStateFailed    = "failed"

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
	executionRefStore *gestalt.ObjectStoreClient
	workflowKeyStore  *gestalt.ObjectStoreClient
	signalStore       *gestalt.ObjectStoreClient
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
	Connection   string
	Instance     string
	Input        map[string]any
	Target       *proto.BoundWorkflowTarget
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
	Connection   string
	Instance     string
	Input        map[string]any
	Target       *proto.BoundWorkflowTarget
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CreatedBy    *proto.WorkflowActor
	ExecutionRef string
}

type workflowRunRecord struct {
	ID                    string
	PluginName            string
	Status                proto.WorkflowRunStatus
	Operation             string
	Connection            string
	Instance              string
	Input                 map[string]any
	Target                *proto.BoundWorkflowTarget
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
	WorkflowKey           string
}

type workflowIdempotencyRecord struct {
	ID             string
	PluginName     string
	IdempotencyKey string
	RunID          string
	SignalID       string
	WorkflowKey    string
	StartedRun     bool
	CreatedAt      time.Time
}

type workflowKeyRecord struct {
	ID                string
	WorkflowKey       string
	RunID             string
	TargetFingerprint string
	CreatedAt         time.Time
}

type workflowSignalRecord struct {
	ID             string
	RunID          string
	WorkflowKey    string
	State          string
	Signal         *proto.WorkflowSignal
	IdempotencyKey string
	Sequence       int64
	StartedRun     bool
	BatchID        string
	CreatedAt      time.Time
	ClaimedAt      *time.Time
	DeliveredAt    *time.Time
	FailedAt       *time.Time
	StatusMessage  string
}

type workflowExecutionReferenceRecord struct {
	ID                  string
	ProviderName        string
	TargetPlugin        string
	TargetOperation     string
	TargetConnection    string
	TargetInstance      string
	Target              *proto.BoundWorkflowTarget
	TargetFingerprint   string
	SubjectID           string
	SubjectKind         string
	DisplayName         string
	AuthSource          string
	CredentialSubjectID string
	PermissionsJSON     string
	CreatedAt           time.Time
	RevokedAt           *time.Time
	CallerPluginName    string
}

type scopedTarget struct {
	PluginName string
	Operation  string
	Connection string
	Instance   string
	Input      map[string]any
	Target     *proto.BoundWorkflowTarget
}

type workflowPluginTargetFields struct {
	PluginName string
	Operation  string
	Connection string
	Instance   string
	Input      *structpb.Struct
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
	workflowKeyStore := db.ObjectStore(storeWorkflowKeys)
	signalStore := db.ObjectStore(storeSignals)
	if err := markStaleRunningRunsFailed(ctx, runStore, workflowKeyStore, signalStore, p.clock().UTC()); err != nil {
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
	p.executionRefStore = db.ObjectStore(storeExecutionRefs)
	p.workflowKeyStore = workflowKeyStore
	p.signalStore = signalStore
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
	p.executionRefStore = nil
	p.workflowKeyStore = nil
	p.signalStore = nil
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
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
	targetFingerprint := ""
	if workflowKey != "" {
		targetFingerprint, err = workflowTargetFingerprint(target.Target)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "target fingerprint: %v", err)
		}
	}

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
	if workflowKey != "" {
		active, found, err := activeWorkflowKeyRun(ctx, state, workflowKey)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load workflow key: %v", err)
		}
		if found && workflowRunTerminal(active.Status) {
			_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey)
			found = false
		}
		if found {
			p.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run", workflowKey)
		}
	}
	run := workflowRunRecord{
		ID:           runID,
		PluginName:   target.PluginName,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Operation:    target.Operation,
		Connection:   target.Connection,
		Instance:     target.Instance,
		Input:        cloneMap(target.Input),
		Target:       cloneTarget(target.Target),
		TriggerKind:  triggerKindManual,
		CreatedAt:    now,
		CreatedBy:    actor,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
		WorkflowKey:  workflowKey,
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
	if workflowKey != "" {
		if err := storeWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey, run.ID, targetFingerprint, now); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "store workflow key: %v", err)
		}
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
	if err := markRunSignalsFailed(ctx, state.signalStore, run.ID, nil, now, reason); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "cancel run signals: %v", err)
	}
	if err := state.runStore.Put(ctx, run.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "cancel run: %v", err)
	}
	if run.WorkflowKey != "" {
		_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, run.WorkflowKey)
	}
	resp, err := run.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) SignalRun(ctx context.Context, req *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.GetRunId())
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	now := p.clock().UTC()
	signal, err := normalizeWorkflowSignal(req.GetSignal(), now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, "", runID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	if workflowRunTerminal(run.Status) {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "workflow run %q is %s", runID, run.Status.String())
	}
	if key := strings.TrimSpace(signal.GetIdempotencyKey()); key != "" {
		existing, found, err := loadIdempotencyRecord(ctx, state.idempotencyStore, run.PluginName, key)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load signal idempotency key: %v", err)
		}
		if found && existing.SignalID != "" {
			resp, err := p.signalIdempotencyResponseLocked(ctx, state, existing)
			p.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
	}
	resp, err := p.enqueueSignalLocked(ctx, state, run, signal, false)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.signalWorkerLocked()
	p.mu.Unlock()
	return resp, nil
}

func (p *Provider) SignalOrStartRun(ctx context.Context, req *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	target, err := normalizeScopedTarget("", req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targetFingerprint, err := workflowTargetFingerprint(target.Target)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "target fingerprint: %v", err)
	}
	now := p.clock().UTC()
	signal, err := normalizeWorkflowSignal(req.GetSignal(), now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if key := strings.TrimSpace(signal.GetIdempotencyKey()); key != "" {
		existing, found, err := loadIdempotencyRecord(ctx, state.idempotencyStore, target.PluginName, key)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load signal idempotency key: %v", err)
		}
		if found && existing.SignalID != "" {
			resp, err := p.signalIdempotencyResponseLocked(ctx, state, existing)
			p.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
	}

	startedRun := false
	run, active, err := activeWorkflowKeyRun(ctx, state, workflowKey)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load workflow key: %v", err)
	}
	if active && workflowRunTerminal(run.Status) {
		_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey)
		active = false
	}
	if active {
		// The existing keyed run owns the execution ref and target. Later
		// signals deliberately do not replace that context, even if the caller's
		// current config would build a different target.
	} else {
		startedRun = true
		run = workflowRunRecord{
			ID:           uuid.NewString(),
			PluginName:   target.PluginName,
			Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Operation:    target.Operation,
			Connection:   target.Connection,
			Instance:     target.Instance,
			Input:        cloneMap(target.Input),
			Target:       cloneTarget(target.Target),
			TriggerKind:  triggerKindManual,
			CreatedAt:    now,
			CreatedBy:    cloneActor(req.GetCreatedBy()),
			ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
			WorkflowKey:  workflowKey,
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "create workflow run: %v", err)
		}
		if err := storeWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey, run.ID, targetFingerprint, now); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "store workflow key: %v", err)
		}
	}
	resp, err := p.enqueueSignalLocked(ctx, state, run, signal, startedRun)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.signalWorkerLocked()
	p.mu.Unlock()
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
		Connection:   target.Connection,
		Instance:     target.Instance,
		Input:        cloneMap(target.Input),
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = createdByForUpsert(existing.CreatedBy, requestedBy)
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
		Connection:   target.Connection,
		Instance:     target.Instance,
		Input:        cloneMap(target.Input),
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = createdByForUpsert(existing.CreatedBy, requestedBy)
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
	providerName := strings.TrimSpace(p.name)
	publishedBy := cloneActor(req.GetPublishedBy())
	enqueued := false
	for _, trigger := range triggers {
		if trigger.Paused || !eventMatchesTrigger(event, trigger) {
			continue
		}
		runID := uuid.NewString()
		if strings.TrimSpace(event.GetId()) != "" {
			runID = eventRunID(trigger.ID, event.GetSource(), event.GetId())
		}
		if _, found, err := loadRunRecord(ctx, state.runStore, trigger.PluginName, runID); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load event run: %v", err)
		} else if found {
			continue
		}
		createdBy := cloneActor(trigger.CreatedBy)
		executionRef := trigger.ExecutionRef
		createdExecutionRef := false
		if actorHasSubject(publishedBy) {
			createdBy = cloneActor(publishedBy)
			ref, err := publishedEventExecutionReference(providerName, runID, trigger, publishedBy, now)
			if err != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				record, err := executionReferenceRecordFromProto(ref)
				if err != nil {
					p.mu.Unlock()
					return nil, status.Errorf(codes.Internal, "build event execution reference record: %v", err)
				}
				if err := state.executionRefStore.Add(ctx, record.toRecord()); err != nil {
					if !errors.Is(err, gestalt.ErrAlreadyExists) {
						p.mu.Unlock()
						return nil, status.Errorf(codes.Internal, "store event execution reference: %v", err)
					}
				} else {
					createdExecutionRef = true
				}
				executionRef = ref.GetId()
			}
		}
		run := workflowRunRecord{
			ID:                    runID,
			PluginName:            trigger.PluginName,
			Status:                proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Operation:             trigger.Operation,
			Connection:            trigger.Connection,
			Instance:              trigger.Instance,
			Input:                 cloneMap(trigger.Input),
			Target:                cloneTarget(trigger.Target),
			TriggerKind:           triggerKindEvent,
			TriggerEventTriggerID: trigger.ID,
			TriggerEvent:          cloneEvent(event),
			CreatedAt:             now,
			CreatedBy:             createdBy,
			ExecutionRef:          executionRef,
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			if errors.Is(err, gestalt.ErrAlreadyExists) {
				continue
			}
			if createdExecutionRef {
				_ = state.executionRefStore.Delete(ctx, executionRef)
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

func (p *Provider) PutExecutionReference(ctx context.Context, req *proto.PutWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	if req == nil || req.GetReference() == nil {
		return nil, status.Error(codes.InvalidArgument, "reference is required")
	}
	ref := cloneExecutionReference(req.GetReference())
	if strings.TrimSpace(ref.GetProviderName()) == "" {
		ref.ProviderName = p.providerName()
	}
	record, err := executionReferenceRecordFromProto(ref)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if existing, found, err := loadExecutionReferenceRecord(ctx, state.executionRefStore, record.ID); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load execution reference: %v", err)
	} else if found && !existing.CreatedAt.IsZero() {
		record.CreatedAt = existing.CreatedAt
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = p.clock().UTC()
	}
	if err := state.executionRefStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "put execution reference: %v", err)
	}
	resp, err := record.toProto()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build execution reference response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetExecutionReference(ctx context.Context, req *proto.GetWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	refID := strings.TrimSpace(req.GetId())
	if refID == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadExecutionReferenceRecord(ctx, state.executionRefStore, refID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get execution reference: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow execution reference %q not found", refID)
	}
	resp, err := record.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build execution reference response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListExecutionReferences(ctx context.Context, req *proto.ListWorkflowExecutionReferencesRequest) (*proto.ListWorkflowExecutionReferencesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	subjectID := strings.TrimSpace(req.GetSubjectId())
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listExecutionReferenceRecords(ctx, state.executionRefStore, subjectID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list execution references: %v", err)
	}
	resp := &proto.ListWorkflowExecutionReferencesResponse{References: make([]*proto.WorkflowExecutionReference, 0, len(records))}
	for _, record := range records {
		pbRef, err := record.toProto()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build execution reference response: %v", err)
		}
		resp.References = append(resp.References, pbRef)
	}
	return resp, nil
}

func (p *Provider) enqueueSignalLocked(ctx context.Context, state *configuredState, run workflowRunRecord, signal *proto.WorkflowSignal, startedRun bool) (*proto.SignalWorkflowRunResponse, error) {
	signal = cloneSignal(signal)
	if strings.TrimSpace(signal.GetId()) == "" {
		signal.Id = workflowSignalID(run, signal)
	}
	if signal.GetSequence() == 0 {
		next, err := nextSignalSequence(ctx, state.signalStore, run.ID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "assign signal sequence: %v", err)
		}
		signal.Sequence = next
	}
	record := workflowSignalRecord{
		ID:             signal.GetId(),
		RunID:          run.ID,
		WorkflowKey:    run.WorkflowKey,
		State:          signalStatePending,
		Signal:         cloneSignal(signal),
		IdempotencyKey: strings.TrimSpace(signal.GetIdempotencyKey()),
		Sequence:       signal.GetSequence(),
		StartedRun:     startedRun,
		CreatedAt:      signal.GetCreatedAt().AsTime().UTC(),
	}
	if err := state.signalStore.Add(ctx, record.toRecord()); err != nil {
		if !errors.Is(err, gestalt.ErrAlreadyExists) {
			return nil, status.Errorf(codes.Internal, "store signal: %v", err)
		}
		existing, found, loadErr := loadSignalRecord(ctx, state.signalStore, record.ID)
		if loadErr != nil {
			return nil, status.Errorf(codes.Internal, "load existing signal: %v", loadErr)
		}
		if found {
			if existing.RunID != run.ID {
				return nil, status.Errorf(codes.FailedPrecondition, "workflow signal %q belongs to a different run", record.ID)
			}
			record = existing
			signal = record.signalProto()
		}
	}
	if key := strings.TrimSpace(record.IdempotencyKey); key != "" {
		if err := storeSignalIdempotencyRecord(ctx, state.idempotencyStore, run.PluginName, key, run.ID, record.ID, run.WorkflowKey, record.StartedRun, record.CreatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "store signal idempotency key: %v", err)
		}
	}
	pbRun, err := run.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal,
		StartedRun:  record.StartedRun,
		WorkflowKey: run.WorkflowKey,
	}, nil
}

func (p *Provider) signalIdempotencyResponseLocked(ctx context.Context, state *configuredState, record workflowIdempotencyRecord) (*proto.SignalWorkflowRunResponse, error) {
	run, found, err := loadRunRecord(ctx, state.runStore, record.PluginName, record.RunID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load idempotent signal run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", record.RunID)
	}
	signal, found, err := loadSignalRecord(ctx, state.signalStore, record.SignalID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load idempotent signal: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow signal %q not found", record.SignalID)
	}
	pbRun, err := run.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	workflowKey := strings.TrimSpace(record.WorkflowKey)
	if workflowKey == "" {
		workflowKey = run.WorkflowKey
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal.signalProto(),
		StartedRun:  record.StartedRun,
		WorkflowKey: workflowKey,
	}, nil
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
			Connection:          schedule.Connection,
			Instance:            schedule.Instance,
			Input:               cloneMap(schedule.Input),
			Target:              cloneTarget(schedule.Target),
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
	pending, found, err := nextRunnableRun(ctx, state.runStore, state.signalStore)
	if err != nil {
		p.mu.Unlock()
		return false, err
	}
	if !found {
		p.mu.Unlock()
		return false, nil
	}

	now := p.clock().UTC()
	var claimedSignals []workflowSignalRecord
	batchID := uuid.NewString()
	var pendingSignals []workflowSignalRecord
	if pending.WorkflowKey != "" {
		pendingSignals, err = listSignalRecords(ctx, state.signalStore, pending.ID, signalStatePending)
		if err != nil {
			p.mu.Unlock()
			return false, err
		}
		if len(pendingSignals) == 0 {
			p.mu.Unlock()
			return false, nil
		}
	}
	pending.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	pending.StartedAt = &now
	pending.StatusMessage = ""
	if err := state.runStore.Put(ctx, pending.toRecord()); err != nil {
		p.mu.Unlock()
		return false, err
	}
	if pending.WorkflowKey != "" {
		for _, signal := range pendingSignals {
			signal.State = signalStateClaimed
			signal.BatchID = batchID
			signal.ClaimedAt = &now
			if err := state.signalStore.Put(ctx, signal.toRecord()); err != nil {
				p.mu.Unlock()
				return false, err
			}
			claimedSignals = append(claimedSignals, signal)
		}
	}
	host := state.host
	p.mu.Unlock()

	resp, invokeErr := host.InvokeOperation(ctx, &proto.InvokeWorkflowOperationRequest{
		Target:       pending.targetProto(),
		RunId:        pending.ID,
		Trigger:      pending.triggerProto(),
		CreatedBy:    cloneActor(pending.CreatedBy),
		ExecutionRef: pending.ExecutionRef,
		Signals:      signalProtos(claimedSignals),
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
		_ = markRunSignalsFailed(ctx, state.signalStore, current.ID, claimedSignals, completedAt, invokeErr.Error())
		if current.WorkflowKey != "" {
			_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, current.WorkflowKey)
		}
	} else {
		current.ResultBody = resp.GetBody()
		if resp.GetStatus() >= http.StatusBadRequest {
			current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			current.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			_ = markRunSignalsFailed(ctx, state.signalStore, current.ID, claimedSignals, completedAt, current.StatusMessage)
			if current.WorkflowKey != "" {
				_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, current.WorkflowKey)
			}
		} else {
			_ = markSignalsDelivered(ctx, state.signalStore, claimedSignals, completedAt)
			hasPending, err := hasPendingSignals(ctx, state.signalStore, current.ID)
			if err != nil {
				return true, err
			}
			if current.WorkflowKey != "" && hasPending {
				current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				current.CompletedAt = nil
			} else {
				current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
				if current.WorkflowKey != "" {
					_ = deleteWorkflowKeyRecord(ctx, state.workflowKeyStore, current.WorkflowKey)
				}
			}
			current.StatusMessage = ""
		}
	}
	if err := state.runStore.Put(ctx, current.toRecord()); err != nil {
		return true, err
	}
	return true, nil
}

func (p *Provider) requireConfiguredLocked() (*configuredState, error) {
	if p.runStore == nil || p.scheduleStore == nil || p.eventTriggerStore == nil || p.idempotencyStore == nil || p.executionRefStore == nil || p.workflowKeyStore == nil || p.signalStore == nil || p.host == nil {
		return nil, errors.New("indexeddb workflow: provider is not configured")
	}
	return &configuredState{
		host:              p.host,
		scheduleStore:     p.scheduleStore,
		eventTriggerStore: p.eventTriggerStore,
		runStore:          p.runStore,
		idempotencyStore:  p.idempotencyStore,
		executionRefStore: p.executionRefStore,
		workflowKeyStore:  p.workflowKeyStore,
		signalStore:       p.signalStore,
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

func (p *Provider) providerName() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.TrimSpace(p.name)
}

type configuredState struct {
	host              *gestalt.WorkflowHostClient
	scheduleStore     *gestalt.ObjectStoreClient
	eventTriggerStore *gestalt.ObjectStoreClient
	runStore          *gestalt.ObjectStoreClient
	idempotencyStore  *gestalt.ObjectStoreClient
	executionRefStore *gestalt.ObjectStoreClient
	workflowKeyStore  *gestalt.ObjectStoreClient
	signalStore       *gestalt.ObjectStoreClient
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
		{
			name:   storeExecutionRefs,
			schema: workflowExecutionReferenceSchema(),
		},
		{
			name:   storeWorkflowKeys,
			schema: workflowKeySchema(),
		},
		{
			name:   storeSignals,
			schema: workflowSignalSchema(),
		},
	}
}

func workflowKeySchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "workflow_key", Type: columnTypeString, NotNull: true},
			{Name: "run_id", Type: columnTypeString, NotNull: true},
			{Name: "target_fingerprint", Type: columnTypeString},
			{Name: "created_at", Type: columnTypeTime},
		},
	}
}

func workflowSignalSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_run", KeyPath: []string{"run_id"}},
			{Name: "by_run_state", KeyPath: []string{"run_id", "state"}},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "run_id", Type: columnTypeString, NotNull: true},
			{Name: "workflow_key", Type: columnTypeString},
			{Name: "state", Type: columnTypeString, NotNull: true},
			{Name: "signal_json", Type: columnTypeString},
			{Name: "idempotency_key", Type: columnTypeString},
			{Name: "sequence", Type: columnTypeInt},
			{Name: "started_run", Type: columnTypeBool},
			{Name: "batch_id", Type: columnTypeString},
			{Name: "created_at", Type: columnTypeTime},
			{Name: "claimed_at", Type: columnTypeTime},
			{Name: "delivered_at", Type: columnTypeTime},
			{Name: "failed_at", Type: columnTypeTime},
			{Name: "status_message", Type: columnTypeString},
		},
	}
}

func workflowExecutionReferenceSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_subject", KeyPath: []string{"subject_id"}},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "provider_name", Type: columnTypeString, NotNull: true},
			{Name: "target_plugin", Type: columnTypeString, NotNull: true},
			{Name: "target_operation", Type: columnTypeString, NotNull: true},
			{Name: "target_connection", Type: columnTypeString},
			{Name: "target_instance", Type: columnTypeString},
			{Name: "target_json", Type: columnTypeString},
			{Name: "target_fingerprint", Type: columnTypeString},
			{Name: "subject_id", Type: columnTypeString, NotNull: true},
			{Name: "subject_kind", Type: columnTypeString},
			{Name: "display_name", Type: columnTypeString},
			{Name: "auth_source", Type: columnTypeString},
			{Name: "credential_subject_id", Type: columnTypeString},
			{Name: "permissions_json", Type: columnTypeString},
			{Name: "caller_plugin_name", Type: columnTypeString},
			{Name: "created_at", Type: columnTypeTime},
			{Name: "revoked_at", Type: columnTypeTime},
		},
	}
}

func markStaleRunningRunsFailed(ctx context.Context, runStore, workflowKeyStore, signalStore *gestalt.ObjectStoreClient, now time.Time) error {
	runs, err := runStore.GetAll(ctx, nil)
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
		if err := runStore.Put(ctx, run.toRecord()); err != nil {
			return err
		}
		if run.WorkflowKey != "" {
			if err := deleteWorkflowKeyRecord(ctx, workflowKeyStore, run.WorkflowKey); err != nil {
				return err
			}
		}
		signals, err := listSignalRecords(ctx, signalStore, run.ID, signalStatePending)
		if err != nil {
			return err
		}
		claimedSignals, err := listSignalRecords(ctx, signalStore, run.ID, signalStateClaimed)
		if err != nil {
			return err
		}
		signals = append(signals, claimedSignals...)
		if err := markSignalsFailed(ctx, signalStore, signals, now, run.StatusMessage); err != nil {
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
	if agentTarget := target.GetAgent(); agentTarget != nil {
		if target.GetPlugin() != nil {
			return scopedTarget{}, errors.New("target cannot include both agent and plugin fields")
		}
		if pluginName != "" {
			return scopedTarget{}, fmt.Errorf("agent target is outside scoped plugin %q", pluginName)
		}
		agentProvider := strings.TrimSpace(agentTarget.GetProviderName())
		if agentProvider == "" {
			return scopedTarget{}, errors.New("target.agent.provider_name is required")
		}
		normalized := &proto.BoundWorkflowTarget{Agent: cloneAgentTarget(agentTarget)}
		if err := normalizeAgentTarget(normalized.GetAgent(), agentProvider); err != nil {
			return scopedTarget{}, err
		}
		return scopedTarget{
			PluginName: "agent:" + agentProvider,
			Target:     normalized,
		}, nil
	}
	pluginFields := pluginTargetFields(target)
	targetPlugin := pluginFields.PluginName
	operation := pluginFields.Operation
	connection := pluginFields.Connection
	instance := pluginFields.Instance
	input := cloneStructMap(pluginFields.Input)
	if pluginName == "" {
		pluginName = targetPlugin
	}
	if pluginName == "" {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	if targetPlugin != "" && targetPlugin != pluginName {
		return scopedTarget{}, fmt.Errorf("target.plugin.plugin_name %q is outside scoped plugin %q", targetPlugin, pluginName)
	}
	if operation == "" {
		return scopedTarget{}, errors.New("target.plugin.operation is required")
	}
	normalized := pluginTargetProto(pluginName, operation, connection, instance, input)
	return scopedTarget{
		PluginName: pluginName,
		Operation:  operation,
		Connection: connection,
		Instance:   instance,
		Input:      input,
		Target:     normalized,
	}, nil
}

func normalizeAgentTarget(target *proto.BoundWorkflowAgentTarget, providerName string) error {
	if target == nil {
		return errors.New("target.agent is required")
	}
	target.ProviderName = strings.TrimSpace(providerName)
	target.Model = strings.TrimSpace(target.GetModel())
	target.Prompt = strings.TrimSpace(target.GetPrompt())
	switch target.GetToolSource() {
	case proto.AgentToolSourceMode_AGENT_TOOL_SOURCE_MODE_UNSPECIFIED:
		target.ToolSource = proto.AgentToolSourceMode_AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH
	case proto.AgentToolSourceMode_AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH:
	default:
		return fmt.Errorf("target.agent.tool_source %v is invalid", target.GetToolSource())
	}
	if target.GetPrompt() == "" && len(target.GetMessages()) == 0 {
		return errors.New("target.agent.prompt or messages is required")
	}
	if target.GetTimeoutSeconds() < 0 {
		return errors.New("target.agent.timeout_seconds must not be negative")
	}
	for i, ref := range target.GetToolRefs() {
		if ref == nil {
			return fmt.Errorf("target.agent.tool_refs[%d] is required", i)
		}
		ref.Plugin = strings.TrimSpace(ref.GetPlugin())
		ref.Operation = strings.TrimSpace(ref.GetOperation())
		ref.Connection = strings.TrimSpace(ref.GetConnection())
		ref.Instance = strings.TrimSpace(ref.GetInstance())
		ref.Title = strings.TrimSpace(ref.GetTitle())
		ref.Description = strings.TrimSpace(ref.GetDescription())
		if ref.GetPlugin() == "" {
			return fmt.Errorf("target.agent.tool_refs[%d].plugin is required", i)
		}
	}
	return nil
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

func normalizeWorkflowSignal(signal *proto.WorkflowSignal, now time.Time) (*proto.WorkflowSignal, error) {
	if signal == nil {
		return nil, errors.New("signal is required")
	}
	name := strings.TrimSpace(signal.GetName())
	if name == "" {
		return nil, errors.New("signal.name is required")
	}
	normalized := &proto.WorkflowSignal{
		Id:             strings.TrimSpace(signal.GetId()),
		Name:           name,
		Payload:        cloneStruct(signal.GetPayload()),
		Metadata:       cloneStruct(signal.GetMetadata()),
		CreatedBy:      cloneActor(signal.GetCreatedBy()),
		IdempotencyKey: strings.TrimSpace(signal.GetIdempotencyKey()),
		Sequence:       signal.GetSequence(),
	}
	if ts := signal.GetCreatedAt(); ts != nil && ts.IsValid() {
		normalized.CreatedAt = timestamppb.New(ts.AsTime().UTC())
	} else {
		normalized.CreatedAt = timestamppb.New(now.UTC())
	}
	return normalized, nil
}

func workflowRunTerminal(status proto.WorkflowRunStatus) bool {
	switch status {
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED:
		return true
	default:
		return false
	}
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

func nextRunnableRun(ctx context.Context, runStore, signalStore *gestalt.ObjectStoreClient) (workflowRunRecord, bool, error) {
	runs, err := listRunRecords(ctx, runStore, "")
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	for _, run := range runs {
		if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			continue
		}
		if strings.TrimSpace(run.WorkflowKey) == "" {
			return run, true, nil
		}
		hasPending, err := hasPendingSignals(ctx, signalStore, run.ID)
		if err != nil {
			return workflowRunRecord{}, false, err
		}
		if hasPending {
			return run, true, nil
		}
	}
	return workflowRunRecord{}, false, nil
}

func hasPendingSignals(ctx context.Context, store *gestalt.ObjectStoreClient, runID string) (bool, error) {
	signals, err := listSignalRecords(ctx, store, runID, signalStatePending)
	if err != nil {
		return false, err
	}
	return len(signals) > 0, nil
}

func signalProtos(records []workflowSignalRecord) []*proto.WorkflowSignal {
	if len(records) == 0 {
		return nil
	}
	out := make([]*proto.WorkflowSignal, 0, len(records))
	for _, record := range records {
		out = append(out, record.signalProto())
	}
	return out
}

func markRunSignalsFailed(ctx context.Context, store *gestalt.ObjectStoreClient, runID string, claimed []workflowSignalRecord, failedAt time.Time, message string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" && len(claimed) > 0 {
		runID = strings.TrimSpace(claimed[0].RunID)
	}
	if runID == "" {
		return nil
	}
	recordsByID := make(map[string]workflowSignalRecord)
	for _, record := range claimed {
		if record.ID != "" {
			recordsByID[record.ID] = record
		}
	}
	for _, state := range []string{signalStatePending, signalStateClaimed} {
		records, err := listSignalRecords(ctx, store, runID, state)
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.ID != "" {
				recordsByID[record.ID] = record
			}
		}
	}
	if len(recordsByID) == 0 {
		return nil
	}
	records := make([]workflowSignalRecord, 0, len(recordsByID))
	for _, record := range recordsByID {
		records = append(records, record)
	}
	return markSignalsFailed(ctx, store, records, failedAt, message)
}

func markSignalsDelivered(ctx context.Context, store *gestalt.ObjectStoreClient, records []workflowSignalRecord, deliveredAt time.Time) error {
	for _, record := range records {
		record.State = signalStateDelivered
		record.DeliveredAt = &deliveredAt
		record.StatusMessage = ""
		if err := store.Put(ctx, record.toRecord()); err != nil {
			return err
		}
	}
	return nil
}

func markSignalsFailed(ctx context.Context, store *gestalt.ObjectStoreClient, records []workflowSignalRecord, failedAt time.Time, message string) error {
	for _, record := range records {
		record.State = signalStateFailed
		record.FailedAt = &failedAt
		record.StatusMessage = strings.TrimSpace(message)
		if err := store.Put(ctx, record.toRecord()); err != nil {
			return err
		}
	}
	return nil
}

func activeWorkflowKeyRun(ctx context.Context, state *configuredState, workflowKey string) (workflowRunRecord, bool, error) {
	key, found, err := loadWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey)
	if err != nil || !found {
		return workflowRunRecord{}, false, err
	}
	run, runFound, err := loadRunRecord(ctx, state.runStore, "", key.RunID)
	if err != nil || !runFound {
		return workflowRunRecord{}, false, err
	}
	return run, true, nil
}

func loadWorkflowKeyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, workflowKey string) (workflowKeyRecord, bool, error) {
	record, err := store.Get(ctx, workflowKeyID(workflowKey))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowKeyRecord{}, false, nil
		}
		return workflowKeyRecord{}, false, err
	}
	return workflowKeyRecordFromRecord(record), true, nil
}

func storeWorkflowKeyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, workflowKey, runID, targetFingerprint string, createdAt time.Time) error {
	record := workflowKeyRecord{
		ID:                workflowKeyID(workflowKey),
		WorkflowKey:       strings.TrimSpace(workflowKey),
		RunID:             strings.TrimSpace(runID),
		TargetFingerprint: strings.TrimSpace(targetFingerprint),
		CreatedAt:         createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func deleteWorkflowKeyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, workflowKey string) error {
	if err := store.Delete(ctx, workflowKeyID(workflowKey)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}
	return nil
}

func workflowKeyID(workflowKey string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(workflowKey)))
	return "workflow_key:" + hex.EncodeToString(sum[:])
}

func workflowSignalID(run workflowRunRecord, signal *proto.WorkflowSignal) string {
	key := strings.TrimSpace(signal.GetIdempotencyKey())
	if key == "" {
		return uuid.NewString()
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(run.PluginName),
		strings.TrimSpace(run.ID),
		key,
	}, "\x00")))
	return "workflow_signal:" + hex.EncodeToString(sum[:])
}

func loadSignalRecord(ctx context.Context, store *gestalt.ObjectStoreClient, signalID string) (workflowSignalRecord, bool, error) {
	record, err := store.Get(ctx, strings.TrimSpace(signalID))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowSignalRecord{}, false, nil
		}
		return workflowSignalRecord{}, false, err
	}
	signal, err := signalRecordFromRecord(record)
	if err != nil {
		return workflowSignalRecord{}, false, err
	}
	return signal, true, nil
}

func listSignalRecords(ctx context.Context, store *gestalt.ObjectStoreClient, runID, state string) ([]workflowSignalRecord, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]workflowSignalRecord, 0, len(records))
	for _, record := range records {
		signal, err := signalRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if runID != "" && signal.RunID != runID {
			continue
		}
		if state != "" && signal.State != state {
			continue
		}
		out = append(out, signal)
	}
	slices.SortFunc(out, func(a, b workflowSignalRecord) int {
		if a.Sequence != b.Sequence {
			if a.Sequence < b.Sequence {
				return -1
			}
			return 1
		}
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func nextSignalSequence(ctx context.Context, store *gestalt.ObjectStoreClient, runID string) (int64, error) {
	signals, err := listSignalRecords(ctx, store, runID, "")
	if err != nil {
		return 0, err
	}
	var max int64
	for _, signal := range signals {
		if signal.Sequence > max {
			max = signal.Sequence
		}
	}
	return max + 1, nil
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

func storeSignalIdempotencyRecord(ctx context.Context, store *gestalt.ObjectStoreClient, pluginName, key, runID, signalID, workflowKey string, startedRun bool, createdAt time.Time) error {
	record := workflowIdempotencyRecord{
		ID:             idempotencyID(pluginName, key),
		PluginName:     pluginName,
		IdempotencyKey: key,
		RunID:          runID,
		SignalID:       signalID,
		WorkflowKey:    workflowKey,
		StartedRun:     startedRun,
		CreatedAt:      createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func loadExecutionReferenceRecord(ctx context.Context, store *gestalt.ObjectStoreClient, refID string) (workflowExecutionReferenceRecord, bool, error) {
	record, err := store.Get(ctx, strings.TrimSpace(refID))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowExecutionReferenceRecord{}, false, nil
		}
		return workflowExecutionReferenceRecord{}, false, err
	}
	ref, err := executionReferenceRecordFromRecord(record)
	if err != nil {
		return workflowExecutionReferenceRecord{}, false, err
	}
	return ref, true, nil
}

func listExecutionReferenceRecords(ctx context.Context, store *gestalt.ObjectStoreClient, subjectID string) ([]workflowExecutionReferenceRecord, error) {
	subjectID = strings.TrimSpace(subjectID)
	var records []gestalt.Record
	var err error
	if subjectID == "" {
		records, err = store.GetAll(ctx, nil)
	} else {
		records, err = store.Index("by_subject").GetAll(ctx, nil, subjectID)
	}
	if err != nil {
		return nil, err
	}
	out := make([]workflowExecutionReferenceRecord, 0, len(records))
	for _, record := range records {
		ref, err := executionReferenceRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if subjectID != "" && ref.SubjectID != subjectID {
			continue
		}
		out = append(out, ref)
	}
	slices.SortFunc(out, func(a, b workflowExecutionReferenceRecord) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
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

func eventExecutionRefID(runID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(runID)))
	return "event_ref:" + hex.EncodeToString(sum[:16])
}

func actorHasSubject(actor *proto.WorkflowActor) bool {
	return strings.TrimSpace(actor.GetSubjectId()) != ""
}

func createdByForUpsert(existing, requested *proto.WorkflowActor) *proto.WorkflowActor {
	if isConfigManagedActor(requested) {
		return cloneActor(requested)
	}
	return cloneActor(existing)
}

func publishedEventExecutionReference(providerName, runID string, trigger workflowEventTriggerRecord, actor *proto.WorkflowActor, createdAt time.Time) (*proto.WorkflowExecutionReference, error) {
	if !actorHasSubject(actor) {
		return nil, nil
	}
	target := trigger.Target
	fingerprint, err := workflowTargetFingerprint(target)
	if err != nil {
		return nil, err
	}
	permissions, err := eventExecutionReferencePermissions(trigger)
	if err != nil {
		return nil, err
	}
	subjectID := strings.TrimSpace(actor.GetSubjectId())
	return &proto.WorkflowExecutionReference{
		Id:                  eventExecutionRefID(runID),
		ProviderName:        strings.TrimSpace(providerName),
		Target:              cloneTarget(target),
		TargetFingerprint:   fingerprint,
		SubjectId:           subjectID,
		SubjectKind:         strings.TrimSpace(actor.GetSubjectKind()),
		DisplayName:         strings.TrimSpace(actor.GetDisplayName()),
		AuthSource:          strings.TrimSpace(actor.GetAuthSource()),
		CredentialSubjectId: subjectID,
		Permissions:         permissions,
		CreatedAt:           timestamppb.New(createdAt.UTC()),
	}, nil
}

func eventExecutionReferencePermissions(trigger workflowEventTriggerRecord) ([]*proto.WorkflowAccessPermission, error) {
	permissions := executionReferencePermissionsForTarget(trigger.Target)
	if !isConfigManagedActor(trigger.CreatedBy) {
		return permissions, nil
	}
	extra, err := configuredEventRunPermissions(trigger.Input)
	if err != nil {
		return nil, err
	}
	return mergeAccessPermissions(permissions, extra), nil
}

func isConfigManagedActor(actor *proto.WorkflowActor) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.GetSubjectId()) == configManagedWorkflowSubject &&
		strings.TrimSpace(actor.GetSubjectKind()) == configManagedWorkflowKind &&
		strings.TrimSpace(actor.GetAuthSource()) == configManagedWorkflowAuth
}

func executionReferencePermissionsForTarget(target *proto.BoundWorkflowTarget) []*proto.WorkflowAccessPermission {
	if target == nil {
		return nil
	}
	if agent := target.GetAgent(); agent != nil {
		permissionsByPlugin := map[string]map[string]struct{}{}
		for _, tool := range agent.GetToolRefs() {
			pluginName := strings.TrimSpace(tool.GetPlugin())
			operation := strings.TrimSpace(tool.GetOperation())
			if pluginName == "" || operation == "" {
				continue
			}
			ops := permissionsByPlugin[pluginName]
			if ops == nil {
				ops = map[string]struct{}{}
				permissionsByPlugin[pluginName] = ops
			}
			ops[operation] = struct{}{}
		}
		return accessPermissionsFromSet(permissionsByPlugin)
	}
	plugin := target.GetPlugin()
	if plugin == nil {
		return nil
	}
	pluginName := strings.TrimSpace(plugin.GetPluginName())
	if pluginName == "" {
		return nil
	}
	permission := &proto.WorkflowAccessPermission{Plugin: pluginName}
	if operation := strings.TrimSpace(plugin.GetOperation()); operation != "" {
		permission.Operations = []string{operation}
	}
	return []*proto.WorkflowAccessPermission{permission}
}

func configuredEventRunPermissions(input map[string]any) ([]*proto.WorkflowAccessPermission, error) {
	rawGestalt, ok := input[gestaltInputKey]
	if !ok || rawGestalt == nil {
		return nil, nil
	}
	gestaltConfig, ok := rawGestalt.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", gestaltInputKey)
	}
	rawPermissions, ok := gestaltConfig[eventRunPermissionsKey]
	if !ok || rawPermissions == nil {
		return nil, nil
	}
	items, ok := rawPermissions.([]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s must be a list", gestaltInputKey, eventRunPermissionsKey)
	}
	out := make([]*proto.WorkflowAccessPermission, 0, len(items))
	for i, item := range items {
		value, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.%s[%d] must be an object", gestaltInputKey, eventRunPermissionsKey, i)
		}
		pluginName := strings.TrimSpace(stringField(value, "plugin"))
		if pluginName == "" {
			return nil, fmt.Errorf("%s.%s[%d].plugin is required", gestaltInputKey, eventRunPermissionsKey, i)
		}
		operations, err := stringListField(value, "operations")
		if err != nil {
			return nil, fmt.Errorf("%s.%s[%d].operations: %w", gestaltInputKey, eventRunPermissionsKey, i, err)
		}
		if len(operations) == 0 {
			return nil, fmt.Errorf("%s.%s[%d].operations is required", gestaltInputKey, eventRunPermissionsKey, i)
		}
		out = append(out, &proto.WorkflowAccessPermission{
			Plugin:     pluginName,
			Operations: operations,
		})
	}
	return out, nil
}

func stringListField(value map[string]any, key string) ([]string, error) {
	raw, ok := value[key]
	if !ok || raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("must be a list")
	}
	out := make([]string, 0, len(items))
	for i, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("[%d] must be a string", i)
		}
		text = strings.TrimSpace(text)
		if text != "" {
			out = append(out, text)
		}
	}
	return out, nil
}

func mergeAccessPermissions(groups ...[]*proto.WorkflowAccessPermission) []*proto.WorkflowAccessPermission {
	set := map[string]map[string]struct{}{}
	for _, group := range groups {
		for _, permission := range group {
			addAccessPermission(set, permission)
		}
	}
	return accessPermissionsFromSet(set)
}

func addAccessPermission(set map[string]map[string]struct{}, permission *proto.WorkflowAccessPermission) {
	if permission == nil {
		return
	}
	pluginName := strings.TrimSpace(permission.GetPlugin())
	if pluginName == "" {
		return
	}
	if len(permission.GetOperations()) == 0 {
		set[pluginName] = nil
		return
	}
	if _, ok := set[pluginName]; ok && set[pluginName] == nil {
		return
	}
	ops := set[pluginName]
	if ops == nil {
		ops = map[string]struct{}{}
		set[pluginName] = ops
	}
	for _, operation := range permission.GetOperations() {
		operation = strings.TrimSpace(operation)
		if operation != "" {
			ops[operation] = struct{}{}
		}
	}
}

func accessPermissionsFromSet(values map[string]map[string]struct{}) []*proto.WorkflowAccessPermission {
	if len(values) == 0 {
		return nil
	}
	plugins := make([]string, 0, len(values))
	for pluginName := range values {
		plugins = append(plugins, pluginName)
	}
	slices.Sort(plugins)
	out := make([]*proto.WorkflowAccessPermission, 0, len(plugins))
	for _, pluginName := range plugins {
		operations := make([]string, 0, len(values[pluginName]))
		for operation := range values[pluginName] {
			operations = append(operations, operation)
		}
		slices.Sort(operations)
		out = append(out, &proto.WorkflowAccessPermission{
			Plugin:     pluginName,
			Operations: operations,
		})
	}
	return out
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

func cloneSignal(signal *proto.WorkflowSignal) *proto.WorkflowSignal {
	if signal == nil {
		return nil
	}
	return &proto.WorkflowSignal{
		Id:             signal.GetId(),
		Name:           signal.GetName(),
		Payload:        cloneStruct(signal.GetPayload()),
		Metadata:       cloneStruct(signal.GetMetadata()),
		CreatedBy:      cloneActor(signal.GetCreatedBy()),
		CreatedAt:      cloneTimestamp(signal.GetCreatedAt()),
		IdempotencyKey: signal.GetIdempotencyKey(),
		Sequence:       signal.GetSequence(),
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

var workflowTargetJSON = protojson.MarshalOptions{EmitUnpopulated: false}
var workflowTargetJSONUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

func pluginTargetFields(target *proto.BoundWorkflowTarget) workflowPluginTargetFields {
	if target == nil {
		return workflowPluginTargetFields{}
	}
	return pluginMessageTargetFields(target.GetPlugin())
}

func pluginMessageTargetFields(plugin *proto.BoundWorkflowPluginTarget) workflowPluginTargetFields {
	if plugin == nil {
		return workflowPluginTargetFields{}
	}
	return workflowPluginTargetFields{
		PluginName: strings.TrimSpace(plugin.GetPluginName()),
		Operation:  strings.TrimSpace(plugin.GetOperation()),
		Connection: strings.TrimSpace(plugin.GetConnection()),
		Instance:   strings.TrimSpace(plugin.GetInstance()),
		Input:      cloneStruct(plugin.GetInput()),
	}
}

func targetJSON(target *proto.BoundWorkflowTarget) string {
	if target == nil {
		return ""
	}
	data, err := workflowTargetJSON.Marshal(target)
	if err != nil {
		return ""
	}
	return string(data)
}

func signalJSON(signal *proto.WorkflowSignal) string {
	if signal == nil {
		return ""
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(signal)
	if err != nil {
		return ""
	}
	return string(data)
}

func signalFromRecordValue(raw any) *proto.WorkflowSignal {
	value := strings.TrimSpace(stringValue(raw))
	if value == "" {
		return nil
	}
	signal := &proto.WorkflowSignal{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(value), signal); err != nil {
		return nil
	}
	return cloneSignal(signal)
}

func targetFromRecordValue(recordKind, id string, raw any) (*proto.BoundWorkflowTarget, error) {
	if raw == nil {
		return nil, fmt.Errorf("%s %q missing target_json", recordKind, id)
	}
	value := strings.TrimSpace(stringValue(raw))
	if value == "" {
		return nil, fmt.Errorf("%s %q missing target_json", recordKind, id)
	}
	target := &proto.BoundWorkflowTarget{}
	if err := workflowTargetJSONUnmarshal.Unmarshal([]byte(value), target); err != nil {
		return nil, fmt.Errorf("%s %q invalid target_json: %w", recordKind, id, err)
	}
	target = cloneTarget(target)
	if target.GetAgent() == nil && !workflowPluginTargetSet(target) {
		return nil, fmt.Errorf("%s %q target_json must contain plugin or agent target", recordKind, id)
	}
	return target, nil
}

type workflowFingerprintTarget struct {
	PluginName string
	Operation  string
	Connection string
	Instance   string
	Input      map[string]any
	Plugin     *workflowFingerprintPluginTarget
	Agent      *workflowFingerprintAgentTarget
}

type workflowFingerprintPluginTarget struct {
	PluginName string
	Operation  string
	Connection string
	Instance   string
	Input      map[string]any
}

type workflowFingerprintAgentTarget struct {
	ProviderName    string
	Model           string
	Prompt          string
	Messages        []agentFingerprintMessage
	ToolRefs        []agentFingerprintToolRef
	ToolSource      string
	ResponseSchema  map[string]any
	ProviderOptions map[string]any
	Metadata        map[string]any
	TimeoutSeconds  int
}

type agentFingerprintMessage struct {
	Role     string
	Text     string
	Parts    []agentFingerprintMessagePart
	Metadata map[string]any
}

type agentFingerprintMessagePart struct {
	Type       string
	Text       string
	JSON       map[string]any
	ToolCall   *agentFingerprintToolCallPart
	ToolResult *agentFingerprintToolResultPart
	ImageRef   *agentFingerprintImageRefPart
}

type agentFingerprintToolCallPart struct {
	ID        string
	ToolID    string
	Arguments map[string]any
}

type agentFingerprintToolResultPart struct {
	ToolCallID string
	Status     int
	Content    string
	Output     map[string]any
}

type agentFingerprintImageRefPart struct {
	URI      string
	MIMEType string
}

type agentFingerprintToolRef struct {
	Plugin      string
	Operation   string
	Connection  string
	Instance    string
	Title       string
	Description string
}

func workflowTargetFingerprint(target *proto.BoundWorkflowTarget) (string, error) {
	if target.GetAgent() != nil && workflowPluginTargetSet(target) {
		return "", errors.New("target cannot include both agent and plugin fields")
	}
	payload := normalizedWorkflowTargetFingerprintPayload(target)
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func normalizedWorkflowTargetFingerprintPayload(target *proto.BoundWorkflowTarget) workflowFingerprintTarget {
	out := workflowFingerprintTarget{}
	if agent := target.GetAgent(); agent != nil {
		out.Agent = workflowAgentFingerprintPayload(agent)
		return out
	}
	plugin := workflowPluginFingerprintPayload(target)
	if !workflowFingerprintPluginTargetEmpty(plugin) {
		out.Plugin = &plugin
	}
	return out
}

func workflowPluginFingerprintPayload(target *proto.BoundWorkflowTarget) workflowFingerprintPluginTarget {
	plugin := pluginTargetFields(target)
	return workflowFingerprintPluginTarget{
		PluginName: plugin.PluginName,
		Operation:  plugin.Operation,
		Connection: plugin.Connection,
		Instance:   plugin.Instance,
		Input:      nilIfEmptyMap(cloneStructMap(plugin.Input)),
	}
}

func workflowPluginTargetSet(target *proto.BoundWorkflowTarget) bool {
	plugin := workflowPluginFingerprintPayload(target)
	return !workflowFingerprintPluginTargetEmpty(plugin)
}

func workflowFingerprintPluginTargetEmpty(target workflowFingerprintPluginTarget) bool {
	return strings.TrimSpace(target.PluginName) == "" &&
		strings.TrimSpace(target.Operation) == "" &&
		strings.TrimSpace(target.Connection) == "" &&
		strings.TrimSpace(target.Instance) == "" &&
		len(target.Input) == 0
}

func workflowAgentFingerprintPayload(agent *proto.BoundWorkflowAgentTarget) *workflowFingerprintAgentTarget {
	return &workflowFingerprintAgentTarget{
		ProviderName:    strings.TrimSpace(agent.GetProviderName()),
		Model:           strings.TrimSpace(agent.GetModel()),
		Prompt:          agent.GetPrompt(),
		Messages:        nilIfEmptySlice(agentMessagesFingerprintPayload(agent.GetMessages())),
		ToolRefs:        nilIfEmptySlice(agentToolRefsFingerprintPayload(agent.GetToolRefs())),
		ToolSource:      agentToolSourceFingerprintValue(agent.GetToolSource()),
		ResponseSchema:  nilIfEmptyMap(cloneStructMap(agent.GetResponseSchema())),
		Metadata:        nilIfEmptyMap(cloneStructMap(agent.GetMetadata())),
		ProviderOptions: nilIfEmptyMap(cloneStructMap(agent.GetProviderOptions())),
		TimeoutSeconds:  int(agent.GetTimeoutSeconds()),
	}
}

func agentMessagesFingerprintPayload(messages []*proto.AgentMessage) []agentFingerprintMessage {
	if len(messages) == 0 {
		return nil
	}
	out := make([]agentFingerprintMessage, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			out = append(out, agentFingerprintMessage{})
			continue
		}
		out = append(out, agentFingerprintMessage{
			Role:     message.GetRole(),
			Text:     message.GetText(),
			Parts:    nilIfEmptySlice(agentMessagePartsFingerprintPayload(message.GetParts())),
			Metadata: nilIfEmptyMap(cloneStructMap(message.GetMetadata())),
		})
	}
	return out
}

func agentMessagePartsFingerprintPayload(parts []*proto.AgentMessagePart) []agentFingerprintMessagePart {
	if len(parts) == 0 {
		return nil
	}
	out := make([]agentFingerprintMessagePart, 0, len(parts))
	for _, part := range parts {
		if part == nil {
			out = append(out, agentFingerprintMessagePart{})
			continue
		}
		value := agentFingerprintMessagePart{
			Type: agentMessagePartTypeFingerprintValue(part.GetType()),
			Text: part.GetText(),
			JSON: nilIfEmptyMap(cloneStructMap(part.GetJson())),
		}
		if toolCall := part.GetToolCall(); toolCall != nil {
			value.ToolCall = &agentFingerprintToolCallPart{
				ID:        toolCall.GetId(),
				ToolID:    toolCall.GetToolId(),
				Arguments: nilIfEmptyMap(cloneStructMap(toolCall.GetArguments())),
			}
		}
		if toolResult := part.GetToolResult(); toolResult != nil {
			value.ToolResult = &agentFingerprintToolResultPart{
				ToolCallID: toolResult.GetToolCallId(),
				Status:     int(toolResult.GetStatus()),
				Content:    toolResult.GetContent(),
				Output:     nilIfEmptyMap(cloneStructMap(toolResult.GetOutput())),
			}
		}
		if imageRef := part.GetImageRef(); imageRef != nil {
			value.ImageRef = &agentFingerprintImageRefPart{
				URI:      imageRef.GetUri(),
				MIMEType: imageRef.GetMimeType(),
			}
		}
		out = append(out, value)
	}
	return out
}

func agentToolRefsFingerprintPayload(refs []*proto.AgentToolRef) []agentFingerprintToolRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]agentFingerprintToolRef, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			out = append(out, agentFingerprintToolRef{})
			continue
		}
		out = append(out, agentFingerprintToolRef{
			Plugin:      ref.GetPlugin(),
			Operation:   ref.GetOperation(),
			Connection:  ref.GetConnection(),
			Instance:    ref.GetInstance(),
			Title:       ref.GetTitle(),
			Description: ref.GetDescription(),
		})
	}
	return out
}

func agentToolSourceFingerprintValue(source proto.AgentToolSourceMode) string {
	if source == proto.AgentToolSourceMode_AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH {
		return "native_search"
	}
	return ""
}

func agentMessagePartTypeFingerprintValue(partType proto.AgentMessagePartType) string {
	switch partType {
	case proto.AgentMessagePartType_AGENT_MESSAGE_PART_TYPE_TEXT:
		return "text"
	case proto.AgentMessagePartType_AGENT_MESSAGE_PART_TYPE_JSON:
		return "json"
	case proto.AgentMessagePartType_AGENT_MESSAGE_PART_TYPE_TOOL_CALL:
		return "tool_call"
	case proto.AgentMessagePartType_AGENT_MESSAGE_PART_TYPE_TOOL_RESULT:
		return "tool_result"
	case proto.AgentMessagePartType_AGENT_MESSAGE_PART_TYPE_IMAGE_REF:
		return "image_ref"
	default:
		return ""
	}
}

func nilIfEmptyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nilIfEmptySlice[T any](value []T) []T {
	if len(value) == 0 {
		return nil
	}
	return value
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
	return stringValue(raw)
}

func stringValue(raw any) string {
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
		"connection":    r.Connection,
		"instance":      r.Instance,
		"input":         cloneMap(r.Input),
		"target_json":   targetJSON(r.Target),
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
	id := stringField(value, "id")
	target, err := targetFromRecordValue("workflow schedule", id, value["target_json"])
	if err != nil {
		return workflowScheduleRecord{}, err
	}
	out := workflowScheduleRecord{
		ID:           id,
		PluginName:   stringField(value, "plugin_name"),
		Cron:         stringField(value, "cron"),
		Timezone:     stringField(value, "timezone"),
		Operation:    stringField(value, "operation"),
		Connection:   stringField(value, "connection"),
		Instance:     stringField(value, "instance"),
		Input:        anyMap(value["input"]),
		Target:       target,
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
		Id:           r.ID,
		Cron:         r.Cron,
		Timezone:     r.Timezone,
		Target:       r.targetProto(),
		Paused:       r.Paused,
		CreatedAt:    timestamppb.New(r.CreatedAt),
		UpdatedAt:    timestamppb.New(r.UpdatedAt),
		NextRunAt:    timeToProto(r.NextRunAt),
		CreatedBy:    cloneActor(r.CreatedBy),
		ExecutionRef: r.ExecutionRef,
	}, nil
}

func (r workflowScheduleRecord) targetProto() *proto.BoundWorkflowTarget {
	return cloneTarget(r.Target)
}

func (r workflowEventTriggerRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":            r.ID,
		"plugin_name":   r.PluginName,
		"match_type":    r.MatchType,
		"match_source":  r.MatchSource,
		"match_subject": r.MatchSubject,
		"operation":     r.Operation,
		"connection":    r.Connection,
		"instance":      r.Instance,
		"input":         cloneMap(r.Input),
		"target_json":   targetJSON(r.Target),
		"paused":        r.Paused,
		"created_at":    r.CreatedAt.UTC(),
		"updated_at":    r.UpdatedAt.UTC(),
		"created_by":    actorToMap(r.CreatedBy),
		"execution_ref": r.ExecutionRef,
	}
}

func eventTriggerRecordFromRecord(record gestalt.Record) (workflowEventTriggerRecord, error) {
	value := map[string]any(record)
	id := stringField(value, "id")
	target, err := targetFromRecordValue("workflow event trigger", id, value["target_json"])
	if err != nil {
		return workflowEventTriggerRecord{}, err
	}
	out := workflowEventTriggerRecord{
		ID:           id,
		PluginName:   stringField(value, "plugin_name"),
		MatchType:    stringField(value, "match_type"),
		MatchSource:  stringField(value, "match_source"),
		MatchSubject: stringField(value, "match_subject"),
		Operation:    stringField(value, "operation"),
		Connection:   stringField(value, "connection"),
		Instance:     stringField(value, "instance"),
		Input:        anyMap(value["input"]),
		Target:       target,
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
		Target:       r.targetProto(),
		Paused:       r.Paused,
		CreatedAt:    timestamppb.New(r.CreatedAt),
		UpdatedAt:    timestamppb.New(r.UpdatedAt),
		CreatedBy:    cloneActor(r.CreatedBy),
		ExecutionRef: r.ExecutionRef,
	}, nil
}

func (r workflowEventTriggerRecord) targetProto() *proto.BoundWorkflowTarget {
	return cloneTarget(r.Target)
}

func (r workflowRunRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":                       r.ID,
		"plugin_name":              r.PluginName,
		"status":                   int64(r.Status),
		"operation":                r.Operation,
		"connection":               r.Connection,
		"instance":                 r.Instance,
		"input":                    cloneMap(r.Input),
		"target_json":              targetJSON(r.Target),
		"trigger_kind":             r.TriggerKind,
		"trigger_schedule_id":      r.TriggerScheduleID,
		"trigger_event_trigger_id": r.TriggerEventTriggerID,
		"trigger_event":            eventToMap(r.TriggerEvent),
		"created_at":               r.CreatedAt.UTC(),
		"status_message":           r.StatusMessage,
		"result_body":              r.ResultBody,
		"created_by":               actorToMap(r.CreatedBy),
		"execution_ref":            r.ExecutionRef,
		"workflow_key":             r.WorkflowKey,
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
	id := stringField(value, "id")
	target, err := targetFromRecordValue("workflow run", id, value["target_json"])
	if err != nil {
		return workflowRunRecord{}, err
	}
	out := workflowRunRecord{
		ID:                    id,
		PluginName:            stringField(value, "plugin_name"),
		Status:                proto.WorkflowRunStatus(intField(value, "status")),
		Operation:             stringField(value, "operation"),
		Connection:            stringField(value, "connection"),
		Instance:              stringField(value, "instance"),
		Input:                 anyMap(value["input"]),
		Target:                target,
		TriggerKind:           stringField(value, "trigger_kind"),
		TriggerScheduleID:     stringField(value, "trigger_schedule_id"),
		TriggerEventTriggerID: stringField(value, "trigger_event_trigger_id"),
		TriggerEvent:          eventFromAny(value["trigger_event"]),
		StatusMessage:         stringField(value, "status_message"),
		ResultBody:            stringField(value, "result_body"),
		CreatedBy:             actorFromAny(value["created_by"]),
		ExecutionRef:          stringField(value, "execution_ref"),
		WorkflowKey:           stringField(value, "workflow_key"),
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
		WorkflowKey:   r.WorkflowKey,
	}, nil
}

func (r workflowRunRecord) targetProto() *proto.BoundWorkflowTarget {
	return cloneTarget(r.Target)
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
		"signal_id":       r.SignalID,
		"workflow_key":    r.WorkflowKey,
		"started_run":     r.StartedRun,
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
		SignalID:       stringField(value, "signal_id"),
		WorkflowKey:    stringField(value, "workflow_key"),
		StartedRun:     boolField(value, "started_run"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	return out, nil
}

func (r workflowKeyRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":                 r.ID,
		"workflow_key":       r.WorkflowKey,
		"run_id":             r.RunID,
		"target_fingerprint": r.TargetFingerprint,
		"created_at":         r.CreatedAt.UTC(),
	}
}

func workflowKeyRecordFromRecord(record gestalt.Record) workflowKeyRecord {
	value := map[string]any(record)
	out := workflowKeyRecord{
		ID:                stringField(value, "id"),
		WorkflowKey:       stringField(value, "workflow_key"),
		RunID:             stringField(value, "run_id"),
		TargetFingerprint: stringField(value, "target_fingerprint"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	return out
}

func (r workflowSignalRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":              r.ID,
		"run_id":          r.RunID,
		"workflow_key":    r.WorkflowKey,
		"state":           r.State,
		"signal_json":     signalJSON(r.signalProto()),
		"idempotency_key": r.IdempotencyKey,
		"sequence":        r.Sequence,
		"started_run":     r.StartedRun,
		"batch_id":        r.BatchID,
		"created_at":      r.CreatedAt.UTC(),
		"status_message":  r.StatusMessage,
	}
	if r.ClaimedAt != nil {
		record["claimed_at"] = r.ClaimedAt.UTC()
	} else {
		record["claimed_at"] = nil
	}
	if r.DeliveredAt != nil {
		record["delivered_at"] = r.DeliveredAt.UTC()
	} else {
		record["delivered_at"] = nil
	}
	if r.FailedAt != nil {
		record["failed_at"] = r.FailedAt.UTC()
	} else {
		record["failed_at"] = nil
	}
	return record
}

func signalRecordFromRecord(record gestalt.Record) (workflowSignalRecord, error) {
	value := map[string]any(record)
	out := workflowSignalRecord{
		ID:             stringField(value, "id"),
		RunID:          stringField(value, "run_id"),
		WorkflowKey:    stringField(value, "workflow_key"),
		State:          stringField(value, "state"),
		Signal:         signalFromRecordValue(value["signal_json"]),
		IdempotencyKey: stringField(value, "idempotency_key"),
		Sequence:       intField(value, "sequence"),
		StartedRun:     boolField(value, "started_run"),
		BatchID:        stringField(value, "batch_id"),
		StatusMessage:  stringField(value, "status_message"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	out.ClaimedAt = timeField(value, "claimed_at")
	out.DeliveredAt = timeField(value, "delivered_at")
	out.FailedAt = timeField(value, "failed_at")
	if out.Signal == nil {
		return workflowSignalRecord{}, errors.New("signal_json is required")
	}
	if out.State == "" {
		out.State = signalStatePending
	}
	return out, nil
}

func (r workflowSignalRecord) signalProto() *proto.WorkflowSignal {
	signal := cloneSignal(r.Signal)
	if signal == nil {
		signal = &proto.WorkflowSignal{}
	}
	if signal.Id == "" {
		signal.Id = r.ID
	}
	if signal.IdempotencyKey == "" {
		signal.IdempotencyKey = r.IdempotencyKey
	}
	if signal.Sequence == 0 {
		signal.Sequence = r.Sequence
	}
	if signal.CreatedAt == nil && !r.CreatedAt.IsZero() {
		signal.CreatedAt = timestamppb.New(r.CreatedAt)
	}
	return signal
}

type workflowAccessPermissionRecord struct {
	Plugin     string   `json:"plugin"`
	Operations []string `json:"operations,omitempty"`
}

func executionReferenceRecordFromProto(ref *proto.WorkflowExecutionReference) (workflowExecutionReferenceRecord, error) {
	if ref == nil {
		return workflowExecutionReferenceRecord{}, errors.New("reference is required")
	}
	target, err := normalizeScopedTarget("", ref.GetTarget())
	if err != nil {
		return workflowExecutionReferenceRecord{}, err
	}
	record := workflowExecutionReferenceRecord{
		ID:                  strings.TrimSpace(ref.GetId()),
		ProviderName:        strings.TrimSpace(ref.GetProviderName()),
		TargetPlugin:        target.PluginName,
		TargetOperation:     target.Operation,
		TargetConnection:    target.Connection,
		TargetInstance:      target.Instance,
		Target:              cloneTarget(target.Target),
		TargetFingerprint:   strings.TrimSpace(ref.GetTargetFingerprint()),
		SubjectID:           strings.TrimSpace(ref.GetSubjectId()),
		SubjectKind:         strings.TrimSpace(ref.GetSubjectKind()),
		DisplayName:         strings.TrimSpace(ref.GetDisplayName()),
		AuthSource:          strings.TrimSpace(ref.GetAuthSource()),
		CredentialSubjectID: strings.TrimSpace(ref.GetCredentialSubjectId()),
		CallerPluginName:    strings.TrimSpace(ref.GetCallerPluginName()),
	}
	if record.Target != nil && record.Target.GetAgent() != nil && record.TargetFingerprint == "" {
		return workflowExecutionReferenceRecord{}, errors.New("target_fingerprint is required for agent execution references")
	}
	if record.ID == "" {
		return workflowExecutionReferenceRecord{}, errors.New("id is required")
	}
	if record.ProviderName == "" {
		return workflowExecutionReferenceRecord{}, errors.New("provider_name is required")
	}
	if record.TargetPlugin == "" {
		return workflowExecutionReferenceRecord{}, errors.New("target plugin scope is required")
	}
	if record.SubjectID == "" {
		return workflowExecutionReferenceRecord{}, errors.New("subject_id is required")
	}
	permissionsJSON, err := executionReferencePermissionsJSON(ref.GetPermissions())
	if err != nil {
		return workflowExecutionReferenceRecord{}, fmt.Errorf("permissions: %w", err)
	}
	record.PermissionsJSON = permissionsJSON
	if ts := ref.GetCreatedAt(); ts != nil && ts.IsValid() {
		record.CreatedAt = ts.AsTime().UTC()
	}
	if ts := ref.GetRevokedAt(); ts != nil && ts.IsValid() {
		record.RevokedAt = timePtr(ts.AsTime())
	}
	return record, nil
}

func executionReferenceRecordFromRecord(record gestalt.Record) (workflowExecutionReferenceRecord, error) {
	value := map[string]any(record)
	id := stringField(value, "id")
	target, err := targetFromRecordValue("workflow execution reference", id, value["target_json"])
	if err != nil {
		return workflowExecutionReferenceRecord{}, err
	}
	out := workflowExecutionReferenceRecord{
		ID:                  id,
		ProviderName:        stringField(value, "provider_name"),
		TargetPlugin:        stringField(value, "target_plugin"),
		TargetOperation:     stringField(value, "target_operation"),
		TargetConnection:    stringField(value, "target_connection"),
		TargetInstance:      stringField(value, "target_instance"),
		Target:              target,
		TargetFingerprint:   stringField(value, "target_fingerprint"),
		SubjectID:           stringField(value, "subject_id"),
		SubjectKind:         stringField(value, "subject_kind"),
		DisplayName:         stringField(value, "display_name"),
		AuthSource:          stringField(value, "auth_source"),
		CredentialSubjectID: stringField(value, "credential_subject_id"),
		PermissionsJSON:     stringField(value, "permissions_json"),
		CallerPluginName:    stringField(value, "caller_plugin_name"),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	out.RevokedAt = timeField(value, "revoked_at")
	return out, nil
}

func (r workflowExecutionReferenceRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":                    r.ID,
		"provider_name":         r.ProviderName,
		"target_plugin":         r.TargetPlugin,
		"target_operation":      r.TargetOperation,
		"target_connection":     r.TargetConnection,
		"target_instance":       r.TargetInstance,
		"target_json":           targetJSON(r.Target),
		"target_fingerprint":    r.TargetFingerprint,
		"subject_id":            r.SubjectID,
		"subject_kind":          r.SubjectKind,
		"display_name":          r.DisplayName,
		"auth_source":           r.AuthSource,
		"credential_subject_id": r.CredentialSubjectID,
		"permissions_json":      r.PermissionsJSON,
		"caller_plugin_name":    r.CallerPluginName,
		"created_at":            r.CreatedAt.UTC(),
	}
	if r.RevokedAt != nil {
		record["revoked_at"] = r.RevokedAt.UTC()
	} else {
		record["revoked_at"] = nil
	}
	return record
}

func (r workflowExecutionReferenceRecord) toProto() (*proto.WorkflowExecutionReference, error) {
	permissions, err := executionReferencePermissionsFromJSON(r.PermissionsJSON)
	if err != nil {
		return nil, err
	}
	return &proto.WorkflowExecutionReference{
		Id:                  r.ID,
		ProviderName:        r.ProviderName,
		Target:              r.targetProto(),
		TargetFingerprint:   r.TargetFingerprint,
		SubjectId:           r.SubjectID,
		SubjectKind:         r.SubjectKind,
		DisplayName:         r.DisplayName,
		AuthSource:          r.AuthSource,
		CredentialSubjectId: r.CredentialSubjectID,
		Permissions:         permissions,
		CallerPluginName:    r.CallerPluginName,
		CreatedAt:           timestamppb.New(r.CreatedAt),
		RevokedAt:           timeToProto(r.RevokedAt),
	}, nil
}

func (r workflowExecutionReferenceRecord) targetProto() *proto.BoundWorkflowTarget {
	return cloneTarget(r.Target)
}

func executionReferencePermissionsJSON(values []*proto.WorkflowAccessPermission) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	records := make([]workflowAccessPermissionRecord, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		pluginName := strings.TrimSpace(value.GetPlugin())
		if pluginName == "" {
			continue
		}
		records = append(records, workflowAccessPermissionRecord{
			Plugin:     pluginName,
			Operations: append([]string(nil), value.GetOperations()...),
		})
	}
	if len(records) == 0 {
		return "", nil
	}
	data, err := json.Marshal(records)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func executionReferencePermissionsFromJSON(raw string) ([]*proto.WorkflowAccessPermission, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var records []workflowAccessPermissionRecord
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		return nil, err
	}
	out := make([]*proto.WorkflowAccessPermission, 0, len(records))
	for _, record := range records {
		pluginName := strings.TrimSpace(record.Plugin)
		if pluginName == "" {
			continue
		}
		out = append(out, &proto.WorkflowAccessPermission{
			Plugin:     pluginName,
			Operations: append([]string(nil), record.Operations...),
		})
	}
	return out, nil
}

func cloneExecutionReference(ref *proto.WorkflowExecutionReference) *proto.WorkflowExecutionReference {
	if ref == nil {
		return nil
	}
	return &proto.WorkflowExecutionReference{
		Id:                  ref.GetId(),
		ProviderName:        ref.GetProviderName(),
		Target:              cloneTarget(ref.GetTarget()),
		TargetFingerprint:   ref.GetTargetFingerprint(),
		SubjectId:           ref.GetSubjectId(),
		SubjectKind:         ref.GetSubjectKind(),
		DisplayName:         ref.GetDisplayName(),
		AuthSource:          ref.GetAuthSource(),
		CredentialSubjectId: ref.GetCredentialSubjectId(),
		Permissions:         cloneAccessPermissions(ref.GetPermissions()),
		CallerPluginName:    ref.GetCallerPluginName(),
		CreatedAt:           cloneTimestamp(ref.GetCreatedAt()),
		RevokedAt:           cloneTimestamp(ref.GetRevokedAt()),
	}
}

func cloneTarget(target *proto.BoundWorkflowTarget) *proto.BoundWorkflowTarget {
	if target == nil {
		return nil
	}
	return gproto.Clone(target).(*proto.BoundWorkflowTarget)
}

func cloneAgentTarget(target *proto.BoundWorkflowAgentTarget) *proto.BoundWorkflowAgentTarget {
	if target == nil {
		return nil
	}
	return gproto.Clone(target).(*proto.BoundWorkflowAgentTarget)
}

func pluginTargetProto(pluginName, operation, connection, instance string, input map[string]any) *proto.BoundWorkflowTarget {
	return &proto.BoundWorkflowTarget{
		Plugin: &proto.BoundWorkflowPluginTarget{
			PluginName: pluginName,
			Operation:  operation,
			Connection: connection,
			Instance:   instance,
			Input:      structFromAny(input),
		},
	}
}

func cloneAccessPermissions(values []*proto.WorkflowAccessPermission) []*proto.WorkflowAccessPermission {
	if len(values) == 0 {
		return nil
	}
	out := make([]*proto.WorkflowAccessPermission, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		out = append(out, &proto.WorkflowAccessPermission{
			Plugin:     value.GetPlugin(),
			Operations: append([]string(nil), value.GetOperations()...),
		})
	}
	return out
}

func timeToProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(value.UTC())
}

var _ gestalt.WorkflowProvider = (*Provider)(nil)
