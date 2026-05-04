package indexeddb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion           = "0.0.1-alpha.45"
	defaultPollInterval       = time.Second
	defaultWorkerCount        = 4
	defaultMaxSignalsPerBatch = 25
	defaultRunClaimTTL        = 10 * time.Minute
	defaultRunClaimRenewEvery = defaultRunClaimTTL / 3
	defaultStaleRecoveryEvery = time.Minute
	nonRunningRunClaimGrace   = time.Minute
	defaultAgentRunTimeout    = 5 * time.Minute
	agentRunStaleGrace        = time.Minute
	maxSignalAddRetries       = 4096

	storeSchedules     = "schedules"
	storeEventTriggers = "event_triggers"
	storeRuns          = "runs"
	storeRunClaims     = "workflow_run_claims"
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
	staleRunStatusMessage        = "workflow provider restarted while run was in progress"

	signalStatePending   = "pending"
	signalStateClaimed   = "claimed"
	signalStateDelivered = "delivered"
	signalStateFailed    = "failed"

	columnTypeString = gestalt.TypeString
	columnTypeInt    = gestalt.TypeInt
	columnTypeBool   = gestalt.TypeBool
	columnTypeTime   = gestalt.TypeTime
	columnTypeJSON   = gestalt.TypeJSON

	defaultSpecVersion = "1.0"
	defaultTimezone    = "UTC"
)

type config struct {
	PollInterval       time.Duration `yaml:"pollInterval"`
	WorkerCount        int           `yaml:"workerCount"`
	RunClaimTTL        time.Duration `yaml:"runClaimTTL"`
	RunClaimRenewEvery time.Duration `yaml:"runClaimRenewEvery"`
}

type Provider struct {
	proto.UnimplementedWorkflowProviderServer

	mu sync.RWMutex
	// workerMu serializes scheduler and worker claim work without blocking
	// foreground enqueue calls on the provider lifecycle lock.
	workerMu sync.Mutex
	// publishMu serializes event publication so deterministic event run IDs and
	// publisher-scoped execution refs stay consistent across duplicate publishes.
	publishMu sync.Mutex

	name              string
	cfg               config
	db                *gestalt.IndexedDBClient
	host              *gestalt.WorkflowHostClient
	scheduleStore     *gestalt.ObjectStoreClient
	eventTriggerStore *gestalt.ObjectStoreClient
	runStore          *gestalt.ObjectStoreClient
	runClaimStore     *gestalt.ObjectStoreClient
	idempotencyStore  *gestalt.ObjectStoreClient
	executionRefStore *gestalt.ObjectStoreClient
	workflowKeyStore  *gestalt.ObjectStoreClient
	signalStore       *gestalt.ObjectStoreClient
	claimOwnerID      string
	startedAt         time.Time
	pollCancel        context.CancelFunc
	pollDone          chan struct{}
	wake              chan string
	lastStaleRecovery time.Time

	now func() time.Time
}

type workflowScheduleRecord struct {
	ID           string
	Cron         string
	Timezone     string
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
	MatchType    string
	MatchSource  string
	MatchSubject string
	Target       *proto.BoundWorkflowTarget
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CreatedBy    *proto.WorkflowActor
	ExecutionRef string
}

type workflowRunRecord struct {
	ID                    string
	Status                proto.WorkflowRunStatus
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
	NextSignalSequence    int64
}

type workflowRunClaimRecord struct {
	ID        string
	RunID     string
	OwnerID   string
	ClaimedAt time.Time
	ExpiresAt time.Time
}

type workflowIdempotencyRecord struct {
	ID             string
	IdempotencyKey string
	RunID          string
	SignalID       string
	WorkflowKey    string
	StartedRun     bool
	CreatedAt      time.Time
}

type workflowKeyRecord struct {
	ID        string
	RunID     string
	CreatedAt time.Time
}

func (r workflowScheduleRecord) ownerKey() string {
	return targetOwnerKey(r.Target)
}

func (r workflowEventTriggerRecord) ownerKey() string {
	return targetOwnerKey(r.Target)
}

func (r workflowRunRecord) ownerKey() string {
	return targetOwnerKey(r.Target)
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
	Target              *proto.BoundWorkflowTarget
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
	OwnerKey string
	Target   *proto.BoundWorkflowTarget
}

func New() *Provider {
	return &Provider{now: time.Now, claimOwnerID: uuid.NewString()}
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

	host, err := gestalt.WorkflowHost()
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("indexeddb workflow: connect workflow host: %w", err)
	}

	cleanup := func() {
		_ = host.Close()
		_ = db.Close()
	}

	if err := ensureWorkflowStores(ctx, db); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: ensure stores: %w", err)
	}

	runStore := db.ObjectStore(storeRuns)
	runClaimStore := db.ObjectStore(storeRunClaims)
	workflowKeyStore := db.ObjectStore(storeWorkflowKeys)
	executionRefStore := db.ObjectStore(storeExecutionRefs)
	signalStore := db.ObjectStore(storeSignals)
	if err := validateWorkflowSignalIndexes(ctx, signalStore); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: validate signal indexes: %w", err)
	}

	p.mu.Lock()
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.db = db
	p.host = host
	p.scheduleStore = db.ObjectStore(storeSchedules)
	p.eventTriggerStore = db.ObjectStore(storeEventTriggers)
	p.runStore = runStore
	p.runClaimStore = runClaimStore
	p.idempotencyStore = db.ObjectStore(storeIdempotency)
	p.executionRefStore = executionRefStore
	p.workflowKeyStore = workflowKeyStore
	p.signalStore = signalStore
	if strings.TrimSpace(p.claimOwnerID) == "" {
		p.claimOwnerID = uuid.NewString()
	}
	p.mu.Unlock()

	return nil
}

func (p *Provider) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.workerMu.Lock()
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		p.workerMu.Unlock()
		return err
	}
	if p.pollCancel != nil {
		p.mu.Unlock()
		p.workerMu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		p.workerMu.Unlock()
		return err
	}

	startedAt := p.clock().UTC()
	workerCount := state.workerCount
	p.wake = make(chan string, max(128, workerCount*8))
	p.pollDone = make(chan struct{})
	loopCtx, cancel := context.WithCancel(context.Background())
	p.pollCancel = cancel
	p.startedAt = startedAt
	wake := p.wake
	pollInterval := p.cfg.PollInterval
	var wg sync.WaitGroup
	wg.Add(workerCount + 1)
	go func() {
		defer wg.Done()
		defer p.workerMu.Unlock()
		if err := recoverStaleWorkflowRunsWithTTL(loopCtx, state.db, state.runStore, state.runClaimStore, state.workflowKeyStore, state.signalStore, p.clock().UTC(), state.runClaimTTL); err != nil && loopCtx.Err() == nil {
			slog.WarnContext(loopCtx, "indexeddb workflow: recover stale workflow runs failed", "error", err)
			return
		}
		p.lastStaleRecovery = p.clock().UTC()
	}()
	for range workerCount {
		go func() {
			defer wg.Done()
			p.pollLoop(loopCtx, pollInterval, wake)
		}()
	}
	done := p.pollDone
	go func() {
		wg.Wait()
		close(done)
	}()
	p.signalWorkerLocked("")
	p.mu.Unlock()
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
	p.mu.RLock()
	store := p.runStore
	p.mu.RUnlock()
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

	p.name = ""
	p.cfg = config{}
	p.db = nil
	p.host = nil
	p.scheduleStore = nil
	p.eventTriggerStore = nil
	p.runStore = nil
	p.runClaimStore = nil
	p.idempotencyStore = nil
	p.executionRefStore = nil
	p.workflowKeyStore = nil
	p.signalStore = nil
	p.startedAt = time.Time{}
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

	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	actor := cloneActor(req.GetCreatedBy())
	key := strings.TrimSpace(req.GetIdempotencyKey())
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	if key != "" {
		existing, found, err := loadIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotency key: %v", err)
		}
		if found {
			run, found, err := loadRunRecord(ctx, state.runStore, target.OwnerKey, existing.RunID)
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
		runID = idempotentManualRunID(target.OwnerKey, key)
		run, found, err := loadRunRecord(ctx, state.runStore, target.OwnerKey, runID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotent run by deterministic id: %v", err)
		}
		if found {
			_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key, run.ID, now)
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
		stale, err := workflowRunRecoverablyStale(ctx, state.runClaimStore, active, now, state.runClaimTTL)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load workflow run claim: %v", err)
		}
		if found && stale {
			if err := failStaleRunningRun(ctx, state.runStore, state.workflowKeyStore, state.signalStore, active, workflowKey, now); err != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "recover stale workflow key: %v", err)
			}
			if err := state.runClaimStore.Delete(ctx, active.ID); err != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "delete stale workflow run claim: %v", err)
			}
			found = false
		}
		if found && workflowRunTerminal(active.Status) {
			_ = deleteWorkflowKeyRecordForRun(ctx, state.workflowKeyStore, workflowKey, active.ID)
			found = false
		}
		if found {
			p.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run", workflowKey)
		}
	}
	run := workflowRunRecord{
		ID:                 runID,
		Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:             cloneTarget(target.Target),
		TriggerKind:        triggerKindManual,
		CreatedAt:          now,
		CreatedBy:          actor,
		ExecutionRef:       strings.TrimSpace(req.GetExecutionRef()),
		WorkflowKey:        workflowKey,
		NextSignalSequence: 1,
	}
	if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
		if key != "" && errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := loadRunRecord(ctx, state.runStore, target.OwnerKey, run.ID)
			if loadErr != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load existing idempotent run: %v", loadErr)
			}
			if found {
				_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key, existing.ID, now)
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
		_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key, run.ID, now)
	}
	if workflowKey != "" {
		if err := storeWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey, run.ID, now); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "store workflow key: %v", err)
		}
	}
	resp, err := run.toProto()
	p.signalWorkerLocked(run.ID)
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, pluginName, runID)
	p.mu.RUnlock()
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	runs, err := listRunRecords(ctx, state.runStore, pluginName)
	p.mu.RUnlock()
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
		_ = deleteWorkflowKeyRecordForRun(ctx, state.workflowKeyStore, run.WorkflowKey, run.ID)
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

	p.mu.RLock()
	defer p.mu.RUnlock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var lastConflictID string
	for attempt := 0; attempt < maxSignalAddRetries; attempt++ {
		tx, stores, err := state.signalTransaction(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "start signal transaction: %v", err)
		}

		resp, signalID, err := signalRunInTransaction(ctx, stores, runID, signal)
		if err != nil {
			_ = tx.Abort(ctx)
			var conflict *workflowSignalAddConflictError
			if errors.As(err, &conflict) {
				lastConflictID = conflict.SignalID
				existing, found, loadErr := loadSignalRecord(ctx, state.signalStore, conflict.SignalID)
				if loadErr != nil {
					return nil, status.Errorf(codes.Internal, "load existing signal: %v", loadErr)
				}
				if found {
					if existing.RunID != runID {
						return nil, status.Errorf(codes.FailedPrecondition, "workflow signal %q belongs to a different run", conflict.SignalID)
					}
					if conflict.WorkflowKey != "" && existing.WorkflowKey != conflict.WorkflowKey {
						return nil, status.Errorf(codes.FailedPrecondition, "workflow signal %q belongs to a different workflow key", conflict.SignalID)
					}
					resp, err := signalRecordResponse(ctx, state.runStore, existing)
					if err != nil {
						return nil, err
					}
					return resp, nil
				}
				if conflict.RetrySequence {
					if err := yieldIndexedDBRetry(ctx); err != nil {
						return nil, status.FromContextError(err).Err()
					}
					continue
				}
				return nil, status.Errorf(codes.AlreadyExists, "workflow signal %q already exists", conflict.SignalID)
			}
			if indexedDBRetryableConflict(err) {
				if err := yieldIndexedDBRetry(ctx); err != nil {
					return nil, status.FromContextError(err).Err()
				}
				continue
			}
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			if indexedDBRetryableConflict(err) {
				if err := yieldIndexedDBRetry(ctx); err != nil {
					return nil, status.FromContextError(err).Err()
				}
				continue
			}
			return nil, status.Errorf(codes.Internal, "commit signal transaction: %v", err)
		}
		if signalID != "" {
			lastConflictID = signalID
		}
		p.signalWorkerLocked(runID)
		return resp, nil
	}
	if lastConflictID != "" {
		return nil, status.Errorf(codes.Aborted, "could not enqueue workflow signal %q after %d attempts", lastConflictID, maxSignalAddRetries)
	}
	return nil, status.Errorf(codes.Aborted, "could not enqueue workflow signal for run %q after %d attempts", runID, maxSignalAddRetries)
}

func (p *Provider) SignalOrStartRun(ctx context.Context, req *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	now := p.clock().UTC()
	signal, err := normalizeWorkflowSignal(req.GetSignal(), now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var preferredRunID string
	var lastConflictID string
	for attempt := 0; attempt < maxSignalAddRetries; attempt++ {
		tx, stores, err := state.signalOrStartTransaction(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "start signal transaction: %v", err)
		}

		resp, signalID, err := signalOrStartRunInTransaction(ctx, stores, target, req, workflowKey, signal, now, state.runClaimTTL)
		if err != nil {
			_ = tx.Abort(ctx)
			var conflict *workflowSignalAddConflictError
			if errors.As(err, &conflict) {
				lastConflictID = conflict.SignalID
				if conflict.SignalID != "" {
					existing, found, loadErr := loadSignalRecord(ctx, state.signalStore, conflict.SignalID)
					if loadErr != nil {
						return nil, status.Errorf(codes.Internal, "load existing signal: %v", loadErr)
					}
					if found {
						if conflict.RunID != "" && existing.RunID != conflict.RunID {
							return nil, status.Errorf(codes.FailedPrecondition, "workflow signal %q belongs to a different run", conflict.SignalID)
						}
						if existing.WorkflowKey != workflowKey {
							return nil, status.Errorf(codes.FailedPrecondition, "workflow signal %q belongs to a different workflow key", conflict.SignalID)
						}
						resp, err := signalRecordResponse(ctx, state.runStore, existing)
						if err != nil {
							return nil, err
						}
						return resp, nil
					}
				}
				if conflict.RetrySequence {
					if err := yieldIndexedDBRetry(ctx); err != nil {
						return nil, status.FromContextError(err).Err()
					}
					continue
				}
				return nil, status.Errorf(codes.AlreadyExists, "workflow signal %q already exists", conflict.SignalID)
			}
			if indexedDBRetryableConflict(err) {
				if err := yieldIndexedDBRetry(ctx); err != nil {
					return nil, status.FromContextError(err).Err()
				}
				continue
			}
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			if indexedDBRetryableConflict(err) {
				if err := yieldIndexedDBRetry(ctx); err != nil {
					return nil, status.FromContextError(err).Err()
				}
				continue
			}
			return nil, status.Errorf(codes.Internal, "commit signal transaction: %v", err)
		}
		preferredRunID = resp.GetRun().GetId()
		if signalID != "" {
			lastConflictID = signalID
		}
		p.signalWorkerLocked(preferredRunID)
		return resp, nil
	}
	if lastConflictID != "" {
		return nil, status.Errorf(codes.Aborted, "could not enqueue workflow signal %q after %d attempts", lastConflictID, maxSignalAddRetries)
	}
	return nil, status.Errorf(codes.Aborted, "could not enqueue workflow signal for workflow key %q after %d attempts", workflowKey, maxSignalAddRetries)
}

func (p *Provider) UpsertSchedule(ctx context.Context, req *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.GetTarget())
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
	existing, found, err := loadScheduleRecord(ctx, state.scheduleStore, target.OwnerKey, scheduleID)
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
		if otherFound && other.ownerKey() != target.OwnerKey {
			p.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "workflow schedule %q is already owned by target owner %q", scheduleID, other.ownerKey())
		}
	}

	now := p.clock().UTC()
	record := workflowScheduleRecord{
		ID:           scheduleID,
		Cron:         cronSpec,
		Timezone:     timezone,
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
	p.signalWorkerLocked("")
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadScheduleRecord(ctx, state.scheduleStore, pluginName, scheduleID)
	p.mu.RUnlock()
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listScheduleRecords(ctx, state.scheduleStore, pluginName)
	p.mu.RUnlock()
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
	target, err := normalizeTarget(req.GetTarget())
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
	existing, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, target.OwnerKey, triggerID)
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
		if otherFound && other.ownerKey() != target.OwnerKey {
			p.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "workflow event trigger %q is already owned by target owner %q", triggerID, other.ownerKey())
		}
	}
	now := p.clock().UTC()
	record := workflowEventTriggerRecord{
		ID:           triggerID,
		MatchType:    matchType,
		MatchSource:  matchSource,
		MatchSubject: matchSubject,
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, pluginName, triggerID)
	p.mu.RUnlock()
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listEventTriggerRecords(ctx, state.eventTriggerStore, pluginName)
	p.mu.RUnlock()
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

	p.publishMu.Lock()
	defer p.publishMu.Unlock()

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	triggers, err := listEventTriggerRecords(ctx, state.eventTriggerStore, pluginName)
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	now := p.clock().UTC()
	providerName := strings.TrimSpace(p.name)
	publishedBy := cloneActor(req.GetPublishedBy())
	enqueued := false
	preferredRunID := ""
	for _, trigger := range triggers {
		if trigger.Paused || !eventMatchesTrigger(event, trigger) {
			continue
		}
		runID := uuid.NewString()
		if strings.TrimSpace(event.GetId()) != "" {
			runID = eventRunID(trigger.ID, event.GetSource(), event.GetId())
		}
		if _, found, err := loadRunRecord(ctx, state.runStore, trigger.ownerKey(), runID); err != nil {
			p.mu.RUnlock()
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
				p.mu.RUnlock()
				return nil, status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				record, err := executionReferenceRecordFromProto(ref)
				if err != nil {
					p.mu.RUnlock()
					return nil, status.Errorf(codes.Internal, "build event execution reference record: %v", err)
				}
				if err := state.executionRefStore.Add(ctx, record.toRecord()); err != nil {
					if !errors.Is(err, gestalt.ErrAlreadyExists) {
						p.mu.RUnlock()
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
			Status:                proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:                cloneTarget(trigger.Target),
			TriggerKind:           triggerKindEvent,
			TriggerEventTriggerID: trigger.ID,
			TriggerEvent:          cloneEvent(event),
			CreatedAt:             now,
			CreatedBy:             createdBy,
			ExecutionRef:          executionRef,
			NextSignalSequence:    1,
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			if errors.Is(err, gestalt.ErrAlreadyExists) {
				continue
			}
			if createdExecutionRef {
				_ = state.executionRefStore.Delete(ctx, executionRef)
			}
			p.mu.RUnlock()
			return nil, status.Errorf(codes.Internal, "enqueue workflow run: %v", err)
		}
		enqueued = true
		if preferredRunID == "" {
			preferredRunID = run.ID
		}
	}
	if enqueued {
		p.signalWorkerLocked(preferredRunID)
	}
	p.mu.RUnlock()
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

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if existing, found, err := loadExecutionReferenceRecord(ctx, state.executionRefStore, record.ID); err != nil {
		p.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "load execution reference: %v", err)
	} else if found {
		if !existing.CreatedAt.IsZero() {
			record.CreatedAt = existing.CreatedAt
		}
		if executionReferenceRecordsEqual(existing, record) {
			resp, err := existing.toProto()
			p.mu.RUnlock()
			if err != nil {
				return nil, status.Errorf(codes.Internal, "build execution reference response: %v", err)
			}
			return resp, nil
		}
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = p.clock().UTC()
	}
	if err := state.executionRefStore.Put(ctx, record.toRecord()); err != nil {
		if errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := loadExecutionReferenceRecord(ctx, state.executionRefStore, record.ID)
			if loadErr != nil {
				p.mu.RUnlock()
				return nil, status.Errorf(codes.Internal, "load existing execution reference after conflict: %v", loadErr)
			}
			if found {
				if !existing.CreatedAt.IsZero() {
					record.CreatedAt = existing.CreatedAt
				}
				if executionReferenceRecordsEqual(existing, record) {
					resp, buildErr := existing.toProto()
					p.mu.RUnlock()
					if buildErr != nil {
						return nil, status.Errorf(codes.Internal, "build execution reference response: %v", buildErr)
					}
					return resp, nil
				}
			}
		}
		p.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "put execution reference: %v", err)
	}
	resp, err := record.toProto()
	p.mu.RUnlock()
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
	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadExecutionReferenceRecord(ctx, state.executionRefStore, refID)
	p.mu.RUnlock()
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
	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listExecutionReferenceRecords(ctx, state.executionRefStore, subjectID)
	p.mu.RUnlock()
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

type workflowSignalAddConflictError struct {
	SignalID      string
	RunID         string
	WorkflowKey   string
	RetrySequence bool
	err           error
}

func (e *workflowSignalAddConflictError) Error() string {
	if e.SignalID != "" {
		return fmt.Sprintf("workflow signal %q already exists", e.SignalID)
	}
	return "workflow signal already exists"
}

func (e *workflowSignalAddConflictError) Unwrap() error {
	if e.err != nil {
		return e.err
	}
	return gestalt.ErrAlreadyExists
}

func signalRunInTransaction(ctx context.Context, stores workflowSignalTxStores, runID string, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, string, error) {
	run, found, err := loadRunRecordTx(ctx, stores.runStore, "", runID)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		return nil, "", status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	if workflowRunTerminal(run.Status) {
		return nil, "", status.Errorf(codes.FailedPrecondition, "workflow run %q is %s", runID, run.Status.String())
	}
	if key := strings.TrimSpace(signal.GetIdempotencyKey()); key != "" {
		existing, found, err := loadIdempotencyRecordTx(ctx, stores.idempotencyStore, run.ownerKey(), key)
		if err != nil {
			return nil, "", status.Errorf(codes.Internal, "load signal idempotency key: %v", err)
		}
		if found && existing.SignalID != "" {
			resp, _, err := signalIdempotencyResponseTx(ctx, stores.runStore, stores.signalStore, run.ownerKey(), "", existing)
			return resp, existing.SignalID, err
		}
	}
	return enqueueSignalInTransaction(ctx, stores.runStore, stores.idempotencyStore, stores.signalStore, run, signal, false)
}

func signalOrStartRunInTransaction(ctx context.Context, stores workflowSignalOrStartTxStores, target scopedTarget, req *proto.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *proto.WorkflowSignal, now time.Time, runClaimTTL time.Duration) (*proto.SignalWorkflowRunResponse, string, error) {
	startedRun := false
	enqueueSignal := signal
	run, active, err := activeWorkflowKeyRunInTransaction(ctx, stores.workflowKeyStore, stores.runStore, workflowKey)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "load workflow key: %v", err)
	}
	stale, err := workflowRunRecoverablyStaleTx(ctx, stores.runClaimStore, run, now, runClaimTTL)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "load workflow run claim: %v", err)
	}
	if active && stale {
		if err := failStaleRunningRunTx(ctx, stores.runStore, stores.workflowKeyStore, stores.signalStore, run, workflowKey, now); err != nil {
			return nil, "", status.Errorf(codes.Internal, "recover stale workflow key: %v", err)
		}
		if err := stores.runClaimStore.Delete(ctx, run.ID); err != nil {
			return nil, "", status.Errorf(codes.Internal, "delete stale workflow run claim: %v", err)
		}
		active = false
	}
	if active && workflowRunTerminal(run.Status) {
		if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, workflowKey, run.ID); err != nil {
			return nil, "", status.Errorf(codes.Internal, "delete terminal workflow key: %v", err)
		}
		active = false
	}
	if key := strings.TrimSpace(signal.GetIdempotencyKey()); key != "" {
		existing, found, err := loadIdempotencyRecordTx(ctx, stores.idempotencyStore, target.OwnerKey, key)
		if err != nil {
			return nil, "", status.Errorf(codes.Internal, "load signal idempotency key: %v", err)
		}
		if found && existing.SignalID != "" {
			resp, reusable, err := signalIdempotencyResponseTx(ctx, stores.runStore, stores.signalStore, target.OwnerKey, workflowKey, existing)
			if err != nil || reusable {
				return resp, existing.SignalID, err
			}
			if strings.TrimSpace(enqueueSignal.GetId()) == existing.SignalID {
				enqueueSignal = cloneSignal(enqueueSignal)
				enqueueSignal.Id = ""
			}
		}
	}
	if !active {
		startedRun = true
		run = workflowRunRecord{
			ID:                 uuid.NewString(),
			Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:             cloneTarget(target.Target),
			TriggerKind:        triggerKindManual,
			CreatedAt:          now,
			CreatedBy:          cloneActor(req.GetCreatedBy()),
			ExecutionRef:       strings.TrimSpace(req.GetExecutionRef()),
			WorkflowKey:        workflowKey,
			NextSignalSequence: 1,
		}
		if err := stores.runStore.Add(ctx, run.toRecord()); err != nil {
			if indexedDBRetryableConflict(err) {
				return nil, "", err
			}
			return nil, "", status.Errorf(codes.Internal, "create workflow run: %v", err)
		}
		if err := addWorkflowKeyRecord(ctx, stores.workflowKeyStore, workflowKey, run.ID, now); err != nil {
			if indexedDBRetryableConflict(err) {
				return nil, "", err
			}
			return nil, "", status.Errorf(codes.Internal, "store workflow key: %v", err)
		}
	}
	// The existing keyed run owns the execution ref and target. Later signals
	// deliberately do not replace that context, even if the caller's current
	// config would build a different target.
	return enqueueSignalInTransaction(ctx, stores.runStore, stores.idempotencyStore, stores.signalStore, run, enqueueSignal, startedRun)
}

func enqueueSignalInTransaction(ctx context.Context, runStore recordPutter, idempotencyStore recordPutter, signalStore *gestalt.TransactionObjectStore, run workflowRunRecord, signal *proto.WorkflowSignal, startedRun bool) (*proto.SignalWorkflowRunResponse, string, error) {
	signal = cloneSignal(signal)
	if strings.TrimSpace(signal.GetId()) == "" {
		signal.Id = workflowSignalID(run, signal)
	}
	assignSequence := signal.GetSequence() == 0
	advanceSequence := false

	if assignSequence {
		if run.NextSignalSequence <= 0 {
			return nil, signal.GetId(), status.Errorf(codes.FailedPrecondition, "workflow run %q has invalid next_signal_sequence", run.ID)
		}
		signal.Sequence = run.NextSignalSequence
		run.NextSignalSequence++
		advanceSequence = true
	} else if signal.GetSequence() >= run.NextSignalSequence {
		run.NextSignalSequence = signal.GetSequence() + 1
		advanceSequence = true
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
	if err := signalStore.Add(ctx, record.toRecord()); err != nil {
		if indexedDBAlreadyExists(err) {
			return nil, record.ID, &workflowSignalAddConflictError{
				SignalID:      record.ID,
				RunID:         run.ID,
				WorkflowKey:   run.WorkflowKey,
				RetrySequence: assignSequence,
				err:           err,
			}
		}
		return nil, record.ID, status.Errorf(codes.Internal, "store signal: %v", err)
	}
	if advanceSequence {
		if err := runStore.Put(ctx, run.toRecord()); err != nil {
			return nil, record.ID, status.Errorf(codes.Internal, "advance signal sequence: %v", err)
		}
	}

	if key := strings.TrimSpace(record.IdempotencyKey); key != "" {
		if err := storeSignalIdempotencyRecord(ctx, idempotencyStore, run.ownerKey(), key, run.ID, record.ID, run.WorkflowKey, record.StartedRun, record.CreatedAt); err != nil {
			return nil, record.ID, status.Errorf(codes.Internal, "store signal idempotency key: %v", err)
		}
	}
	pbRun, err := run.toProto()
	if err != nil {
		return nil, record.ID, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal,
		StartedRun:  record.StartedRun,
		WorkflowKey: run.WorkflowKey,
	}, record.ID, nil
}

func signalIdempotencyResponseTx(ctx context.Context, runStore, signalStore *gestalt.TransactionObjectStore, pluginName, workflowKey string, record workflowIdempotencyRecord) (*proto.SignalWorkflowRunResponse, bool, error) {
	run, found, err := loadRunRecordTx(ctx, runStore, pluginName, record.RunID)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "load idempotent signal run: %v", err)
	}
	if !found {
		return nil, false, status.Errorf(codes.NotFound, "workflow run %q not found", record.RunID)
	}
	signal, found, err := loadSignalRecordTx(ctx, signalStore, record.SignalID)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "load idempotent signal: %v", err)
	}
	if !found {
		return nil, false, status.Errorf(codes.NotFound, "workflow signal %q not found", record.SignalID)
	}
	if signalIdempotencyRecordIsStaleRecovery(run, signal, record, workflowKey) {
		return nil, false, nil
	}
	pbRun, err := run.toProto()
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	responseWorkflowKey := strings.TrimSpace(record.WorkflowKey)
	if responseWorkflowKey == "" {
		responseWorkflowKey = run.WorkflowKey
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal.signalProto(),
		StartedRun:  record.StartedRun,
		WorkflowKey: responseWorkflowKey,
	}, true, nil
}

func signalIdempotencyRecordIsStaleRecovery(run workflowRunRecord, signal workflowSignalRecord, record workflowIdempotencyRecord, workflowKey string) bool {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return false
	}
	if firstNonEmpty(record.WorkflowKey, signal.WorkflowKey, run.WorkflowKey) != workflowKey {
		return false
	}
	return run.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED &&
		run.StatusMessage == staleRunStatusMessage &&
		signal.State == signalStateFailed &&
		signal.StatusMessage == staleRunStatusMessage
}

func signalRecordResponse(ctx context.Context, runStore recordGetter, signal workflowSignalRecord) (*proto.SignalWorkflowRunResponse, error) {
	run, found, err := loadRunRecord(ctx, runStore, "", signal.RunID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load signal run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", signal.RunID)
	}
	pbRun, err := run.toProto()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	workflowKey := strings.TrimSpace(signal.WorkflowKey)
	if workflowKey == "" {
		workflowKey = run.WorkflowKey
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal.signalProto(),
		StartedRun:  signal.StartedRun,
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
		p.signalWorkerLocked("")
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

func (p *Provider) pollLoop(ctx context.Context, pollInterval time.Duration, wake <-chan string) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		preferredRunID := ""
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case preferredRunID = <-wake:
			preferredRunID = drainPreferredRunID(preferredRunID, wake)
		}
		if ctx.Err() != nil {
			return
		}
		p.logTickError(ctx, p.tick(ctx, preferredRunID))
	}
}

func drainPreferredRunID(preferredRunID string, wake <-chan string) string {
	preferredRunID = strings.TrimSpace(preferredRunID)
	for {
		select {
		case next := <-wake:
			if next = strings.TrimSpace(next); next != "" {
				preferredRunID = next
			}
		default:
			return preferredRunID
		}
	}
}

func (p *Provider) logTickError(ctx context.Context, err error) {
	if err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "indexeddb workflow tick failed", "provider", p.providerName(), "error", err)
	}
}

func (p *Provider) tick(ctx context.Context, preferredRunID string) error {
	if strings.TrimSpace(preferredRunID) != "" {
		processed, err := p.processNextPendingRun(ctx, preferredRunID)
		if err != nil {
			return err
		}
		if processed {
			p.mu.RLock()
			p.signalWorkerLocked("")
			p.mu.RUnlock()
			return nil
		}
	}

	if err := p.recoverStaleWorkflowRunsIfDue(ctx); err != nil {
		slog.WarnContext(ctx, "indexeddb workflow: recover stale workflow runs failed", "provider", p.providerName(), "error", err)
	}
	if err := p.enqueueDueSchedules(ctx); err != nil {
		return err
	}
	processed, err := p.processNextPendingRun(ctx, "")
	if err != nil {
		return err
	}
	if processed {
		p.mu.RLock()
		p.signalWorkerLocked("")
		p.mu.RUnlock()
	}
	return nil
}

func (p *Provider) recoverStaleWorkflowRuns(ctx context.Context) error {
	return p.recoverStaleWorkflowRunsLocked(ctx, false)
}

func (p *Provider) recoverStaleWorkflowRunsIfDue(ctx context.Context) error {
	return p.recoverStaleWorkflowRunsLocked(ctx, true)
}

func (p *Provider) recoverStaleWorkflowRunsLocked(ctx context.Context, onlyIfDue bool) error {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	p.mu.RLock()
	defer p.mu.RUnlock()

	state, err := p.requireConfiguredLocked()
	if err != nil {
		return err
	}
	now := p.clock().UTC()
	if onlyIfDue && !p.lastStaleRecovery.IsZero() && now.Sub(p.lastStaleRecovery) < defaultStaleRecoveryEvery {
		return nil
	}
	p.lastStaleRecovery = now
	return recoverStaleWorkflowRunsWithTTL(ctx, state.db, state.runStore, state.runClaimStore, state.workflowKeyStore, state.signalStore, now, state.runClaimTTL)
}

func (p *Provider) enqueueDueSchedules(ctx context.Context) error {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	p.mu.RLock()
	defer p.mu.RUnlock()

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
	preferredRunID := ""

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
			Status:              proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:              cloneTarget(schedule.Target),
			TriggerKind:         triggerKindSchedule,
			TriggerScheduleID:   schedule.ID,
			TriggerScheduledFor: timePtr(latestDue),
			CreatedAt:           now,
			CreatedBy:           cloneActor(schedule.CreatedBy),
			ExecutionRef:        schedule.ExecutionRef,
			NextSignalSequence:  1,
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
		if preferredRunID == "" {
			preferredRunID = run.ID
		}
	}
	if enqueued {
		p.signalWorkerLocked(preferredRunID)
	}
	return nil
}

func (p *Provider) processNextPendingRun(ctx context.Context, preferredRunID string) (bool, error) {
	p.workerMu.Lock()
	p.mu.RLock()
	releaseWorker := func() {
		p.mu.RUnlock()
		p.workerMu.Unlock()
	}
	state, err := p.requireConfiguredLocked()
	if err != nil {
		releaseWorker()
		return false, err
	}
	now := p.clock().UTC()
	pending, found, err := nextClaimedRunnableRun(ctx, state, preferredRunID, now)
	if err != nil {
		releaseWorker()
		return false, err
	}
	if !found {
		releaseWorker()
		return false, nil
	}
	claimOwnerID := state.claimOwnerID
	runClaimRenewEvery := state.runClaimRenewEvery
	releaseUninvokedClaim := func(retErr error) error {
		if err := state.runClaimStore.Delete(ctx, pending.ID); err != nil {
			return errors.Join(retErr, fmt.Errorf("release workflow run claim %q: %w", pending.ID, err))
		}
		return retErr
	}
	var claimedSignals []workflowSignalRecord
	if pending.WorkflowKey != "" {
		claimedSignals, err = listSignalRecordsLimit(ctx, state.signalStore, pending.ID, signalStateClaimed, defaultMaxSignalsPerBatch)
		if err != nil {
			err = releaseUninvokedClaim(err)
			releaseWorker()
			return false, err
		}
		if len(claimedSignals) == 0 {
			pendingSignals, err := listSignalRecordsLimit(ctx, state.signalStore, pending.ID, signalStatePending, defaultMaxSignalsPerBatch)
			if err != nil {
				err = releaseUninvokedClaim(err)
				releaseWorker()
				return false, err
			}
			if len(pendingSignals) == 0 {
				err = releaseUninvokedClaim(nil)
				releaseWorker()
				return false, err
			}
			batchID := uuid.NewString()
			for _, signal := range pendingSignals {
				signal.State = signalStateClaimed
				signal.BatchID = batchID
				signal.ClaimedAt = &now
				if err := state.signalStore.Put(ctx, signal.toRecord()); err != nil {
					err = releaseUninvokedClaim(err)
					releaseWorker()
					return false, err
				}
				claimedSignals = append(claimedSignals, signal)
			}
		}
	}
	if pending.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		pending.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
		pending.StartedAt = &now
		pending.CompletedAt = nil
		pending.StatusMessage = ""
		if err := state.runStore.Put(ctx, pending.toRecord()); err != nil {
			err = releaseUninvokedClaim(err)
			releaseWorker()
			return false, err
		}
	}
	host := state.host
	releaseWorker()

	stopRenewingClaim := p.startRunClaimRenewal(ctx, pending.ID, claimOwnerID, runClaimRenewEvery)
	resp, invokeErr := host.InvokeOperation(ctx, &proto.InvokeWorkflowOperationRequest{
		Target:       cloneTarget(pending.Target),
		RunId:        pending.ID,
		Trigger:      pending.triggerProto(),
		CreatedBy:    cloneActor(pending.CreatedBy),
		ExecutionRef: pending.ExecutionRef,
		Signals:      signalProtos(claimedSignals),
	})
	stopRenewingClaim()

	for attempt := 0; attempt < maxSignalAddRetries; attempt++ {
		if err := p.completeRunAfterInvoke(ctx, pending, claimedSignals, claimOwnerID, resp, invokeErr); err != nil {
			if indexedDBRetryableConflict(err) {
				if err := yieldIndexedDBRetry(ctx); err != nil {
					return true, err
				}
				continue
			}
			return true, err
		}
		return true, nil
	}
	return true, status.Errorf(codes.Aborted, "could not complete workflow run %q after %d attempts", pending.ID, maxSignalAddRetries)
}

func (p *Provider) startRunClaimRenewal(ctx context.Context, runID, ownerID string, renewEvery time.Duration) func() {
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if renewEvery <= 0 {
			renewEvery = defaultRunClaimRenewEvery
		}
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				renewed, err := p.renewWorkflowRunClaim(renewCtx, runID, ownerID)
				if err != nil && renewCtx.Err() == nil {
					slog.WarnContext(renewCtx, "indexeddb workflow: renew run claim failed", "provider", p.providerName(), "run_id", runID, "error", err)
					continue
				}
				if !renewed {
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (p *Provider) renewWorkflowRunClaim(ctx context.Context, runID, ownerID string) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		return false, err
	}
	return renewWorkflowRunClaim(ctx, state.db, runID, ownerID, p.clock().UTC(), state.runClaimTTL)
}

func (p *Provider) completeRunAfterInvoke(ctx context.Context, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, resp *proto.InvokeWorkflowOperationResponse, invokeErr error) error {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	p.mu.RLock()
	defer p.mu.RUnlock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		return err
	}
	tx, stores, err := state.runCompletionTransaction(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	completedAt := p.clock().UTC()
	if err := completeRunInTransaction(ctx, stores, pending, claimedSignals, claimOwnerID, resp, invokeErr, completedAt); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func completeRunInTransaction(ctx context.Context, stores workflowRunCompletionTxStores, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, resp *proto.InvokeWorkflowOperationResponse, invokeErr error, completedAt time.Time) error {
	claim, claimFound, err := loadRunClaimRecordTx(ctx, stores.runClaimStore, pending.ID)
	if err != nil {
		return err
	}
	if !claimFound || claim.OwnerID != strings.TrimSpace(claimOwnerID) {
		return nil
	}
	current, found, err := loadRunRecordTx(ctx, stores.runStore, pending.ownerKey(), pending.ID)
	if err != nil {
		return err
	}
	if !found {
		return stores.runClaimStore.Delete(ctx, pending.ID)
	}
	if err := stores.runClaimStore.Delete(ctx, pending.ID); err != nil {
		return err
	}
	if current.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		return stores.runStore.Put(ctx, current.toRecord())
	}
	current.CompletedAt = &completedAt
	if invokeErr != nil {
		current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
		current.StatusMessage = invokeErr.Error()
		if err := markRunSignalsFailedTx(ctx, stores.signalStore, current.ID, claimedSignals, completedAt, invokeErr.Error()); err != nil {
			return err
		}
		if current.WorkflowKey != "" {
			if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
				return err
			}
		}
	} else {
		current.ResultBody = resp.GetBody()
		if resp.GetStatus() >= http.StatusBadRequest {
			current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			current.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			if err := markRunSignalsFailedTx(ctx, stores.signalStore, current.ID, claimedSignals, completedAt, current.StatusMessage); err != nil {
				return err
			}
			if current.WorkflowKey != "" {
				if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
					return err
				}
			}
		} else {
			if err := markSignalsDeliveredTx(ctx, stores.signalStore, claimedSignals, completedAt); err != nil {
				return err
			}
			hasPending, err := hasPendingSignalsTx(ctx, stores.signalStore, current.ID)
			if err != nil {
				return err
			}
			if current.WorkflowKey != "" && hasPending {
				current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				current.CompletedAt = nil
			} else {
				current.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
				if current.WorkflowKey != "" {
					if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
						return err
					}
				}
			}
			current.StatusMessage = ""
		}
	}
	return stores.runStore.Put(ctx, current.toRecord())
}

func (p *Provider) requireConfiguredLocked() (*configuredState, error) {
	if p.db == nil || p.runStore == nil || p.runClaimStore == nil || p.scheduleStore == nil || p.eventTriggerStore == nil || p.idempotencyStore == nil || p.executionRefStore == nil || p.workflowKeyStore == nil || p.signalStore == nil || p.host == nil {
		return nil, errors.New("indexeddb workflow: provider is not configured")
	}
	return &configuredState{
		db:                 p.db,
		host:               p.host,
		scheduleStore:      p.scheduleStore,
		eventTriggerStore:  p.eventTriggerStore,
		runStore:           p.runStore,
		runClaimStore:      p.runClaimStore,
		idempotencyStore:   p.idempotencyStore,
		executionRefStore:  p.executionRefStore,
		workflowKeyStore:   p.workflowKeyStore,
		signalStore:        p.signalStore,
		claimOwnerID:       p.claimOwnerID,
		startedAt:          p.startedAt,
		workerCount:        p.cfg.WorkerCount,
		runClaimTTL:        p.cfg.RunClaimTTL,
		runClaimRenewEvery: p.cfg.RunClaimRenewEvery,
	}, nil
}

func (p *Provider) signalWorkerLocked(preferredRunID string) {
	if p.wake == nil {
		return
	}
	preferredRunID = strings.TrimSpace(preferredRunID)
	select {
	case p.wake <- preferredRunID:
	default:
		if preferredRunID == "" {
			return
		}
		select {
		case <-p.wake:
		default:
		}
		select {
		case p.wake <- preferredRunID:
		default:
		}
	}
}

func (p *Provider) clock() time.Time {
	if p.now == nil {
		return time.Now()
	}
	return p.now()
}

func (p *Provider) providerName() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return strings.TrimSpace(p.name)
}

type configuredState struct {
	db                 *gestalt.IndexedDBClient
	host               *gestalt.WorkflowHostClient
	scheduleStore      *gestalt.ObjectStoreClient
	eventTriggerStore  *gestalt.ObjectStoreClient
	runStore           *gestalt.ObjectStoreClient
	runClaimStore      *gestalt.ObjectStoreClient
	idempotencyStore   *gestalt.ObjectStoreClient
	executionRefStore  *gestalt.ObjectStoreClient
	workflowKeyStore   *gestalt.ObjectStoreClient
	signalStore        *gestalt.ObjectStoreClient
	claimOwnerID       string
	startedAt          time.Time
	workerCount        int
	runClaimTTL        time.Duration
	runClaimRenewEvery time.Duration
}

type recordGetter interface {
	Get(context.Context, string) (gestalt.Record, error)
}

type recordAdder interface {
	Add(context.Context, gestalt.Record) error
}

type recordPutter interface {
	Put(context.Context, gestalt.Record) error
}

type recordDeleter interface {
	Delete(context.Context, string) error
}

func transactionGetRecord(ctx context.Context, store *gestalt.TransactionObjectStore, id string) (gestalt.Record, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, false, nil
	}
	records, err := store.GetAll(ctx, &gestalt.KeyRange{Lower: id, Upper: id})
	if err != nil {
		return nil, false, err
	}
	for _, record := range records {
		if fmt.Sprint(record["id"]) == id {
			return record, true, nil
		}
	}
	return nil, false, nil
}

type workflowSignalOrStartTxStores struct {
	runStore         *gestalt.TransactionObjectStore
	runClaimStore    *gestalt.TransactionObjectStore
	idempotencyStore *gestalt.TransactionObjectStore
	workflowKeyStore *gestalt.TransactionObjectStore
	signalStore      *gestalt.TransactionObjectStore
}

func (s *configuredState) signalOrStartTransaction(ctx context.Context) (*gestalt.Transaction, workflowSignalOrStartTxStores, error) {
	tx, err := s.db.Transaction(ctx, []string{
		storeRuns,
		storeRunClaims,
		storeIdempotency,
		storeWorkflowKeys,
		storeSignals,
	}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, workflowSignalOrStartTxStores{}, err
	}
	return tx, workflowSignalOrStartTxStores{
		runStore:         tx.ObjectStore(storeRuns),
		runClaimStore:    tx.ObjectStore(storeRunClaims),
		idempotencyStore: tx.ObjectStore(storeIdempotency),
		workflowKeyStore: tx.ObjectStore(storeWorkflowKeys),
		signalStore:      tx.ObjectStore(storeSignals),
	}, nil
}

type workflowSignalTxStores struct {
	runStore         *gestalt.TransactionObjectStore
	idempotencyStore *gestalt.TransactionObjectStore
	signalStore      *gestalt.TransactionObjectStore
}

func (s *configuredState) signalTransaction(ctx context.Context) (*gestalt.Transaction, workflowSignalTxStores, error) {
	tx, err := s.db.Transaction(ctx, []string{
		storeRuns,
		storeIdempotency,
		storeSignals,
	}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, workflowSignalTxStores{}, err
	}
	return tx, workflowSignalTxStores{
		runStore:         tx.ObjectStore(storeRuns),
		idempotencyStore: tx.ObjectStore(storeIdempotency),
		signalStore:      tx.ObjectStore(storeSignals),
	}, nil
}

type workflowRunCompletionTxStores struct {
	runStore         *gestalt.TransactionObjectStore
	runClaimStore    *gestalt.TransactionObjectStore
	workflowKeyStore *gestalt.TransactionObjectStore
	signalStore      *gestalt.TransactionObjectStore
}

func (s *configuredState) runCompletionTransaction(ctx context.Context) (*gestalt.Transaction, workflowRunCompletionTxStores, error) {
	tx, err := s.db.Transaction(ctx, []string{
		storeRuns,
		storeRunClaims,
		storeWorkflowKeys,
		storeSignals,
	}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return nil, workflowRunCompletionTxStores{}, err
	}
	return tx, workflowRunCompletionTxStores{
		runStore:         tx.ObjectStore(storeRuns),
		runClaimStore:    tx.ObjectStore(storeRunClaims),
		workflowKeyStore: tx.ObjectStore(storeWorkflowKeys),
		signalStore:      tx.ObjectStore(storeSignals),
	}, nil
}

func claimWorkflowRun(ctx context.Context, db *gestalt.IndexedDBClient, runID, ownerID string, now time.Time, runClaimTTL time.Duration) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, errors.New("workflow run claim requires run id")
	}
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		ownerID = "unknown"
	}
	tx, err := db.Transaction(ctx, []string{storeRunClaims}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return false, nil
		}
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeRunClaims)
	existing, found, err := loadRunClaimRecordTx(ctx, store, runID)
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return false, nil
		}
		return false, err
	}
	if found {
		if existing.ExpiresAt.After(now) {
			if err := tx.Abort(ctx); err != nil && !errors.Is(err, gestalt.ErrTransactionDone) {
				return false, err
			}
			committed = true
			return false, nil
		}
		if err := store.Delete(ctx, runID); err != nil {
			if indexedDBRetryableConflict(err) {
				return false, nil
			}
			return false, err
		}
	}
	claim := workflowRunClaimRecord{
		ID:        runID,
		RunID:     runID,
		OwnerID:   ownerID,
		ClaimedAt: now.UTC(),
		ExpiresAt: now.Add(runClaimTTL).UTC(),
	}
	if err := store.Add(ctx, claim.toRecord()); err != nil {
		if indexedDBRetryableConflict(err) {
			return false, nil
		}
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if indexedDBRetryableConflict(err) {
			return false, nil
		}
		return false, err
	}
	committed = true
	return true, nil
}

func claimAndStartPendingRun(ctx context.Context, db *gestalt.IndexedDBClient, expected workflowRunRecord, ownerID string, now time.Time, runClaimTTL time.Duration) (workflowRunRecord, bool, error) {
	runID := strings.TrimSpace(expected.ID)
	if runID == "" {
		return workflowRunRecord{}, false, errors.New("workflow run claim requires run id")
	}
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		ownerID = "unknown"
	}
	tx, err := db.Transaction(ctx, []string{storeRuns, storeRunClaims}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	runStore := tx.ObjectStore(storeRuns)
	claimStore := tx.ObjectStore(storeRunClaims)
	run, found, err := loadRunRecordTx(ctx, runStore, expected.ownerKey(), runID)
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	if !found ||
		run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING ||
		strings.TrimSpace(run.WorkflowKey) != "" {
		if err := tx.Abort(ctx); err != nil && !errors.Is(err, gestalt.ErrTransactionDone) {
			return workflowRunRecord{}, false, err
		}
		committed = true
		return workflowRunRecord{}, false, nil
	}
	existing, found, err := loadRunClaimRecordTx(ctx, claimStore, runID)
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	if found {
		if existing.ExpiresAt.After(now) {
			if err := tx.Abort(ctx); err != nil && !errors.Is(err, gestalt.ErrTransactionDone) {
				return workflowRunRecord{}, false, err
			}
			committed = true
			return workflowRunRecord{}, false, nil
		}
		if err := claimStore.Delete(ctx, runID); err != nil {
			if indexedDBRetryableConflict(err) {
				return workflowRunRecord{}, false, nil
			}
			return workflowRunRecord{}, false, err
		}
	}
	claim := workflowRunClaimRecord{
		ID:        runID,
		RunID:     runID,
		OwnerID:   ownerID,
		ClaimedAt: now.UTC(),
		ExpiresAt: now.Add(runClaimTTL).UTC(),
	}
	if err := claimStore.Add(ctx, claim.toRecord()); err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	run.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	run.StartedAt = timePtr(now.UTC())
	run.CompletedAt = nil
	run.StatusMessage = ""
	if err := runStore.Put(ctx, run.toRecord()); err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if indexedDBRetryableConflict(err) {
			return workflowRunRecord{}, false, nil
		}
		return workflowRunRecord{}, false, err
	}
	committed = true
	return run, true, nil
}

func renewWorkflowRunClaim(ctx context.Context, db *gestalt.IndexedDBClient, runID, ownerID string, now time.Time, runClaimTTL time.Duration) (bool, error) {
	runID = strings.TrimSpace(runID)
	ownerID = strings.TrimSpace(ownerID)
	if runID == "" || ownerID == "" {
		return false, nil
	}
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	tx, err := db.Transaction(ctx, []string{storeRunClaims}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return true, nil
		}
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeRunClaims)
	claim, found, err := loadRunClaimRecordTx(ctx, store, runID)
	if err != nil {
		if indexedDBRetryableConflict(err) {
			return true, nil
		}
		return false, err
	}
	if !found || claim.OwnerID != ownerID || !claim.ExpiresAt.After(now) {
		if err := tx.Abort(ctx); err != nil && !errors.Is(err, gestalt.ErrTransactionDone) {
			return false, err
		}
		committed = true
		return false, nil
	}
	claim.ExpiresAt = now.Add(runClaimTTL).UTC()
	if err := store.Put(ctx, claim.toRecord()); err != nil {
		if indexedDBRetryableConflict(err) {
			return true, nil
		}
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if indexedDBRetryableConflict(err) {
			return true, nil
		}
		return false, err
	}
	committed = true
	return true, nil
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
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}
	cfg.RunClaimTTL = normalizeRunClaimTTL(cfg.RunClaimTTL)
	cfg.RunClaimRenewEvery = normalizeRunClaimRenewEvery(cfg.RunClaimTTL, cfg.RunClaimRenewEvery)
	return cfg, nil
}

func normalizeRunClaimTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultRunClaimTTL
	}
	return ttl
}

func normalizeRunClaimRenewEvery(ttl, renewEvery time.Duration) time.Duration {
	ttl = normalizeRunClaimTTL(ttl)
	if renewEvery <= 0 || renewEvery >= ttl {
		renewEvery = ttl / 3
	}
	if renewEvery <= 0 {
		return ttl
	}
	return renewEvery
}

func ensureWorkflowStores(ctx context.Context, db *gestalt.IndexedDBClient) error {
	for _, def := range []storeSchemaDef{
		{name: storeSchedules, schema: gestalt.ObjectStoreSchema{}},
		{name: storeEventTriggers, schema: gestalt.ObjectStoreSchema{}},
		{name: storeIdempotency, schema: gestalt.ObjectStoreSchema{}},
	} {
		if err := createWorkflowStore(ctx, db, def.name, def.schema); err != nil {
			return err
		}
	}
	if err := ensureWorkflowStoreExists(ctx, db, db.ObjectStore(storeWorkflowKeys), storeWorkflowKeys, gestalt.ObjectStoreSchema{}); err != nil {
		return err
	}

	runStore := db.ObjectStore(storeRuns)
	if err := ensureWorkflowStoreExists(ctx, db, runStore, storeRuns, gestalt.ObjectStoreSchema{}); err != nil {
		return err
	}
	if err := ensureWorkflowStoreExists(ctx, db, db.ObjectStore(storeRunClaims), storeRunClaims, workflowRunClaimSchema()); err != nil {
		return err
	}

	if err := ensureIndexedWorkflowStore(ctx, db, db.ObjectStore(storeExecutionRefs), storeExecutionRefs, workflowExecutionReferenceSchema(), validateWorkflowExecutionReferenceIndexes); err != nil {
		return err
	}
	if err := ensureIndexedWorkflowStore(ctx, db, db.ObjectStore(storeSignals), storeSignals, workflowSignalSchema(), validateWorkflowSignalIndexes); err != nil {
		return err
	}
	return nil
}

func createWorkflowStore(ctx context.Context, db *gestalt.IndexedDBClient, name string, schema gestalt.ObjectStoreSchema) error {
	err := db.CreateObjectStore(ctx, name, schema)
	if err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return err
	}
	return nil
}

func ensureWorkflowStoreExists(ctx context.Context, db *gestalt.IndexedDBClient, store *gestalt.ObjectStoreClient, name string, schema gestalt.ObjectStoreSchema) error {
	if _, err := store.Count(ctx, nil); err == nil {
		return nil
	} else if !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}
	return createWorkflowStore(ctx, db, name, schema)
}

func ensureIndexedWorkflowStore(ctx context.Context, db *gestalt.IndexedDBClient, store *gestalt.ObjectStoreClient, name string, schema gestalt.ObjectStoreSchema, validate func(context.Context, *gestalt.ObjectStoreClient) error) error {
	if _, err := store.Count(ctx, nil); errors.Is(err, gestalt.ErrNotFound) {
		if err := createWorkflowStore(ctx, db, name, schema); err != nil {
			return err
		}
		return validate(ctx, store)
	} else if err != nil {
		return err
	}
	return validate(ctx, store)
}

func validateWorkflowExecutionReferenceIndexes(ctx context.Context, store *gestalt.ObjectStoreClient) error {
	if _, err := store.Index("by_subject").Count(ctx, nil, "__workflow_schema_probe__"); err != nil {
		return fmt.Errorf("by_subject: %w", err)
	}
	return nil
}

func validateWorkflowSignalIndexes(ctx context.Context, store *gestalt.ObjectStoreClient) error {
	probes := []struct {
		name   string
		values []any
	}{
		{name: "by_run", values: []any{"__workflow_schema_probe__"}},
		{name: "by_run_state", values: []any{"__workflow_schema_probe__", signalStatePending}},
		{name: "by_run_sequence", values: []any{"__workflow_schema_probe__", int64(-1)}},
	}
	for _, probe := range probes {
		if _, err := store.Index(probe.name).Count(ctx, nil, probe.values...); err != nil {
			return fmt.Errorf("%s: %w", probe.name, err)
		}
	}
	return nil
}

type storeSchemaDef struct {
	name   string
	schema gestalt.ObjectStoreSchema
}

func workflowKeySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "run_id", Type: columnTypeString, NotNull: true},
			{Name: "created_at", Type: columnTypeTime},
		},
	}
}

func workflowRunClaimSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "run_id", Type: columnTypeString, NotNull: true},
			{Name: "owner_id", Type: columnTypeString, NotNull: true},
			{Name: "claimed_at", Type: columnTypeTime},
			{Name: "expires_at", Type: columnTypeTime},
		},
	}
}

func workflowSignalSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_run", KeyPath: []string{"run_id"}},
			{Name: "by_run_state", KeyPath: []string{"run_id", "state"}},
			{Name: "by_run_sequence", KeyPath: []string{"run_id", "sequence"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
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

func workflowExecutionReferenceSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_subject", KeyPath: []string{"subject_id"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: columnTypeString, PrimaryKey: true},
			{Name: "provider_name", Type: columnTypeString, NotNull: true},
			{Name: "target_json", Type: columnTypeString},
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

func recoverStaleWorkflowRuns(ctx context.Context, db *gestalt.IndexedDBClient, runStore, runClaimStore, workflowKeyStore, signalStore *gestalt.ObjectStoreClient, now time.Time) error {
	return recoverStaleWorkflowRunsWithTTL(ctx, db, runStore, runClaimStore, workflowKeyStore, signalStore, now, defaultRunClaimTTL)
}

func recoverStaleWorkflowRunsWithTTL(ctx context.Context, db *gestalt.IndexedDBClient, runStore, runClaimStore, workflowKeyStore, signalStore *gestalt.ObjectStoreClient, now time.Time, runClaimTTL time.Duration) error {
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	cursor, err := runStore.OpenKeyCursor(ctx, nil, gestalt.CursorNext)
	if err != nil {
		return err
	}
	defer cursor.Close()

	var errs []error
	for cursor.Continue() {
		key := strings.TrimSpace(cursor.PrimaryKey())
		if key == "" {
			continue
		}
		run, found, err := loadRunRecord(ctx, runStore, "", key)
		if err != nil {
			errs = append(errs, fmt.Errorf("load run %q: %w", key, err))
			continue
		}
		if !found {
			continue
		}
		stale, err := workflowRunRecoverablyStale(ctx, runClaimStore, run, now, runClaimTTL)
		if err != nil {
			errs = append(errs, fmt.Errorf("load run claim %q: %w", run.ID, err))
			continue
		}
		if stale {
			if err := failStaleRunningRun(ctx, runStore, workflowKeyStore, signalStore, run, run.WorkflowKey, now); err != nil {
				errs = append(errs, fmt.Errorf("fail stale run %q: %w", run.ID, err))
				continue
			}
			if err := runClaimStore.Delete(ctx, run.ID); err != nil {
				errs = append(errs, fmt.Errorf("delete stale run claim %q: %w", run.ID, err))
				continue
			}
			continue
		}
		claim, claimFound, err := loadRunClaimRecord(ctx, runClaimStore, run.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("load run claim %q: %w", run.ID, err))
			continue
		}
		if claimFound && run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING && inactiveRunClaimRecoverable(run, claim, now) {
			if _, err := deleteInactiveRunClaimIfRecoverable(ctx, db, run, claim, now); err != nil {
				errs = append(errs, fmt.Errorf("delete inactive run claim %q: %w", run.ID, err))
				continue
			}
		}
	}
	if err := cursor.Err(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func deleteInactiveRunClaimIfRecoverable(ctx context.Context, db *gestalt.IndexedDBClient, observedRun workflowRunRecord, observedClaim workflowRunClaimRecord, now time.Time) (bool, error) {
	tx, err := db.Transaction(ctx, []string{storeRuns, storeRunClaims}, gestalt.TransactionReadwrite, gestalt.TransactionOptions{DurabilityHint: gestalt.TransactionDurabilityStrict})
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	runStore := tx.ObjectStore(storeRuns)
	claimStore := tx.ObjectStore(storeRunClaims)
	run, found, err := loadRunRecordTx(ctx, runStore, observedRun.ownerKey(), observedRun.ID)
	if err != nil || !found {
		return false, err
	}
	claim, found, err := loadRunClaimRecordTx(ctx, claimStore, observedClaim.ID)
	if err != nil || !found {
		return false, err
	}
	if !sameRunClaim(claim, observedClaim) || !inactiveRunClaimRecoverable(run, claim, now) {
		if err := tx.Abort(ctx); err != nil && !errors.Is(err, gestalt.ErrTransactionDone) {
			return false, err
		}
		committed = true
		return false, nil
	}
	if err := claimStore.Delete(ctx, claim.ID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

func sameRunClaim(a, b workflowRunClaimRecord) bool {
	return a.ID == b.ID &&
		a.RunID == b.RunID &&
		a.OwnerID == b.OwnerID &&
		a.ClaimedAt.Equal(b.ClaimedAt) &&
		a.ExpiresAt.Equal(b.ExpiresAt)
}

func inactiveRunClaimRecoverable(run workflowRunRecord, claim workflowRunClaimRecord, now time.Time) bool {
	if run.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		return false
	}
	if workflowRunTerminal(run.Status) || !claim.ExpiresAt.After(now) {
		return true
	}
	if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING || strings.TrimSpace(run.WorkflowKey) != "" {
		return false
	}
	return !claim.ClaimedAt.Add(nonRunningRunClaimGrace).After(now)
}

func workflowRunRecoverablyStale(ctx context.Context, claimStore recordGetter, run workflowRunRecord, now time.Time, runClaimTTL time.Duration) (bool, error) {
	if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		return false, nil
	}
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	claim, found, err := loadRunClaimRecord(ctx, claimStore, run.ID)
	if err != nil {
		return false, err
	}
	if workflowRunExceededAgentDeadline(run, now) {
		return true, nil
	}
	if found {
		return !claim.ExpiresAt.After(now), nil
	}
	return workflowRunMissingClaimExpired(run, now, runClaimTTL), nil
}

func workflowRunRecoverablyStaleTx(ctx context.Context, claimStore *gestalt.TransactionObjectStore, run workflowRunRecord, now time.Time, runClaimTTL time.Duration) (bool, error) {
	if run.Status != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		return false, nil
	}
	runClaimTTL = normalizeRunClaimTTL(runClaimTTL)
	claim, found, err := loadRunClaimRecordTx(ctx, claimStore, run.ID)
	if err != nil {
		return false, err
	}
	if workflowRunExceededAgentDeadline(run, now) {
		return true, nil
	}
	if found {
		return !claim.ExpiresAt.After(now), nil
	}
	return workflowRunMissingClaimExpired(run, now, runClaimTTL), nil
}

func workflowRunExceededAgentDeadline(run workflowRunRecord, now time.Time) bool {
	if run.StartedAt == nil || run.Target == nil || run.Target.GetAgent() == nil {
		return false
	}
	timeout := defaultAgentRunTimeout
	if seconds := run.Target.GetAgent().GetTimeoutSeconds(); seconds > 0 {
		timeout = time.Duration(seconds) * time.Second
	}
	return !run.StartedAt.Add(timeout + agentRunStaleGrace).After(now)
}

func workflowRunMissingClaimExpired(run workflowRunRecord, now time.Time, runClaimTTL time.Duration) bool {
	if run.StartedAt == nil {
		return true
	}
	return !run.StartedAt.Add(normalizeRunClaimTTL(runClaimTTL)).After(now)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func failStaleRunningRun(ctx context.Context, runStore recordPutter, workflowKeyStore *gestalt.ObjectStoreClient, signalStore *gestalt.ObjectStoreClient, run workflowRunRecord, workflowKey string, now time.Time) error {
	run.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
	run.CompletedAt = &now
	run.StatusMessage = staleRunStatusMessage
	if err := runStore.Put(ctx, run.toRecord()); err != nil {
		return err
	}
	if key := firstNonEmpty(workflowKey, run.WorkflowKey); key != "" {
		if err := deleteWorkflowKeyRecordForRun(ctx, workflowKeyStore, key, run.ID); err != nil {
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
	return markSignalsFailed(ctx, signalStore, signals, now, run.StatusMessage)
}

func failStaleRunningRunTx(ctx context.Context, runStore recordPutter, workflowKeyStore *gestalt.TransactionObjectStore, signalStore *gestalt.TransactionObjectStore, run workflowRunRecord, workflowKey string, now time.Time) error {
	run.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
	run.CompletedAt = &now
	run.StatusMessage = staleRunStatusMessage
	if err := runStore.Put(ctx, run.toRecord()); err != nil {
		return err
	}
	if key := firstNonEmpty(workflowKey, run.WorkflowKey); key != "" {
		if err := deleteWorkflowKeyRecordForRunTx(ctx, workflowKeyStore, key, run.ID); err != nil {
			return err
		}
	}
	signals, err := listSignalRecordsTx(ctx, signalStore, run.ID, signalStatePending)
	if err != nil {
		return err
	}
	claimedSignals, err := listSignalRecordsTx(ctx, signalStore, run.ID, signalStateClaimed)
	if err != nil {
		return err
	}
	signals = append(signals, claimedSignals...)
	return markSignalsFailedTx(ctx, signalStore, signals, now, run.StatusMessage)
}

func normalizeTarget(target *proto.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	if agentTarget := target.GetAgent(); agentTarget != nil {
		agentProvider := strings.TrimSpace(agentTarget.GetProviderName())
		if agentProvider == "" {
			return scopedTarget{}, errors.New("target.agent.provider_name is required")
		}
		agent := cloneAgentTarget(agentTarget)
		if err := normalizeAgentTarget(agent, agentProvider); err != nil {
			return scopedTarget{}, err
		}
		normalized := &proto.BoundWorkflowTarget{
			Kind: &proto.BoundWorkflowTarget_Agent{Agent: agent},
		}
		return scopedTarget{
			OwnerKey: "agent:" + agentProvider,
			Target:   normalized,
		}, nil
	}
	pluginTarget := target.GetPlugin()
	if pluginTarget == nil {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	pluginName := strings.TrimSpace(pluginTarget.GetPluginName())
	operation := strings.TrimSpace(pluginTarget.GetOperation())
	if pluginName == "" {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	if operation == "" {
		return scopedTarget{}, errors.New("target.plugin.operation is required")
	}
	normalized := pluginTargetProto(
		pluginName,
		operation,
		strings.TrimSpace(pluginTarget.GetConnection()),
		strings.TrimSpace(pluginTarget.GetInstance()),
		cloneStructMap(pluginTarget.GetInput()),
	)
	return scopedTarget{
		OwnerKey: pluginName,
		Target:   normalized,
	}, nil
}

func targetOwnerKey(target *proto.BoundWorkflowTarget) string {
	if target == nil {
		return ""
	}
	if agentTarget := target.GetAgent(); agentTarget != nil {
		if providerName := strings.TrimSpace(agentTarget.GetProviderName()); providerName != "" {
			return "agent:" + providerName
		}
		return ""
	}
	if pluginTarget := target.GetPlugin(); pluginTarget != nil {
		return strings.TrimSpace(pluginTarget.GetPluginName())
	}
	return ""
}

func normalizeAgentTarget(target *proto.BoundWorkflowAgentTarget, providerName string) error {
	if target == nil {
		return errors.New("target.agent is required")
	}
	target.ProviderName = strings.TrimSpace(providerName)
	target.Model = strings.TrimSpace(target.GetModel())
	target.Prompt = strings.TrimSpace(target.GetPrompt())
	if target.GetPrompt() == "" && len(target.GetMessages()) == 0 {
		return errors.New("target.agent.prompt or messages is required")
	}
	if target.GetTimeoutSeconds() < 0 {
		return errors.New("target.agent.timeout_seconds must not be negative")
	}
	if err := normalizeAgentOutputDelivery(target.GetOutputDelivery()); err != nil {
		return err
	}
	return nil
}

func normalizeAgentOutputDelivery(delivery *proto.WorkflowOutputDelivery) error {
	if delivery == nil {
		return nil
	}
	deliveryTarget := delivery.GetTarget()
	if deliveryTarget == nil {
		return errors.New("target.agent.output_delivery.target.plugin_name is required")
	}
	pluginName := strings.TrimSpace(deliveryTarget.GetPluginName())
	operation := strings.TrimSpace(deliveryTarget.GetOperation())
	if pluginName == "" {
		return errors.New("target.agent.output_delivery.target.plugin_name is required")
	}
	if operation == "" {
		return errors.New("target.agent.output_delivery.target.operation is required")
	}
	credentialMode := strings.ToLower(strings.TrimSpace(delivery.GetCredentialMode()))
	switch credentialMode {
	case "", "none", "user":
	default:
		return fmt.Errorf("target.agent.output_delivery.credential_mode %q is not supported", delivery.GetCredentialMode())
	}
	delivery.Target = &proto.BoundWorkflowPluginTarget{
		PluginName: pluginName,
		Operation:  operation,
		Connection: strings.TrimSpace(deliveryTarget.GetConnection()),
		Instance:   strings.TrimSpace(deliveryTarget.GetInstance()),
		Input:      structFromAny(cloneStructMap(deliveryTarget.GetInput())),
	}
	delivery.CredentialMode = credentialMode
	for _, binding := range delivery.GetInputBindings() {
		if binding == nil {
			return errors.New("target.agent.output_delivery.input_bindings.value is required")
		}
		binding.InputField = strings.TrimSpace(binding.GetInputField())
		if binding.GetInputField() == "" {
			return errors.New("target.agent.output_delivery.input_bindings.input_field is required")
		}
		value := binding.GetValue()
		if value == nil || value.GetKind() == nil {
			return errors.New("target.agent.output_delivery.input_bindings.value is required")
		}
		switch kind := value.GetKind().(type) {
		case *proto.WorkflowOutputValueSource_AgentOutput:
			kind.AgentOutput = strings.TrimSpace(kind.AgentOutput)
			if kind.AgentOutput == "" {
				return errors.New("target.agent.output_delivery.input_bindings.value.agent_output is required")
			}
		case *proto.WorkflowOutputValueSource_SignalPayload:
			kind.SignalPayload = strings.TrimSpace(kind.SignalPayload)
			if kind.SignalPayload == "" {
				return errors.New("target.agent.output_delivery.input_bindings.value.signal_payload is required")
			}
		case *proto.WorkflowOutputValueSource_SignalMetadata:
			kind.SignalMetadata = strings.TrimSpace(kind.SignalMetadata)
			if kind.SignalMetadata == "" {
				return errors.New("target.agent.output_delivery.input_bindings.value.signal_metadata is required")
			}
		case *proto.WorkflowOutputValueSource_Literal:
			if kind.Literal == nil {
				return errors.New("target.agent.output_delivery.input_bindings.value.literal is required")
			}
		default:
			return errors.New("target.agent.output_delivery.input_bindings.value is required")
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

func nextClaimedRunnableRun(ctx context.Context, state *configuredState, preferredRunID string, now time.Time) (workflowRunRecord, bool, error) {
	preferredRunID = strings.TrimSpace(preferredRunID)
	triedPreferred := false
	runs, err := listRunRecords(ctx, state.runStore, "")
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	runs = sortRunRecordsForDispatch(runs)
	for _, run := range runs {
		if preferredRunID != "" && run.ID == preferredRunID {
			triedPreferred = true
		}
		claimedRun, claimed, err := claimRunnableRun(ctx, state, run, now)
		if err != nil {
			return workflowRunRecord{}, false, err
		}
		if claimed {
			return claimedRun, true, nil
		}
	}
	if preferredRunID != "" && !triedPreferred {
		run, found, err := loadRunRecord(ctx, state.runStore, "", preferredRunID)
		if err != nil {
			return workflowRunRecord{}, false, err
		}
		if found {
			claimedRun, claimed, err := claimRunnableRun(ctx, state, run, now)
			if err != nil {
				return workflowRunRecord{}, false, err
			}
			if claimed {
				return claimedRun, true, nil
			}
		}
	}
	return workflowRunRecord{}, false, nil
}

func sortRunRecordsForDispatch(runs []workflowRunRecord) []workflowRunRecord {
	if len(runs) == 0 {
		return nil
	}
	sorted := append([]workflowRunRecord(nil), runs...)
	slices.SortFunc(sorted, func(a, b workflowRunRecord) int {
		if pa, pb := workflowRunDispatchPriority(a), workflowRunDispatchPriority(b); pa != pb {
			return pa - pb
		}
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return sorted
}

func workflowRunDispatchPriority(run workflowRunRecord) int {
	if run.TriggerKind == triggerKindEvent && run.Target != nil && run.Target.GetPlugin() != nil {
		return 0
	}
	if workflowRunHasInteractiveAgentOutput(run) {
		return 5
	}
	if strings.TrimSpace(run.WorkflowKey) != "" {
		return 10
	}
	if run.Target != nil && run.Target.GetPlugin() != nil {
		return 20
	}
	if run.TriggerKind == triggerKindEvent {
		return 30
	}
	if run.Target != nil && run.Target.GetAgent() != nil {
		return 40
	}
	return 50
}

func workflowRunHasInteractiveAgentOutput(run workflowRunRecord) bool {
	if run.Target == nil {
		return false
	}
	agent := run.Target.GetAgent()
	if agent == nil {
		return false
	}
	if metadata := agent.GetMetadata(); metadata != nil {
		if _, ok := metadata.GetFields()["slack"]; ok {
			return true
		}
	}
	delivery := agent.GetOutputDelivery()
	if delivery == nil || delivery.GetTarget() == nil {
		return false
	}
	target := delivery.GetTarget()
	return target.GetPluginName() == "slack" && strings.HasPrefix(target.GetOperation(), "events.")
}

func claimRunnableRun(ctx context.Context, state *configuredState, run workflowRunRecord, now time.Time) (workflowRunRecord, bool, error) {
	runnable, err := workflowRunRunnable(ctx, state.signalStore, run)
	if err != nil || !runnable {
		return workflowRunRecord{}, false, err
	}
	if run.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING && strings.TrimSpace(run.WorkflowKey) == "" {
		return claimAndStartPendingRun(ctx, state.db, run, state.claimOwnerID, now, state.runClaimTTL)
	}
	claimed, err := claimWorkflowRun(ctx, state.db, run.ID, state.claimOwnerID, now, state.runClaimTTL)
	if err != nil || !claimed {
		return workflowRunRecord{}, false, err
	}
	return run, true, nil
}

func nextRunnableRun(ctx context.Context, runStore, signalStore *gestalt.ObjectStoreClient, preferredRunID string) (workflowRunRecord, bool, error) {
	if preferredRunID = strings.TrimSpace(preferredRunID); preferredRunID != "" {
		run, found, err := loadRunRecord(ctx, runStore, "", preferredRunID)
		if err != nil {
			return workflowRunRecord{}, false, err
		}
		if found {
			runnable, err := workflowRunRunnable(ctx, signalStore, run)
			if err != nil {
				return workflowRunRecord{}, false, err
			}
			if runnable {
				return run, true, nil
			}
		}
	}
	runs, err := listRunRecords(ctx, runStore, "")
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	runs = sortRunRecordsForDispatch(runs)
	for _, run := range runs {
		runnable, err := workflowRunRunnable(ctx, signalStore, run)
		if err != nil {
			return workflowRunRecord{}, false, err
		}
		if runnable {
			return run, true, nil
		}
	}
	return workflowRunRecord{}, false, nil
}

func workflowRunRunnable(ctx context.Context, signalStore *gestalt.ObjectStoreClient, run workflowRunRecord) (bool, error) {
	if run.Status == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		if strings.TrimSpace(run.WorkflowKey) == "" {
			return true, nil
		}
		hasPending, err := hasPendingSignals(ctx, signalStore, run.ID)
		if err != nil {
			return false, err
		}
		return hasPending, nil
	}
	if strings.TrimSpace(run.WorkflowKey) == "" || !workflowRunTerminal(run.Status) {
		return false, nil
	}
	hasPending, err := hasPendingSignals(ctx, signalStore, run.ID)
	if err != nil {
		return false, err
	}
	return hasPending, nil
}

func hasPendingSignals(ctx context.Context, store *gestalt.ObjectStoreClient, runID string) (bool, error) {
	return hasSignalsInState(ctx, store, runID, signalStatePending)
}

func hasPendingSignalsTx(ctx context.Context, store *gestalt.TransactionObjectStore, runID string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	count, err := store.Index("by_run_state").Count(ctx, nil, runID, signalStatePending)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func hasSignalsInState(ctx context.Context, store *gestalt.ObjectStoreClient, runID, state string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	count, err := store.Index("by_run_state").Count(ctx, nil, runID, strings.TrimSpace(state))
	if err != nil {
		return false, err
	}
	return count > 0, nil
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

func markSignalsDeliveredTx(ctx context.Context, store *gestalt.TransactionObjectStore, records []workflowSignalRecord, deliveredAt time.Time) error {
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

func markRunSignalsFailedTx(ctx context.Context, store *gestalt.TransactionObjectStore, runID string, claimed []workflowSignalRecord, failedAt time.Time, message string) error {
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
		records, err := listSignalRecordsTx(ctx, store, runID, state)
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
	return markSignalsFailedTx(ctx, store, records, failedAt, message)
}

func markSignalsFailedTx(ctx context.Context, store *gestalt.TransactionObjectStore, records []workflowSignalRecord, failedAt time.Time, message string) error {
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
	return activeWorkflowKeyRunFromStores(ctx, state.workflowKeyStore, state.runStore, workflowKey)
}

func activeWorkflowKeyRunFromStores(ctx context.Context, workflowKeyStore recordGetter, runStore recordGetter, workflowKey string) (workflowRunRecord, bool, error) {
	key, found, err := loadWorkflowKeyRecord(ctx, workflowKeyStore, workflowKey)
	if err != nil || !found {
		return workflowRunRecord{}, false, err
	}
	run, runFound, err := loadRunRecord(ctx, runStore, "", key.RunID)
	if err != nil || !runFound {
		return workflowRunRecord{}, false, err
	}
	return run, true, nil
}

func activeWorkflowKeyRunInTransaction(ctx context.Context, workflowKeyStore, runStore *gestalt.TransactionObjectStore, workflowKey string) (workflowRunRecord, bool, error) {
	key, found, err := loadWorkflowKeyRecordTx(ctx, workflowKeyStore, workflowKey)
	if err != nil || !found {
		return workflowRunRecord{}, false, err
	}
	run, runFound, err := loadRunRecordTx(ctx, runStore, "", key.RunID)
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	if !runFound {
		if err := deleteWorkflowKeyRecord(ctx, workflowKeyStore, workflowKey); err != nil {
			return workflowRunRecord{}, false, err
		}
		return workflowRunRecord{}, false, nil
	}
	return run, true, nil
}

func loadWorkflowKeyRecord(ctx context.Context, store recordGetter, workflowKey string) (workflowKeyRecord, bool, error) {
	record, err := store.Get(ctx, workflowKeyID(workflowKey))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowKeyRecord{}, false, nil
		}
		return workflowKeyRecord{}, false, err
	}
	return workflowKeyRecordFromRecord(record), true, nil
}

func loadWorkflowKeyRecordTx(ctx context.Context, store *gestalt.TransactionObjectStore, workflowKey string) (workflowKeyRecord, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, workflowKeyID(workflowKey))
	if err != nil || !found {
		return workflowKeyRecord{}, false, err
	}
	return workflowKeyRecordFromRecord(record), true, nil
}

func loadRunClaimRecord(ctx context.Context, store recordGetter, runID string) (workflowRunClaimRecord, bool, error) {
	record, err := store.Get(ctx, strings.TrimSpace(runID))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowRunClaimRecord{}, false, nil
		}
		return workflowRunClaimRecord{}, false, err
	}
	return workflowRunClaimRecordFromRecord(record), true, nil
}

func loadRunClaimRecordTx(ctx context.Context, store *gestalt.TransactionObjectStore, runID string) (workflowRunClaimRecord, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, strings.TrimSpace(runID))
	if err != nil || !found {
		return workflowRunClaimRecord{}, false, err
	}
	return workflowRunClaimRecordFromRecord(record), true, nil
}

func storeWorkflowKeyRecord(ctx context.Context, store recordPutter, workflowKey, runID string, createdAt time.Time) error {
	record := workflowKeyRecord{
		ID:        workflowKeyID(workflowKey),
		RunID:     strings.TrimSpace(runID),
		CreatedAt: createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func addWorkflowKeyRecord(ctx context.Context, store recordAdder, workflowKey, runID string, createdAt time.Time) error {
	record := workflowKeyRecord{
		ID:        workflowKeyID(workflowKey),
		RunID:     strings.TrimSpace(runID),
		CreatedAt: createdAt,
	}
	return store.Add(ctx, record.toRecord())
}

func deleteWorkflowKeyRecord(ctx context.Context, store recordDeleter, workflowKey string) error {
	if err := store.Delete(ctx, workflowKeyID(workflowKey)); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
		return err
	}
	return nil
}

func deleteWorkflowKeyRecordForRun(ctx context.Context, store *gestalt.ObjectStoreClient, workflowKey, runID string) error {
	key, found, err := loadWorkflowKeyRecord(ctx, store, workflowKey)
	if err != nil || !found {
		return err
	}
	if key.RunID != strings.TrimSpace(runID) {
		return nil
	}
	return deleteWorkflowKeyRecord(ctx, store, workflowKey)
}

func deleteWorkflowKeyRecordForRunTx(ctx context.Context, store *gestalt.TransactionObjectStore, workflowKey, runID string) error {
	key, found, err := loadWorkflowKeyRecordTx(ctx, store, workflowKey)
	if err != nil || !found {
		return err
	}
	if key.RunID != strings.TrimSpace(runID) {
		return nil
	}
	return deleteWorkflowKeyRecord(ctx, store, workflowKey)
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
		strings.TrimSpace(run.ownerKey()),
		strings.TrimSpace(run.ID),
		key,
	}, "\x00")))
	return "workflow_signal:" + hex.EncodeToString(sum[:])
}

func loadSignalRecord(ctx context.Context, store recordGetter, signalID string) (workflowSignalRecord, bool, error) {
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

func loadSignalRecordTx(ctx context.Context, store *gestalt.TransactionObjectStore, signalID string) (workflowSignalRecord, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, strings.TrimSpace(signalID))
	if err != nil || !found {
		return workflowSignalRecord{}, false, err
	}
	signal, err := signalRecordFromRecord(record)
	if err != nil {
		return workflowSignalRecord{}, false, err
	}
	return signal, true, nil
}

func listSignalRecords(ctx context.Context, store *gestalt.ObjectStoreClient, runID, state string) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case runID != "" && state != "":
		records, err = store.Index("by_run_state").GetAll(ctx, nil, runID, state)
	case runID != "":
		records, err = store.Index("by_run").GetAll(ctx, nil, runID)
	default:
		records, err = store.GetAll(ctx, nil)
	}
	if err != nil {
		return nil, err
	}
	return signalRecordsFromRecords(records, runID, state)
}

func signalRecordsFromRecords(records []gestalt.Record, runID, state string) ([]workflowSignalRecord, error) {
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

func listSignalRecordsTx(ctx context.Context, store *gestalt.TransactionObjectStore, runID, state string) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case runID != "" && state != "":
		records, err = store.Index("by_run_state").GetAll(ctx, nil, runID, state)
	case runID != "":
		records, err = store.Index("by_run").GetAll(ctx, nil, runID)
	default:
		records, err = store.GetAll(ctx, nil)
	}
	if err != nil {
		return nil, err
	}
	return signalRecordsFromRecords(records, runID, state)
}

func listSignalRecordsLimit(ctx context.Context, store *gestalt.ObjectStoreClient, runID, state string, limit int) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	if runID == "" || limit <= 0 {
		return listSignalRecords(ctx, store, runID, state)
	}

	cursor, err := store.Index("by_run_sequence").OpenKeyCursor(ctx, nil, gestalt.CursorNext, runID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close() }()

	out := make([]workflowSignalRecord, 0, limit)
	for cursor.Continue() {
		primaryKey := cursor.PrimaryKey()
		if primaryKey == "" {
			continue
		}
		record, err := store.Get(ctx, primaryKey)
		if err != nil {
			if errors.Is(err, gestalt.ErrNotFound) {
				continue
			}
			return nil, err
		}
		signal, err := signalRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if signal.RunID != runID {
			continue
		}
		if state != "" && signal.State != state {
			continue
		}
		out = append(out, signal)
		if len(out) >= limit {
			break
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func indexedDBAlreadyExists(err error) bool {
	if errors.Is(err, gestalt.ErrAlreadyExists) {
		return true
	}
	return status.Code(err) == codes.AlreadyExists
}

func indexedDBRetryableConflict(err error) bool {
	if indexedDBAlreadyExists(err) || status.Code(err) == codes.Aborted {
		return true
	}
	return status.Code(err) == codes.Internal && strings.Contains(err.Error(), "database is locked")
}

func yieldIndexedDBRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		runtime.Gosched()
		return nil
	}
}

func loadScheduleRecord(ctx context.Context, store *gestalt.ObjectStoreClient, ownerKey, scheduleID string) (workflowScheduleRecord, bool, error) {
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
	if ownerKey != "" && schedule.ownerKey() != ownerKey {
		return workflowScheduleRecord{}, false, nil
	}
	return schedule, true, nil
}

func listScheduleRecords(ctx context.Context, store *gestalt.ObjectStoreClient, ownerKey string) ([]workflowScheduleRecord, error) {
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
		if ownerKey != "" && schedule.ownerKey() != ownerKey {
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

func loadEventTriggerRecord(ctx context.Context, store *gestalt.ObjectStoreClient, ownerKey, triggerID string) (workflowEventTriggerRecord, bool, error) {
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
	if ownerKey != "" && trigger.ownerKey() != ownerKey {
		return workflowEventTriggerRecord{}, false, nil
	}
	return trigger, true, nil
}

func listEventTriggerRecords(ctx context.Context, store *gestalt.ObjectStoreClient, ownerKey string) ([]workflowEventTriggerRecord, error) {
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
		if ownerKey != "" && trigger.ownerKey() != ownerKey {
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

func loadRunRecord(ctx context.Context, store recordGetter, ownerKey, runID string) (workflowRunRecord, bool, error) {
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
	if ownerKey != "" && run.ownerKey() != ownerKey {
		return workflowRunRecord{}, false, nil
	}
	return run, true, nil
}

func loadRunRecordTx(ctx context.Context, store *gestalt.TransactionObjectStore, ownerKey, runID string) (workflowRunRecord, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, strings.TrimSpace(runID))
	if err != nil || !found {
		return workflowRunRecord{}, false, err
	}
	run, err := runRecordFromRecord(record)
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	if ownerKey != "" && run.ownerKey() != ownerKey {
		return workflowRunRecord{}, false, nil
	}
	return run, true, nil
}

func listRunRecords(ctx context.Context, store *gestalt.ObjectStoreClient, ownerKey string) ([]workflowRunRecord, error) {
	cursor, err := store.OpenKeyCursor(ctx, nil, gestalt.CursorNext)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var out []workflowRunRecord
	for cursor.Continue() {
		key := strings.TrimSpace(cursor.PrimaryKey())
		if key == "" {
			continue
		}
		run, found, err := loadRunRecord(ctx, store, "", key)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if ownerKey != "" && run.ownerKey() != ownerKey {
			continue
		}
		out = append(out, run)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b workflowRunRecord) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func loadIdempotencyRecord(ctx context.Context, store recordGetter, ownerKey, key string) (workflowIdempotencyRecord, bool, error) {
	record, err := store.Get(ctx, idempotencyID(ownerKey, key))
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
	return value, true, nil
}

func loadIdempotencyRecordTx(ctx context.Context, store *gestalt.TransactionObjectStore, ownerKey, key string) (workflowIdempotencyRecord, bool, error) {
	record, found, err := transactionGetRecord(ctx, store, idempotencyID(ownerKey, key))
	if err != nil || !found {
		return workflowIdempotencyRecord{}, false, err
	}
	value, err := idempotencyRecordFromRecord(record)
	if err != nil {
		return workflowIdempotencyRecord{}, false, err
	}
	return value, true, nil
}

func storeIdempotencyRecord(ctx context.Context, store recordPutter, ownerKey, key, runID string, createdAt time.Time) error {
	record := workflowIdempotencyRecord{
		ID:             idempotencyID(ownerKey, key),
		IdempotencyKey: key,
		RunID:          runID,
		CreatedAt:      createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func storeSignalIdempotencyRecord(ctx context.Context, store recordPutter, ownerKey, key, runID, signalID, workflowKey string, startedRun bool, createdAt time.Time) error {
	record := workflowIdempotencyRecord{
		ID:             idempotencyID(ownerKey, key),
		IdempotencyKey: key,
		RunID:          runID,
		SignalID:       signalID,
		WorkflowKey:    workflowKey,
		StartedRun:     startedRun,
		CreatedAt:      createdAt,
	}
	return store.Put(ctx, record.toRecord())
}

func loadExecutionReferenceRecord(ctx context.Context, store recordGetter, refID string) (workflowExecutionReferenceRecord, bool, error) {
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

func idempotencyID(ownerKey, key string) string {
	return ownerKey + ":" + key
}

func idempotentManualRunID(ownerKey, key string) string {
	sum := sha256.Sum256([]byte(ownerKey + "\x00" + key))
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
	permissions, err := eventExecutionReferencePermissions(trigger)
	if err != nil {
		return nil, err
	}
	subjectID := strings.TrimSpace(actor.GetSubjectId())
	return &proto.WorkflowExecutionReference{
		Id:                  eventExecutionRefID(runID),
		ProviderName:        strings.TrimSpace(providerName),
		Target:              cloneTarget(target),
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
	extra, err := configuredEventRunPermissions(pluginTargetInput(trigger.Target))
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
		if delivery := agent.GetOutputDelivery(); delivery != nil {
			deliveryTarget := delivery.GetTarget()
			pluginName := strings.TrimSpace(deliveryTarget.GetPluginName())
			operation := strings.TrimSpace(deliveryTarget.GetOperation())
			if pluginName != "" && operation != "" {
				ops := permissionsByPlugin[pluginName]
				if ops == nil {
					ops = map[string]struct{}{}
					permissionsByPlugin[pluginName] = ops
				}
				ops[operation] = struct{}{}
			}
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

func pluginTargetInput(target *proto.BoundWorkflowTarget) map[string]any {
	if target == nil || target.GetPlugin() == nil {
		return nil
	}
	return cloneStructMap(target.GetPlugin().GetInput())
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
	if err := (protojson.UnmarshalOptions{}).Unmarshal([]byte(value), target); err != nil {
		return nil, fmt.Errorf("%s %q invalid target_json: %w", recordKind, id, err)
	}
	target = cloneTarget(target)
	if target.GetAgent() == nil && target.GetPlugin() == nil {
		return nil, fmt.Errorf("%s %q target_json must contain plugin or agent target", recordKind, id)
	}
	if targetOwnerKey(target) == "" {
		return nil, fmt.Errorf("%s %q target_json must contain plugin.plugin_name or agent.provider_name", recordKind, id)
	}
	return target, nil
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
		"cron":          r.Cron,
		"timezone":      r.Timezone,
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
		Cron:         stringField(value, "cron"),
		Timezone:     stringField(value, "timezone"),
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
		Target:       cloneTarget(r.Target),
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
		"match_type":    r.MatchType,
		"match_source":  r.MatchSource,
		"match_subject": r.MatchSubject,
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
		MatchType:    stringField(value, "match_type"),
		MatchSource:  stringField(value, "match_source"),
		MatchSubject: stringField(value, "match_subject"),
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
		Target:       cloneTarget(r.Target),
		Paused:       r.Paused,
		CreatedAt:    timestamppb.New(r.CreatedAt),
		UpdatedAt:    timestamppb.New(r.UpdatedAt),
		CreatedBy:    cloneActor(r.CreatedBy),
		ExecutionRef: r.ExecutionRef,
	}, nil
}

func (r workflowRunRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":                       r.ID,
		"status":                   int64(r.Status),
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
		"next_signal_sequence":     r.NextSignalSequence,
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
		Status:                proto.WorkflowRunStatus(intField(value, "status")),
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
		NextSignalSequence:    intField(value, "next_signal_sequence"),
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
		Target:        cloneTarget(r.Target),
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

func (r workflowRunClaimRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":         r.ID,
		"run_id":     r.RunID,
		"owner_id":   r.OwnerID,
		"claimed_at": r.ClaimedAt.UTC(),
		"expires_at": r.ExpiresAt.UTC(),
	}
}

func workflowRunClaimRecordFromRecord(record gestalt.Record) workflowRunClaimRecord {
	value := map[string]any(record)
	out := workflowRunClaimRecord{
		ID:      stringField(value, "id"),
		RunID:   stringField(value, "run_id"),
		OwnerID: stringField(value, "owner_id"),
	}
	if out.RunID == "" {
		out.RunID = out.ID
	}
	if claimedAt := timeField(value, "claimed_at"); claimedAt != nil {
		out.ClaimedAt = claimedAt.UTC()
	}
	if expiresAt := timeField(value, "expires_at"); expiresAt != nil {
		out.ExpiresAt = expiresAt.UTC()
	}
	return out
}

func (r workflowIdempotencyRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":              r.ID,
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
		"id":         r.ID,
		"run_id":     r.RunID,
		"created_at": r.CreatedAt.UTC(),
	}
}

func workflowKeyRecordFromRecord(record gestalt.Record) workflowKeyRecord {
	value := map[string]any(record)
	out := workflowKeyRecord{
		ID:    stringField(value, "id"),
		RunID: stringField(value, "run_id"),
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
	target, err := normalizeTarget(ref.GetTarget())
	if err != nil {
		return workflowExecutionReferenceRecord{}, err
	}
	record := workflowExecutionReferenceRecord{
		ID:                  strings.TrimSpace(ref.GetId()),
		ProviderName:        strings.TrimSpace(ref.GetProviderName()),
		Target:              cloneTarget(target.Target),
		SubjectID:           strings.TrimSpace(ref.GetSubjectId()),
		SubjectKind:         strings.TrimSpace(ref.GetSubjectKind()),
		DisplayName:         strings.TrimSpace(ref.GetDisplayName()),
		AuthSource:          strings.TrimSpace(ref.GetAuthSource()),
		CredentialSubjectID: strings.TrimSpace(ref.GetCredentialSubjectId()),
		CallerPluginName:    strings.TrimSpace(ref.GetCallerPluginName()),
	}
	if record.ID == "" {
		return workflowExecutionReferenceRecord{}, errors.New("id is required")
	}
	if record.ProviderName == "" {
		return workflowExecutionReferenceRecord{}, errors.New("provider_name is required")
	}
	if target.OwnerKey == "" {
		return workflowExecutionReferenceRecord{}, errors.New("target owner is required")
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
		Target:              target,
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
		"target_json":           targetJSON(r.Target),
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
		Target:              cloneTarget(r.Target),
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

func executionReferenceRecordsEqual(left, right workflowExecutionReferenceRecord) bool {
	if left.ID != right.ID ||
		left.ProviderName != right.ProviderName ||
		targetJSON(left.Target) != targetJSON(right.Target) ||
		left.SubjectID != right.SubjectID ||
		left.SubjectKind != right.SubjectKind ||
		left.DisplayName != right.DisplayName ||
		left.AuthSource != right.AuthSource ||
		left.CredentialSubjectID != right.CredentialSubjectID ||
		left.PermissionsJSON != right.PermissionsJSON ||
		left.CallerPluginName != right.CallerPluginName ||
		!left.CreatedAt.Equal(right.CreatedAt) {
		return false
	}
	if (left.RevokedAt == nil) != (right.RevokedAt == nil) {
		return false
	}
	return left.RevokedAt == nil || left.RevokedAt.Equal(*right.RevokedAt)
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
		Kind: &proto.BoundWorkflowTarget_Plugin{
			Plugin: &proto.BoundWorkflowPluginTarget{
				PluginName: pluginName,
				Operation:  operation,
				Connection: connection,
				Instance:   instance,
				Input:      structFromAny(input),
			},
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
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Starter = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
