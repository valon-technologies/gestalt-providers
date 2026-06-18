package indexeddb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion           = "0.0.1-alpha.1"
	defaultPollInterval       = time.Second
	defaultWorkerCount        = 4
	defaultMaxSignalsPerBatch = 25
	defaultRunClaimTTL        = 10 * time.Minute
	defaultRunClaimRenewEvery = defaultRunClaimTTL / 3
	defaultStaleRecoveryEvery = time.Minute
	maxPollFailureBackoff     = 30 * time.Second
	nonRunningRunClaimGrace   = time.Minute
	defaultAgentRunTimeout    = 5 * time.Minute
	agentRunStaleGrace        = time.Minute
	maxSignalAddRetries       = 4096

	storeSchedules    = "schedules"
	storeDefinitions  = "definitions"
	storeRuns         = "runs"
	storeRunClaims    = "workflow_run_claims"
	storeIdempotency  = "idempotency"
	storeWorkflowKeys = "workflow_keys"
	storeSignals      = "workflow_signals"

	triggerKindManual   = "manual"
	triggerKindSchedule = "schedule"
	triggerKindEvent    = "event"

	gestaltInputKey              = "_gestalt"
	configManagedWorkflowSubject = "system:config"
	workflowMetadataKey          = "workflow"
	dispatchPriorityMetadataKey  = "dispatchPriority"
	staleRunStatusMessage        = "workflow provider restarted while run was in progress"

	signalStatePending   = "pending"
	signalStateClaimed   = "claimed"
	signalStateDelivered = "delivered"
	signalStateFailed    = "failed"

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
	mu sync.RWMutex
	// workerMu serializes scheduler and worker claim work without blocking
	// foreground enqueue calls on the provider lifecycle lock.
	workerMu sync.Mutex
	// deliverMu serializes event delivery so deterministic event run IDs stay
	// consistent across duplicate deliveries.
	deliverMu sync.Mutex

	name              string
	cfg               config
	db                indexeddb.Database
	stepExecutor      gestaltworkflow.StepExecutor
	scheduleStore     indexeddb.ObjectStore
	definitionStore   indexeddb.ObjectStore
	runStore          indexeddb.ObjectStore
	runClaimStore     indexeddb.ObjectStore
	idempotencyStore  indexeddb.ObjectStore
	workflowKeyStore  indexeddb.ObjectStore
	signalStore       indexeddb.ObjectStore
	indexedDB         indexeddb.Database
	workflowExecutor  gestaltworkflow.StepExecutor
	claimOwnerID      string
	startedAt         time.Time
	pollCancel        context.CancelFunc
	pollDone          chan struct{}
	wake              chan string
	lastStaleRecovery time.Time

	now func() time.Time
}

type workflowScheduleRecord struct {
	ID                   string
	ActivationID         string
	Cron                 string
	Timezone             string
	Target               *gestalt.BoundWorkflowTarget
	Paused               bool
	CreatedAt            time.Time
	UpdatedAt            time.Time
	NextRunAt            *time.Time
	CreatedBySubjectID   string
	DefinitionID         string
	DefinitionGeneration int64
	RunAs                *gestalt.Subject
}

type workflowRunRecord struct {
	ID                    string
	ProviderName          string
	Status                gestalt.WorkflowRunStatus
	Target                *gestalt.BoundWorkflowTarget
	TriggerKind           string
	TriggerScheduleID     string
	TriggerScheduledFor   *time.Time
	TriggerEventTriggerID string
	TriggerEvent          *gestalt.WorkflowEvent
	CreatedAt             time.Time
	StartedAt             *time.Time
	CompletedAt           *time.Time
	StatusMessage         string
	Output                any
	CreatedBySubjectID    string
	DefinitionID          string
	DefinitionGeneration  int64
	Input                 map[string]any
	CurrentStepID         string
	Steps                 []gestalt.WorkflowStepExecution
	RunAs                 *gestalt.Subject
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

func (r workflowRunRecord) ownerKey() string {
	return targetOwnerKey(r.Target)
}

type workflowSignalRecord struct {
	ID             string
	RunID          string
	WorkflowKey    string
	State          string
	Signal         *gestalt.WorkflowSignal
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

type scopedTarget struct {
	OwnerKey string
	Target   *gestalt.BoundWorkflowTarget
}

var _ gestalt.WorkflowProvider = (*Provider)(nil)

func New() *Provider { return newProviderCore() }

func newProviderCore() *Provider {
	return newProviderCoreWithDB(nil)
}

func newProviderCoreWithDB(db indexeddb.Database) *Provider {
	return &Provider{now: time.Now, claimOwnerID: uuid.NewString(), indexedDB: db}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	if err := p.Close(); err != nil {
		return err
	}

	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("indexeddb workflow: %w", err)
	}

	db := p.indexedDB
	if db == nil {
		db, err = gestalt.IndexedDB(ctx)
		if err != nil {
			return fmt.Errorf("indexeddb workflow: connect indexeddb: %w", err)
		}
	}

	executor := p.workflowExecutor
	if executor == nil {
		executor = gestaltworkflow.New(gestaltworkflow.Config{})
	}

	cleanup := func() {
		_ = executor.Close()
		_ = db.Close()
	}

	if err := ensureWorkflowObjectStores(ctx, db); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: ensure stores: %w", err)
	}

	runStore := db.ObjectStore(storeRuns)
	runClaimStore := db.ObjectStore(storeRunClaims)
	workflowKeyStore := db.ObjectStore(storeWorkflowKeys)
	signalStore := db.ObjectStore(storeSignals)
	if err := validateWorkflowSignalIndexes(ctx, signalStore); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: validate signal indexes: %w", err)
	}

	p.mu.Lock()
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.db = db
	p.stepExecutor = executor
	p.scheduleStore = db.ObjectStore(storeSchedules)
	p.definitionStore = db.ObjectStore(storeDefinitions)
	p.runStore = runStore
	p.runClaimStore = runClaimStore
	p.idempotencyStore = db.ObjectStore(storeIdempotency)
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
	executor := p.stepExecutor
	db := p.db

	p.name = ""
	p.cfg = config{}
	p.db = nil
	p.stepExecutor = nil
	p.scheduleStore = nil
	p.definitionStore = nil
	p.runStore = nil
	p.runClaimStore = nil
	p.idempotencyStore = nil
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
	if executor != nil {
		if err := executor.Close(); err != nil {
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

func (p *Provider) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	actor := cloneCreatedBySubjectID(req.CreatedBySubjectID)
	key := strings.TrimSpace(req.IdempotencyKey)
	workflowKey := strings.TrimSpace(req.WorkflowKey)

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	definition, err := loadRunnableDefinition(ctx, state.definitionStore, definitionID, req.ExpectedDefinitionGeneration)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	ownerKey := targetOwnerKey(definition.Target)

	if key != "" {
		existing, found, err := loadIdempotencyRecord(ctx, state.idempotencyStore, ownerKey, key)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotency key: %v", err)
		}
		if found {
			run, found, err := loadRunRecord(ctx, state.runStore, ownerKey, existing.RunID)
			if err != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load idempotent run: %v", err)
			}
			if found {
				resp, err := run.toInput()
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
		runID = idempotentManualRunID(ownerKey, key)
		run, found, err := loadRunRecord(ctx, state.runStore, ownerKey, runID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotent run by deterministic id: %v", err)
		}
		if found {
			_ = storeIdempotencyRecord(ctx, state.idempotencyStore, ownerKey, key, run.ID, now)
			resp, err := run.toInput()
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
		ID:                   runID,
		ProviderName:         state.providerName,
		Status:               gestalt.WorkflowRunStatusValuePending,
		Target:               cloneTarget(definition.Target),
		TriggerKind:          triggerKindManual,
		CreatedAt:            now,
		CreatedBySubjectID:   actor,
		DefinitionID:         definition.ID,
		DefinitionGeneration: definition.Generation,
		Input:                cloneAnyMap(req.Input),
		RunAs:                firstSubject(req.RunAs, definition.RunAs),
		WorkflowKey:          workflowKey,
		NextSignalSequence:   1,
	}
	if err := validateWorkflowRunAs(run.RunAs); err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
		if key != "" && errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := loadRunRecord(ctx, state.runStore, ownerKey, run.ID)
			if loadErr != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load existing idempotent run: %v", loadErr)
			}
			if found {
				_ = storeIdempotencyRecord(ctx, state.idempotencyStore, ownerKey, key, existing.ID, now)
				resp, err := existing.toInput()
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
		_ = storeIdempotencyRecord(ctx, state.idempotencyStore, ownerKey, key, run.ID, now)
	}
	if workflowKey != "" {
		if err := storeWorkflowKeyRecord(ctx, state.workflowKeyStore, workflowKey, run.ID, now); err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "store workflow key: %v", err)
		}
	}
	resp, err := run.toInput()
	p.signalWorkerLocked(run.ID)
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := ""
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, appName, runID)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	resp, err := run.toInput()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListRuns(ctx context.Context, req *gestalt.ListWorkflowProviderRunsRequest) (*gestalt.ListWorkflowProviderRunsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := ""

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	runs, err := listRunRecords(ctx, state.runStore, appName)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list runs: %v", err)
	}
	resp := &gestalt.ListWorkflowProviderRunsResponse{Runs: make([]gestalt.WorkflowRun, 0, len(runs))}
	for _, run := range runs {
		pbRun, err := run.toInput()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build run response: %v", err)
		}
		resp.Runs = append(resp.Runs, *pbRun)
	}
	return resp, nil
}

func (p *Provider) GetRunEvents(ctx context.Context, req *gestalt.GetWorkflowProviderRunEventsRequest) (*gestalt.GetWorkflowProviderRunEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	run, err := p.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: req.RunID})
	if err != nil {
		return nil, err
	}
	return &gestalt.GetWorkflowProviderRunEventsResponse{Events: workflowRunEventsFromRun(run)}, nil
}

func (p *Provider) GetRunOutput(ctx context.Context, req *gestalt.GetWorkflowProviderRunOutputRequest) (*gestalt.GetWorkflowProviderRunOutputResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	run, err := p.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: req.RunID})
	if err != nil {
		return nil, err
	}
	return &gestalt.GetWorkflowProviderRunOutputResponse{Output: cloneAny(run.Output)}, nil
}

func (p *Provider) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := ""
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "canceled"
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	run, found, err := loadRunRecord(ctx, state.runStore, appName, runID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	if run.Status != gestalt.WorkflowRunStatusValuePending {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "workflow run %q is %s; only pending runs can be canceled", runID, workflowRunStatusString(run.Status))
	}
	now := p.clock().UTC()
	run.Status = gestalt.WorkflowRunStatusValueCanceled
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
	resp, err := run.toInput()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return resp, nil
}

func (p *Provider) SignalRun(ctx context.Context, req *gestalt.SignalWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	now := p.clock().UTC()
	signal, err := normalizeWorkflowSignal(req.Signal, now)
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

func (p *Provider) SignalOrStartRun(ctx context.Context, req *gestalt.SignalOrStartWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	now := p.clock().UTC()
	signal, err := normalizeWorkflowSignal(req.Signal, now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	definition, err := loadRunnableDefinition(ctx, state.definitionStore, definitionID, req.ExpectedDefinitionGeneration)
	if err != nil {
		return nil, err
	}
	target := scopedTarget{OwnerKey: targetOwnerKey(definition.Target), Target: cloneTarget(definition.Target)}

	var preferredRunID string
	var lastConflictID string
	for attempt := 0; attempt < maxSignalAddRetries; attempt++ {
		tx, stores, err := state.signalOrStartTransaction(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "start signal transaction: %v", err)
		}

		resp, signalID, err := signalOrStartRunInTransaction(ctx, stores, state.providerName, target, definition, req, workflowKey, signal, now, state.runClaimTTL)
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
		preferredRunID = resp.Run.ID
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

func (p *Provider) DeliverEvent(ctx context.Context, req *gestalt.DeliverWorkflowProviderEventRequest) (*gestalt.WorkflowEvent, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := strings.TrimSpace(req.AppName)
	if appName == "" {
		return nil, status.Error(codes.InvalidArgument, "app_name is required")
	}
	eventRequest := cloneWorkflowEvent(req.Event)
	if eventRequest != nil {
		eventRequest.Source = appName
	}
	event, err := normalizeWorkflowEvent(eventRequest, p.clock())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.deliverMu.Lock()
	defer p.deliverMu.Unlock()

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	matches, err := listMatchingDefinitionEventActivations(ctx, state.definitionStore, event)
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "list matching event activations: %v", err)
	}
	now := p.clock().UTC()
	deliveredBy := cloneCreatedBySubjectID(req.DeliveredBySubjectID)
	enqueued := false
	preferredRunID := ""
	for _, match := range matches {
		runID := uuid.NewString()
		if strings.TrimSpace(event.ID) != "" {
			runID = eventRunID(match.Definition.ID, match.Activation.ID, event.Source, event.ID)
		}
		if _, found, err := loadRunRecord(ctx, state.runStore, targetOwnerKey(match.Definition.Target), runID); err != nil {
			p.mu.RUnlock()
			return nil, status.Errorf(codes.Internal, "load event run: %v", err)
		} else if found {
			continue
		}
		createdBy := cloneCreatedBySubjectID(match.Definition.CreatedBySubjectID)
		if createdBySubjectIDSet(deliveredBy) {
			createdBy = cloneCreatedBySubjectID(deliveredBy)
		}
		input, err := eventActivationRunInput(match.Activation, event)
		if err != nil {
			p.mu.RUnlock()
			return nil, status.Errorf(codes.InvalidArgument, "activation %q input: %v", match.Activation.ID, err)
		}
		run := workflowRunRecord{
			ID:                    runID,
			ProviderName:          state.providerName,
			Status:                gestalt.WorkflowRunStatusValuePending,
			Target:                cloneTarget(match.Definition.Target),
			TriggerKind:           triggerKindEvent,
			TriggerEventTriggerID: match.Activation.ID,
			TriggerEvent:          cloneEvent(event),
			CreatedAt:             now,
			CreatedBySubjectID:    createdBy,
			DefinitionID:          match.Definition.ID,
			DefinitionGeneration:  match.Definition.Generation,
			Input:                 cloneAnyMap(input),
			RunAs:                 cloneSubject(match.Definition.RunAs),
			NextSignalSequence:    1,
		}
		if err := validateWorkflowRunAs(run.RunAs); err != nil {
			p.mu.RUnlock()
			return nil, status.Errorf(codes.FailedPrecondition, "workflow definition %q run_as: %v", match.Definition.ID, err)
		}
		if err := state.runStore.Add(ctx, run.toRecord()); err != nil {
			if errors.Is(err, gestalt.ErrAlreadyExists) {
				continue
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
	return cloneWorkflowEvent(event), nil
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

func signalRunInTransaction(ctx context.Context, stores workflowSignalTxStores, runID string, signal *gestalt.WorkflowSignal) (*gestalt.SignalWorkflowRunResponse, string, error) {
	run, found, err := loadRunRecordTx(ctx, stores.runStore, "", runID)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "load run: %v", err)
	}
	if !found {
		return nil, "", status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	if workflowRunTerminal(run.Status) {
		return nil, "", status.Errorf(codes.FailedPrecondition, "workflow run %q is %s", runID, workflowRunStatusString(run.Status))
	}
	if key := strings.TrimSpace(signal.IdempotencyKey); key != "" {
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

func signalOrStartRunInTransaction(ctx context.Context, stores workflowSignalOrStartTxStores, providerName string, target scopedTarget, definition workflowDefinitionRecord, req *gestalt.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *gestalt.WorkflowSignal, now time.Time, runClaimTTL time.Duration) (*gestalt.SignalWorkflowRunResponse, string, error) {
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
	if key := strings.TrimSpace(signal.IdempotencyKey); key != "" {
		existing, found, err := loadIdempotencyRecordTx(ctx, stores.idempotencyStore, target.OwnerKey, key)
		if err != nil {
			return nil, "", status.Errorf(codes.Internal, "load signal idempotency key: %v", err)
		}
		if found && existing.SignalID != "" {
			resp, reusable, err := signalIdempotencyResponseTx(ctx, stores.runStore, stores.signalStore, target.OwnerKey, workflowKey, existing)
			if err != nil || reusable {
				return resp, existing.SignalID, err
			}
			if strings.TrimSpace(enqueueSignal.ID) == existing.SignalID {
				enqueueSignal = cloneSignal(enqueueSignal)
				enqueueSignal.ID = ""
			}
		}
	}
	if !active {
		startedRun = true
		run = workflowRunRecord{
			ID:                   uuid.NewString(),
			ProviderName:         strings.TrimSpace(providerName),
			Status:               gestalt.WorkflowRunStatusValuePending,
			Target:               cloneTarget(target.Target),
			TriggerKind:          triggerKindManual,
			CreatedAt:            now,
			CreatedBySubjectID:   cloneCreatedBySubjectID(req.CreatedBySubjectID),
			DefinitionID:         definition.ID,
			DefinitionGeneration: definition.Generation,
			Input:                cloneAnyMap(req.Input),
			RunAs:                firstSubject(req.RunAs, definition.RunAs),
			WorkflowKey:          workflowKey,
			NextSignalSequence:   1,
		}
		if err := validateWorkflowRunAs(run.RunAs); err != nil {
			return nil, "", status.Error(codes.InvalidArgument, err.Error())
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
	// The existing keyed run owns the definition and target. Later signals
	// deliberately do not replace that context, even if the caller's current
	// config would build a different target.
	return enqueueSignalInTransaction(ctx, stores.runStore, stores.idempotencyStore, stores.signalStore, run, enqueueSignal, startedRun)
}

func enqueueSignalInTransaction(ctx context.Context, runStore recordPutter, idempotencyStore recordPutter, signalStore indexeddb.TransactionObjectStore, run workflowRunRecord, signal *gestalt.WorkflowSignal, startedRun bool) (*gestalt.SignalWorkflowRunResponse, string, error) {
	signal = cloneSignal(signal)
	if strings.TrimSpace(signal.ID) == "" {
		signal.ID = workflowSignalID(run, signal)
	}
	assignSequence := signal.Sequence == 0
	advanceSequence := false

	if assignSequence {
		if run.NextSignalSequence <= 0 {
			return nil, signal.ID, status.Errorf(codes.FailedPrecondition, "workflow run %q has invalid next_signal_sequence", run.ID)
		}
		signal.Sequence = run.NextSignalSequence
		run.NextSignalSequence++
		advanceSequence = true
	} else if signal.Sequence >= run.NextSignalSequence {
		run.NextSignalSequence = signal.Sequence + 1
		advanceSequence = true
	}

	record := workflowSignalRecord{
		ID:             signal.ID,
		RunID:          run.ID,
		WorkflowKey:    run.WorkflowKey,
		State:          signalStatePending,
		Signal:         cloneSignal(signal),
		IdempotencyKey: strings.TrimSpace(signal.IdempotencyKey),
		Sequence:       signal.Sequence,
		StartedRun:     startedRun,
		CreatedAt:      signal.CreatedAt.UTC(),
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
	pbRun, err := run.toInput()
	if err != nil {
		return nil, record.ID, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	return &gestalt.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal,
		StartedRun:  record.StartedRun,
		WorkflowKey: run.WorkflowKey,
	}, record.ID, nil
}

func signalIdempotencyResponseTx(ctx context.Context, runStore, signalStore indexeddb.TransactionObjectStore, appName, workflowKey string, record workflowIdempotencyRecord) (*gestalt.SignalWorkflowRunResponse, bool, error) {
	run, found, err := loadRunRecordTx(ctx, runStore, appName, record.RunID)
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
	pbRun, err := run.toInput()
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	responseWorkflowKey := strings.TrimSpace(record.WorkflowKey)
	if responseWorkflowKey == "" {
		responseWorkflowKey = run.WorkflowKey
	}
	return &gestalt.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal.signalInput(),
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
	return run.Status == gestalt.WorkflowRunStatusValueFailed &&
		run.StatusMessage == staleRunStatusMessage &&
		signal.State == signalStateFailed &&
		signal.StatusMessage == staleRunStatusMessage
}

func signalRecordResponse(ctx context.Context, runStore recordGetter, signal workflowSignalRecord) (*gestalt.SignalWorkflowRunResponse, error) {
	run, found, err := loadRunRecord(ctx, runStore, "", signal.RunID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load signal run: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", signal.RunID)
	}
	pbRun, err := run.toInput()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build run response: %v", err)
	}
	workflowKey := strings.TrimSpace(signal.WorkflowKey)
	if workflowKey == "" {
		workflowKey = run.WorkflowKey
	}
	return &gestalt.SignalWorkflowRunResponse{
		Run:         pbRun,
		Signal:      signal.signalInput(),
		StartedRun:  signal.StartedRun,
		WorkflowKey: workflowKey,
	}, nil
}

func (p *Provider) pollLoop(ctx context.Context, pollInterval time.Duration, wake <-chan string) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	failureBackoff := time.Duration(0)
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
		if err := p.tick(ctx, preferredRunID); err != nil {
			p.logTickError(ctx, err)
			failureBackoff = nextPollFailureBackoff(failureBackoff, pollInterval)
			if err := waitPollFailureBackoff(ctx, failureBackoff, wake); err != nil {
				return
			}
			continue
		}
		failureBackoff = 0
	}
}

func nextPollFailureBackoff(current, pollInterval time.Duration) time.Duration {
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if current <= 0 {
		return pollInterval
	}
	next := current * 2
	if next > maxPollFailureBackoff {
		return maxPollFailureBackoff
	}
	return next
}

func waitPollFailureBackoff(ctx context.Context, delay time.Duration, wake <-chan string) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			continue
		case <-timer.C:
			return nil
		}
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
		definition, activation, found, err := scheduleDefinitionActivation(ctx, state.definitionStore, schedule)
		if err != nil {
			return err
		}
		if !found {
			if err := state.scheduleStore.Delete(ctx, schedule.ID); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
				return err
			}
			continue
		}
		if definition.Paused || activation.Paused || activation.Schedule == nil {
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
		input, err := activationRunInput(activation, nil)
		if err != nil {
			return fmt.Errorf("schedule %q input: %w", schedule.ID, err)
		}
		run := workflowRunRecord{
			ID:                   scheduleRunID(schedule.ID, latestDue),
			ProviderName:         state.providerName,
			Status:               gestalt.WorkflowRunStatusValuePending,
			Target:               cloneTarget(definition.Target),
			TriggerKind:          triggerKindSchedule,
			TriggerScheduleID:    activation.ID,
			TriggerScheduledFor:  timePtr(latestDue),
			CreatedAt:            now,
			CreatedBySubjectID:   cloneCreatedBySubjectID(definition.CreatedBySubjectID),
			DefinitionID:         definition.ID,
			DefinitionGeneration: definition.Generation,
			Input:                cloneAnyMap(input),
			RunAs:                cloneSubject(definition.RunAs),
			NextSignalSequence:   1,
		}
		if err := validateWorkflowRunAs(run.RunAs); err != nil {
			return fmt.Errorf("schedule %q definition %q run_as: %w", schedule.ID, definition.ID, err)
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
	if pending.Status != gestalt.WorkflowRunStatusValueRunning {
		pending.Status = gestalt.WorkflowRunStatusValueRunning
		if pending.StartedAt == nil {
			pending.StartedAt = &now
		}
		pending.CompletedAt = nil
		pending.StatusMessage = ""
		if err := state.runStore.Put(ctx, pending.toRecord()); err != nil {
			err = releaseUninvokedClaim(err)
			releaseWorker()
			return false, err
		}
	}
	executor := state.stepExecutor
	releaseWorker()

	var targetInput *gestalt.BoundWorkflowTarget
	if pending.Target != nil {
		targetInput = cloneTarget(pending.Target)
	}
	signals := signalInputs(claimedSignals)
	stepIndex := nextWorkflowStepIndex(pending)
	stepOutputs := workflowStepOutputsFromExecutions(pending.Steps)
	stepInputs := workflowStepInputsFromExecutions(pending.Steps)
	skippedStepIDs := workflowSkippedStepIDsFromExecutions(pending.Steps)
	stepStartedAt := p.clock().UTC()
	stopRenewingClaim := p.startRunClaimRenewal(ctx, pending.ID, claimOwnerID, runClaimRenewEvery)
	stepResp, invokeErr := executor.ExecuteStep(ctx, gestaltworkflow.StepRequest{
		Request: gestaltworkflow.Request{
			ProviderName:         pending.ProviderName,
			RunID:                pending.ID,
			DefinitionID:         pending.DefinitionID,
			DefinitionGeneration: pending.DefinitionGeneration,
			WorkflowKey:          pending.WorkflowKey,
			Target:               targetInput,
			Trigger:              pending.triggerInput(),
			Input:                cloneAnyMap(pending.Input),
			CreatedBySubjectID:   cloneCreatedBySubjectID(pending.CreatedBySubjectID),
			RunAs:                cloneSubject(pending.RunAs),
			Signals:              signals,
		},
		StepIndex:      stepIndex,
		Outputs:        stepOutputs,
		StepInputs:     stepInputs,
		SkippedStepIDs: skippedStepIDs,
	})
	stopRenewingClaim()

	for attempt := 0; attempt < maxSignalAddRetries; attempt++ {
		if err := p.completeRunAfterStep(ctx, pending, claimedSignals, claimOwnerID, stepStartedAt, stepResp, invokeErr); err != nil {
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

func (p *Provider) completeRunAfterStep(ctx context.Context, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, stepStartedAt time.Time, resp *gestaltworkflow.StepResponse, invokeErr error) error {
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
	if err := completeRunStepInTransaction(ctx, stores, pending, claimedSignals, claimOwnerID, stepStartedAt, resp, invokeErr, completedAt); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func completeRunStepInTransaction(ctx context.Context, stores workflowRunCompletionTxStores, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, stepStartedAt time.Time, resp *gestaltworkflow.StepResponse, invokeErr error, completedAt time.Time) error {
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
	if current.Status != gestalt.WorkflowRunStatusValueRunning {
		return stores.runStore.Put(ctx, current.toRecord())
	}
	if invokeErr != nil {
		current.Status = gestalt.WorkflowRunStatusValueFailed
		current.CompletedAt = &completedAt
		current.StatusMessage = invokeErr.Error()
		if err := markRunSignalsFailedTx(ctx, stores.signalStore, current.ID, claimedSignals, completedAt, invokeErr.Error()); err != nil {
			return err
		}
		if current.WorkflowKey != "" {
			if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
				return err
			}
		}
		return stores.runStore.Put(ctx, current.toRecord())
	}
	if resp == nil {
		current.Status = gestalt.WorkflowRunStatusValueFailed
		current.CompletedAt = &completedAt
		current.StatusMessage = "workflow step executor returned no response"
		if err := markRunSignalsFailedTx(ctx, stores.signalStore, current.ID, claimedSignals, completedAt, current.StatusMessage); err != nil {
			return err
		}
		if current.WorkflowKey != "" {
			if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
				return err
			}
		}
		return stores.runStore.Put(ctx, current.toRecord())
	}
	execution := workflowStepExecutionFromStepResponse(*resp, stepStartedAt, completedAt)
	current.Steps = append(cloneWorkflowStepExecutions(current.Steps), execution)
	current.CurrentStepID = execution.StepID
	if resp.Output != nil || execution.Status == gestalt.WorkflowStepStatusValueSucceeded {
		current.Output = cloneAny(resp.Output)
	}
	if resp.Status >= http.StatusBadRequest || execution.Status == gestalt.WorkflowStepStatusValueFailed {
		current.Status = gestalt.WorkflowRunStatusValueFailed
		current.CompletedAt = &completedAt
		current.StatusMessage = workflowStepFailureMessage(resp, execution.StatusMessage)
		if current.StatusMessage == "" {
			current.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.Status)
		}
		if err := markRunSignalsFailedTx(ctx, stores.signalStore, current.ID, claimedSignals, completedAt, current.StatusMessage); err != nil {
			return err
		}
		if current.WorkflowKey != "" {
			if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
				return err
			}
		}
		return stores.runStore.Put(ctx, current.toRecord())
	}
	if nextWorkflowStepIndex(current) < workflowTargetStepCount(current.Target) {
		current.Status = gestalt.WorkflowRunStatusValuePending
		current.CompletedAt = nil
		current.StatusMessage = ""
		return stores.runStore.Put(ctx, current.toRecord())
	}
	if err := markSignalsDeliveredTx(ctx, stores.signalStore, claimedSignals, completedAt); err != nil {
		return err
	}
	hasPending, err := hasPendingSignalsTx(ctx, stores.signalStore, current.ID)
	if err != nil {
		return err
	}
	if current.WorkflowKey != "" && hasPending {
		current.Status = gestalt.WorkflowRunStatusValuePending
		current.CompletedAt = nil
		current.StatusMessage = ""
		current.CurrentStepID = ""
		current.Steps = nil
		current.Output = nil
		return stores.runStore.Put(ctx, current.toRecord())
	}
	current.Status = gestalt.WorkflowRunStatusValueSucceeded
	current.CompletedAt = &completedAt
	current.StatusMessage = ""
	if current.WorkflowKey != "" {
		if err := deleteWorkflowKeyRecordForRunTx(ctx, stores.workflowKeyStore, current.WorkflowKey, current.ID); err != nil {
			return err
		}
	}
	return stores.runStore.Put(ctx, current.toRecord())
}

func (p *Provider) requireConfiguredLocked() (*configuredState, error) {
	if p.db == nil || p.runStore == nil || p.runClaimStore == nil || p.scheduleStore == nil || p.definitionStore == nil || p.idempotencyStore == nil || p.workflowKeyStore == nil || p.signalStore == nil || p.stepExecutor == nil {
		return nil, errors.New("indexeddb workflow: provider is not configured")
	}
	return &configuredState{
		providerName:       strings.TrimSpace(p.name),
		db:                 p.db,
		stepExecutor:       p.stepExecutor,
		scheduleStore:      p.scheduleStore,
		definitionStore:    p.definitionStore,
		runStore:           p.runStore,
		runClaimStore:      p.runClaimStore,
		idempotencyStore:   p.idempotencyStore,
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
	providerName       string
	db                 indexeddb.Database
	stepExecutor       gestaltworkflow.StepExecutor
	scheduleStore      indexeddb.ObjectStore
	definitionStore    indexeddb.ObjectStore
	runStore           indexeddb.ObjectStore
	runClaimStore      indexeddb.ObjectStore
	idempotencyStore   indexeddb.ObjectStore
	workflowKeyStore   indexeddb.ObjectStore
	signalStore        indexeddb.ObjectStore
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

func transactionGetRecord(ctx context.Context, store indexeddb.TransactionObjectStore, id string) (gestalt.Record, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, false, nil
	}
	records, err := store.GetAll(ctx, indexeddb.Only(id))
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
	runStore         indexeddb.TransactionObjectStore
	runClaimStore    indexeddb.TransactionObjectStore
	idempotencyStore indexeddb.TransactionObjectStore
	workflowKeyStore indexeddb.TransactionObjectStore
	signalStore      indexeddb.TransactionObjectStore
}

func (s *configuredState) signalOrStartTransaction(ctx context.Context) (indexeddb.Transaction, workflowSignalOrStartTxStores, error) {
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
	runStore         indexeddb.TransactionObjectStore
	idempotencyStore indexeddb.TransactionObjectStore
	signalStore      indexeddb.TransactionObjectStore
}

func (s *configuredState) signalTransaction(ctx context.Context) (indexeddb.Transaction, workflowSignalTxStores, error) {
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
	runStore         indexeddb.TransactionObjectStore
	runClaimStore    indexeddb.TransactionObjectStore
	workflowKeyStore indexeddb.TransactionObjectStore
	signalStore      indexeddb.TransactionObjectStore
}

func (s *configuredState) runCompletionTransaction(ctx context.Context) (indexeddb.Transaction, workflowRunCompletionTxStores, error) {
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

func claimWorkflowRun(ctx context.Context, db indexeddb.Database, runID, ownerID string, now time.Time, runClaimTTL time.Duration) (bool, error) {
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

func claimAndStartPendingRun(ctx context.Context, db indexeddb.Database, expected workflowRunRecord, ownerID string, now time.Time, runClaimTTL time.Duration) (workflowRunRecord, bool, error) {
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
		run.Status != gestalt.WorkflowRunStatusValuePending ||
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
	run.Status = gestalt.WorkflowRunStatusValueRunning
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

func renewWorkflowRunClaim(ctx context.Context, db indexeddb.Database, runID, ownerID string, now time.Time, runClaimTTL time.Duration) (bool, error) {
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

func ensureWorkflowObjectStores(ctx context.Context, db indexeddb.Database) error {
	if db == nil {
		return fmt.Errorf("indexeddb database is required")
	}
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreOptions
	}{
		{name: storeSchedules, schema: gestalt.ObjectStoreOptions{}},
		{name: storeDefinitions, schema: gestalt.ObjectStoreOptions{}},
		{name: storeIdempotency, schema: gestalt.ObjectStoreOptions{}},
		{name: storeWorkflowKeys, schema: gestalt.ObjectStoreOptions{}},
		{name: storeRuns, schema: gestalt.ObjectStoreOptions{}},
		{name: storeRunClaims, schema: workflowRunClaimSchema()},
		{name: storeSignals, schema: workflowSignalSchema()},
	} {
		if _, err := db.CreateObjectStore(ctx, def.name, def.schema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			return fmt.Errorf("create %s store: %w", def.name, err)
		}
	}
	return nil
}

func workflowRunClaimSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "claimed_at", Type: gestalt.TypeTime},
			{Name: "expires_at", Type: gestalt.TypeTime},
		},
	}
}

func workflowSignalSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_run", KeyPath: []string{"run_id"}},
			{Name: "by_run_state", KeyPath: []string{"run_id", "state"}},
			{Name: "by_run_sequence", KeyPath: []string{"run_id", "sequence"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "workflow_key", Type: gestalt.TypeString},
			{Name: "state", Type: gestalt.TypeString, NotNull: true},
			{Name: "signal_json", Type: gestalt.TypeString},
			{Name: "idempotency_key", Type: gestalt.TypeString},
			{Name: "sequence", Type: gestalt.TypeInt},
			{Name: "started_run", Type: gestalt.TypeBool},
			{Name: "batch_id", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "claimed_at", Type: gestalt.TypeTime},
			{Name: "delivered_at", Type: gestalt.TypeTime},
			{Name: "failed_at", Type: gestalt.TypeTime},
			{Name: "status_message", Type: gestalt.TypeString},
		},
	}
}

func validateWorkflowSignalIndexes(ctx context.Context, store indexeddb.ObjectStore) error {
	probes := []struct {
		name   string
		values []any
	}{
		{name: "by_run", values: []any{"__workflow_schema_probe__"}},
		{name: "by_run_state", values: []any{"__workflow_schema_probe__", signalStatePending}},
		{name: "by_run_sequence", values: []any{"__workflow_schema_probe__", int64(-1)}},
	}
	for _, probe := range probes {
		if _, err := store.Index(probe.name).Count(ctx, indexQueryKey(probe.values...)); err != nil {
			return fmt.Errorf("%s: %w", probe.name, err)
		}
	}
	return nil
}

func recoverStaleWorkflowRuns(ctx context.Context, db indexeddb.Database, runStore, runClaimStore, workflowKeyStore, signalStore indexeddb.ObjectStore, now time.Time) error {
	return recoverStaleWorkflowRunsWithTTL(ctx, db, runStore, runClaimStore, workflowKeyStore, signalStore, now, defaultRunClaimTTL)
}

func recoverStaleWorkflowRunsWithTTL(ctx context.Context, db indexeddb.Database, runStore, runClaimStore, workflowKeyStore, signalStore indexeddb.ObjectStore, now time.Time, runClaimTTL time.Duration) error {
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
		if claimFound && run.Status != gestalt.WorkflowRunStatusValueRunning && inactiveRunClaimRecoverable(run, claim, now) {
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

func deleteInactiveRunClaimIfRecoverable(ctx context.Context, db indexeddb.Database, observedRun workflowRunRecord, observedClaim workflowRunClaimRecord, now time.Time) (bool, error) {
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
	if run.Status == gestalt.WorkflowRunStatusValueRunning {
		return false
	}
	if workflowRunTerminal(run.Status) || !claim.ExpiresAt.After(now) {
		return true
	}
	if run.Status != gestalt.WorkflowRunStatusValuePending || strings.TrimSpace(run.WorkflowKey) != "" {
		return false
	}
	return !claim.ClaimedAt.Add(nonRunningRunClaimGrace).After(now)
}

func workflowRunRecoverablyStale(ctx context.Context, claimStore recordGetter, run workflowRunRecord, now time.Time, runClaimTTL time.Duration) (bool, error) {
	if run.Status != gestalt.WorkflowRunStatusValueRunning {
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

func workflowRunRecoverablyStaleTx(ctx context.Context, claimStore indexeddb.TransactionObjectStore, run workflowRunRecord, now time.Time, runClaimTTL time.Duration) (bool, error) {
	if run.Status != gestalt.WorkflowRunStatusValueRunning {
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
	if run.StartedAt == nil || run.Target == nil {
		return false
	}
	_, step := firstWorkflowAgentStep(run.Target)
	if step == nil {
		return false
	}
	timeout := defaultAgentRunTimeout
	if step.TimeoutSeconds > 0 {
		timeout = time.Duration(step.TimeoutSeconds) * time.Second
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

func failStaleRunningRun(ctx context.Context, runStore recordPutter, workflowKeyStore indexeddb.ObjectStore, signalStore indexeddb.ObjectStore, run workflowRunRecord, workflowKey string, now time.Time) error {
	run.Status = gestalt.WorkflowRunStatusValueFailed
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

func failStaleRunningRunTx(ctx context.Context, runStore recordPutter, workflowKeyStore indexeddb.TransactionObjectStore, signalStore indexeddb.TransactionObjectStore, run workflowRunRecord, workflowKey string, now time.Time) error {
	run.Status = gestalt.WorkflowRunStatusValueFailed
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

func applyWorkflowExecutionProjection(run *workflowRunRecord, body string, completedAt time.Time) {
	if run == nil {
		return
	}
	result := workflowExecutionResultFromBody(body)
	run.Output = result.FinalOutput
	run.CurrentStepID = result.FinalStepID
	run.Steps = workflowStepExecutionsFromResult(result, completedAt)
}

func workflowExecutionResultFromBody(body string) gestaltworkflow.StepsResult {
	var result gestaltworkflow.StepsResult
	if strings.TrimSpace(body) == "" {
		return result
	}
	_ = json.Unmarshal([]byte(body), &result)
	return result
}

func workflowStepExecutionsFromResult(result gestaltworkflow.StepsResult, completedAt time.Time) []gestalt.WorkflowStepExecution {
	if len(result.Steps) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowStepExecution, 0, len(result.Steps))
	for _, step := range result.Steps {
		statusValue := workflowStepStatusFromString(step.Status)
		output := cloneAny(result.Outputs[strings.TrimSpace(step.ID)])
		message := ""
		if step.Error != nil {
			message = strings.TrimSpace(step.Error.Message)
		}
		execution := gestalt.WorkflowStepExecution{
			StepID:        strings.TrimSpace(step.ID),
			Status:        statusValue,
			Output:        output,
			StatusMessage: message,
			SkipReason:    strings.TrimSpace(step.SkippedReason),
			CompletedAt:   timePtr(completedAt),
		}
		if statusValue == gestalt.WorkflowStepStatusValueSucceeded ||
			statusValue == gestalt.WorkflowStepStatusValueFailed ||
			statusValue == gestalt.WorkflowStepStatusValueSkipped {
			execution.Attempts = []gestalt.WorkflowStepAttempt{{
				ID:            execution.StepID + ":1",
				Status:        statusValue,
				Output:        output,
				StatusMessage: message,
				CompletedAt:   timePtr(completedAt),
			}}
		}
		out = append(out, execution)
	}
	return out
}

func nextWorkflowStepIndex(run workflowRunRecord) int {
	return len(run.Steps)
}

func workflowTargetStepCount(target *gestalt.BoundWorkflowTarget) int {
	if target == nil {
		return 0
	}
	return len(target.Steps)
}

func workflowStepOutputsFromExecutions(steps []gestalt.WorkflowStepExecution) map[string]any {
	if len(steps) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, step := range steps {
		if step.Status != gestalt.WorkflowStepStatusValueSucceeded {
			continue
		}
		stepID := strings.TrimSpace(step.StepID)
		if stepID == "" {
			continue
		}
		out[stepID] = cloneAny(step.Output)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowStepInputsFromExecutions(steps []gestalt.WorkflowStepExecution) map[string]any {
	if len(steps) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, step := range steps {
		stepID := strings.TrimSpace(step.StepID)
		if stepID == "" {
			continue
		}
		out[stepID] = cloneAny(step.Input)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowSkippedStepIDsFromExecutions(steps []gestalt.WorkflowStepExecution) []string {
	if len(steps) == 0 {
		return nil
	}
	out := make([]string, 0)
	for _, step := range steps {
		if step.Status != gestalt.WorkflowStepStatusValueSkipped {
			continue
		}
		if stepID := strings.TrimSpace(step.StepID); stepID != "" {
			out = append(out, stepID)
		}
	}
	return out
}

func workflowStepExecutionFromStepResponse(resp gestaltworkflow.StepResponse, startedAt, completedAt time.Time) gestalt.WorkflowStepExecution {
	statusValue := workflowStepStatusFromString(resp.Step.Status)
	stepID := strings.TrimSpace(resp.Step.ID)
	input := cloneAny(resp.Input)
	output := cloneAny(resp.Output)
	message := ""
	if resp.Step.Error != nil {
		message = strings.TrimSpace(resp.Step.Error.Message)
	}
	execution := gestalt.WorkflowStepExecution{
		StepID:        stepID,
		Status:        statusValue,
		Input:         input,
		Output:        output,
		StatusMessage: message,
		SkipReason:    strings.TrimSpace(resp.Step.SkippedReason),
		StartedAt:     timePtr(startedAt),
		CompletedAt:   timePtr(completedAt),
	}
	if workflowStepStatusTerminal(statusValue) {
		execution.Attempts = []gestalt.WorkflowStepAttempt{{
			ID:            execution.StepID + ":1",
			Status:        statusValue,
			Input:         input,
			Output:        output,
			StatusMessage: message,
			StartedAt:     timePtr(startedAt),
			CompletedAt:   timePtr(completedAt),
		}}
	}
	return execution
}

func workflowStepStatusTerminal(status gestalt.WorkflowStepStatus) bool {
	switch status {
	case gestalt.WorkflowStepStatusValueSkipped,
		gestalt.WorkflowStepStatusValueSucceeded,
		gestalt.WorkflowStepStatusValueFailed:
		return true
	default:
		return false
	}
}

func workflowStepFailureMessage(resp *gestaltworkflow.StepResponse, fallback string) string {
	if resp == nil {
		return strings.TrimSpace(fallback)
	}
	if resp.Step.Error != nil && strings.TrimSpace(resp.Step.Error.Message) != "" {
		return strings.TrimSpace(resp.Step.Error.Message)
	}
	return strings.TrimSpace(fallback)
}

func workflowStepStatusFromString(status string) gestalt.WorkflowStepStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return gestalt.WorkflowStepStatusValuePending
	case "running":
		return gestalt.WorkflowStepStatusValueRunning
	case "skipped":
		return gestalt.WorkflowStepStatusValueSkipped
	case "succeeded", "success":
		return gestalt.WorkflowStepStatusValueSucceeded
	case "failed", "failure":
		return gestalt.WorkflowStepStatusValueFailed
	case "unknown":
		return gestalt.WorkflowStepStatusValueUnknown
	default:
		return gestalt.WorkflowStepStatusValueUnspecified
	}
}

func workflowRunEventsFromRun(run *gestalt.WorkflowRun) []gestalt.WorkflowRunEvent {
	if run == nil {
		return nil
	}
	events := []gestalt.WorkflowRunEvent{{
		ID:        run.ID + ":run",
		RunID:     run.ID,
		Type:      "run." + workflowRunStatusString(run.Status),
		Data:      map[string]any{"status": workflowRunStatusString(run.Status)},
		CreatedAt: run.CreatedAt,
	}}
	for _, step := range run.Steps {
		createdAt := run.CreatedAt
		if step.CompletedAt != nil {
			createdAt = *step.CompletedAt
		}
		events = append(events, gestalt.WorkflowRunEvent{
			ID:        run.ID + ":step:" + strings.TrimSpace(step.StepID),
			RunID:     run.ID,
			StepID:    step.StepID,
			Type:      "step." + workflowStepStatusEventName(step.Status),
			Data:      map[string]any{"status": workflowStepStatusEventName(step.Status), "message": step.StatusMessage},
			CreatedAt: createdAt,
		})
	}
	return events
}

func workflowStepStatusEventName(status gestalt.WorkflowStepStatus) string {
	switch status {
	case gestalt.WorkflowStepStatusValuePending:
		return "pending"
	case gestalt.WorkflowStepStatusValueRunning:
		return "running"
	case gestalt.WorkflowStepStatusValueSkipped:
		return "skipped"
	case gestalt.WorkflowStepStatusValueSucceeded:
		return "succeeded"
	case gestalt.WorkflowStepStatusValueFailed:
		return "failed"
	case gestalt.WorkflowStepStatusValueUnknown:
		return "unknown"
	default:
		return "unspecified"
	}
}

func loadRunnableDefinition(ctx context.Context, store indexeddb.ObjectStore, definitionID string, expectedGeneration int64) (workflowDefinitionRecord, error) {
	definition, found, err := loadDefinitionRecord(ctx, store, definitionID)
	if err != nil {
		return workflowDefinitionRecord{}, status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	if !found {
		return workflowDefinitionRecord{}, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	if definition.Paused {
		return workflowDefinitionRecord{}, status.Errorf(codes.FailedPrecondition, "workflow definition %q is paused", definitionID)
	}
	if expectedGeneration > 0 && definition.Generation != expectedGeneration {
		return workflowDefinitionRecord{}, status.Errorf(codes.FailedPrecondition, "workflow definition %q generation is %d, want %d", definitionID, definition.Generation, expectedGeneration)
	}
	if _, err := normalizeTarget(definition.Target); err != nil {
		return workflowDefinitionRecord{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return definition, nil
}

func normalizeWorkflowActivations(activations []gestalt.WorkflowActivation) ([]gestalt.WorkflowActivation, error) {
	if len(activations) == 0 {
		return nil, nil
	}
	out := make([]gestalt.WorkflowActivation, 0, len(activations))
	seen := map[string]struct{}{}
	for i, activation := range activations {
		path := fmt.Sprintf("activations[%d]", i)
		activation.ID = strings.TrimSpace(activation.ID)
		if activation.ID == "" {
			return nil, fmt.Errorf("%s.id is required", path)
		}
		if _, ok := seen[activation.ID]; ok {
			return nil, fmt.Errorf("%s.id duplicates %q", path, activation.ID)
		}
		seen[activation.ID] = struct{}{}
		switch {
		case activation.Schedule != nil && activation.Event != nil:
			return nil, fmt.Errorf("%s must set exactly one of schedule or event", path)
		case activation.Schedule != nil:
			schedule := *activation.Schedule
			schedule.Cron = strings.TrimSpace(schedule.Cron)
			if schedule.Cron == "" {
				return nil, fmt.Errorf("%s.schedule.cron is required", path)
			}
			if _, _, err := parseTimezone(schedule.Timezone); err != nil {
				return nil, fmt.Errorf("%s.schedule.timezone: %w", path, err)
			}
			if _, err := cronParser().Parse(schedule.Cron); err != nil {
				return nil, fmt.Errorf("%s.schedule.cron: %w", path, err)
			}
			activation.Schedule = &schedule
		case activation.Event != nil:
			event := *activation.Event
			if event.Match == nil || strings.TrimSpace(event.Match.Type) == "" {
				return nil, fmt.Errorf("%s.event.match.type is required", path)
			}
			match := *event.Match
			match.Type = strings.TrimSpace(match.Type)
			match.Source = strings.TrimSpace(match.Source)
			match.Subject = strings.TrimSpace(match.Subject)
			event.Match = &match
			activation.Event = &event
		default:
			return nil, fmt.Errorf("%s must set schedule or event", path)
		}
		out = append(out, activation)
	}
	return cloneWorkflowActivations(out), nil
}

func eventActivationRunInput(activation gestalt.WorkflowActivation, event *gestalt.WorkflowEvent) (map[string]any, error) {
	if event == nil {
		return nil, nil
	}
	signal := gestalt.WorkflowSignal{Name: strings.TrimSpace(event.Type), Payload: eventToMap(event)}
	return activationRunInput(activation, []gestalt.WorkflowSignal{signal})
}

func activationRunInput(activation gestalt.WorkflowActivation, signals []gestalt.WorkflowSignal) (map[string]any, error) {
	if !workflowValueSet(activation.Input) {
		return nil, nil
	}
	resolved, ok, err := (gestaltworkflow.EvalContext{
		Request: gestaltworkflow.Request{Signals: signals},
	}).EvaluateValue(activation.Input)
	if err != nil {
		return nil, err
	}
	if !ok || resolved == nil {
		return nil, nil
	}
	object, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("activation input must resolve to an object")
	}
	return cloneAnyMap(object), nil
}

func scheduleDefinitionActivation(ctx context.Context, store indexeddb.ObjectStore, schedule workflowScheduleRecord) (workflowDefinitionRecord, gestalt.WorkflowActivation, bool, error) {
	definition, found, err := loadDefinitionRecord(ctx, store, schedule.DefinitionID)
	if err != nil || !found {
		return workflowDefinitionRecord{}, gestalt.WorkflowActivation{}, false, err
	}
	activationID := strings.TrimSpace(schedule.ActivationID)
	for _, activation := range definition.Activations {
		if strings.TrimSpace(activation.ID) == activationID {
			return definition, activation, true, nil
		}
	}
	return workflowDefinitionRecord{}, gestalt.WorkflowActivation{}, false, nil
}

func workflowValueSet(value gestalt.WorkflowValue) bool {
	return value.LiteralSet ||
		value.Object != nil ||
		value.Array != nil ||
		value.Template != nil ||
		strings.TrimSpace(value.Input) != "" ||
		strings.TrimSpace(value.Signal) != "" ||
		value.StepOutput != nil ||
		value.StepInput != nil
}

func normalizeTarget(target *gestalt.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	if len(target.Steps) == 0 {
		return scopedTarget{}, errors.New("target.steps is required")
	}
	steps := append([]gestalt.WorkflowStep(nil), target.Steps...)
	seen := map[string]struct{}{}
	ownerKey := ""
	for i := range steps {
		step := &steps[i]
		stepPath := fmt.Sprintf("target.steps[%d]", i)
		step.ID = strings.TrimSpace(step.ID)
		if step.ID == "" {
			return scopedTarget{}, fmt.Errorf("%s.id is required", stepPath)
		}
		if _, exists := seen[step.ID]; exists {
			return scopedTarget{}, fmt.Errorf("%s.id duplicates %q", stepPath, step.ID)
		}
		if step.TimeoutSeconds < 0 {
			return scopedTarget{}, fmt.Errorf("%s.timeout_seconds must not be negative", stepPath)
		}
		switch {
		case step.App != nil && step.Agent != nil:
			return scopedTarget{}, fmt.Errorf("%s must set exactly one of app or agent", stepPath)
		case step.App != nil:
			app, stepOwner, err := normalizeWorkflowStepApp(step.App, stepPath+".app")
			if err != nil {
				return scopedTarget{}, err
			}
			step.App = app
			if ownerKey == "" {
				ownerKey = stepOwner
			}
		case step.Agent != nil:
			agent, stepOwner, err := normalizeWorkflowStepAgent(step.Agent, stepPath+".agent")
			if err != nil {
				return scopedTarget{}, err
			}
			step.Agent = agent
			if ownerKey == "" {
				ownerKey = stepOwner
			}
		default:
			return scopedTarget{}, fmt.Errorf("%s must set app or agent", stepPath)
		}
		seen[step.ID] = struct{}{}
	}
	if ownerKey == "" {
		return scopedTarget{}, errors.New("target owner is required")
	}
	return scopedTarget{
		OwnerKey: ownerKey,
		Target:   cloneTarget(&gestalt.BoundWorkflowTarget{Steps: steps}),
	}, nil
}

func normalizeWorkflowStepApp(app *gestalt.WorkflowStepAppCall, path string) (*gestalt.WorkflowStepAppCall, string, error) {
	if app == nil {
		return nil, "", fmt.Errorf("%s is required", path)
	}
	out := *app
	appName := strings.TrimSpace(out.Name)
	operation := strings.TrimSpace(out.Operation)
	if appName == "" {
		return nil, "", fmt.Errorf("%s.name is required", path)
	}
	if operation == "" {
		return nil, "", fmt.Errorf("%s.operation is required", path)
	}
	credentialMode := strings.ToLower(strings.TrimSpace(out.CredentialMode))
	switch credentialMode {
	case "", "none", "subject":
	default:
		return nil, "", fmt.Errorf("%s.credential_mode %q is not supported", path, out.CredentialMode)
	}
	out.Name = appName
	out.Operation = operation
	out.Connection = strings.TrimSpace(out.Connection)
	out.Instance = strings.TrimSpace(out.Instance)
	out.CredentialMode = credentialMode
	return &out, appName, nil
}

func normalizeWorkflowStepAgent(agent *gestalt.WorkflowStepAgentTurn, path string) (*gestalt.WorkflowStepAgentTurn, string, error) {
	if agent == nil {
		return nil, "", fmt.Errorf("%s is required", path)
	}
	out := *agent
	providerName := strings.TrimSpace(out.Provider)
	out.Model = strings.TrimSpace(out.Model)
	out.SessionKey = strings.TrimSpace(out.SessionKey)
	out.Prompt = gestalt.WorkflowText{Template: strings.TrimSpace(out.Prompt.Template)}
	if providerName == "" {
		return nil, "", fmt.Errorf("%s.provider is required", path)
	}
	if out.Prompt.Template == "" && len(out.Messages) == 0 {
		return nil, "", fmt.Errorf("%s.prompt or messages is required", path)
	}
	out.Provider = providerName
	if out.Workspace != nil {
		cloned := *out.Workspace
		cloned.Checkouts = append([]gestalt.AgentWorkspaceGitCheckout(nil), out.Workspace.Checkouts...)
		out.Workspace = &cloned
	}
	return &out, "agent:" + providerName, nil
}

func targetOwnerKey(target *gestalt.BoundWorkflowTarget) string {
	if target == nil || len(target.Steps) == 0 {
		return ""
	}
	for _, step := range target.Steps {
		if step.App != nil {
			if appName := strings.TrimSpace(step.App.Name); appName != "" {
				return appName
			}
		}
		if step.Agent != nil {
			if provider := strings.TrimSpace(step.Agent.Provider); provider != "" {
				return "agent:" + provider
			}
		}
	}
	return ""
}

func firstWorkflowAppStep(target *gestalt.BoundWorkflowTarget) *gestalt.WorkflowStepAppCall {
	if target == nil {
		return nil
	}
	for i := range target.Steps {
		if target.Steps[i].App != nil {
			return target.Steps[i].App
		}
	}
	return nil
}

func firstWorkflowAgentStep(target *gestalt.BoundWorkflowTarget) (*gestalt.WorkflowStepAgentTurn, *gestalt.WorkflowStep) {
	if target == nil {
		return nil, nil
	}
	for i := range target.Steps {
		if target.Steps[i].Agent != nil {
			step := target.Steps[i]
			return step.Agent, &step
		}
	}
	return nil, nil
}

func targetHasAppStep(target *gestalt.BoundWorkflowTarget) bool {
	return firstWorkflowAppStep(target) != nil
}

func targetHasAgentStep(target *gestalt.BoundWorkflowTarget) bool {
	agent, _ := firstWorkflowAgentStep(target)
	return agent != nil
}

func normalizeWorkflowEvent(event *gestalt.WorkflowEvent, now time.Time) (*gestalt.WorkflowEvent, error) {
	if event == nil {
		return nil, errors.New("event is required")
	}
	eventType := strings.TrimSpace(event.Type)
	if eventType == "" {
		return nil, errors.New("event.type is required")
	}
	id := strings.TrimSpace(event.ID)
	if id == "" {
		id = uuid.NewString()
	}
	specVersion := strings.TrimSpace(event.SpecVersion)
	if specVersion == "" {
		specVersion = defaultSpecVersion
	}
	eventTime := now.UTC()
	if !event.Time.IsZero() {
		eventTime = event.Time.UTC()
	}
	return cloneWorkflowEvent(&gestalt.WorkflowEvent{
		ID:              id,
		Source:          strings.TrimSpace(event.Source),
		SpecVersion:     specVersion,
		Type:            eventType,
		Subject:         strings.TrimSpace(event.Subject),
		Time:            eventTime,
		DataContentType: strings.TrimSpace(event.DataContentType),
		Data:            cloneAny(event.Data),
		Extensions:      cloneAnyMap(event.Extensions),
	}), nil
}

func normalizeWorkflowSignal(signal *gestalt.WorkflowSignal, now time.Time) (*gestalt.WorkflowSignal, error) {
	if signal == nil {
		return nil, errors.New("signal is required")
	}
	name := strings.TrimSpace(signal.Name)
	if name == "" {
		return nil, errors.New("signal.name is required")
	}
	createdAt := now.UTC()
	if !signal.CreatedAt.IsZero() {
		createdAt = signal.CreatedAt.UTC()
	}
	return cloneWorkflowSignal(&gestalt.WorkflowSignal{
		ID:                 strings.TrimSpace(signal.ID),
		Name:               name,
		Payload:            cloneAny(signal.Payload),
		Metadata:           cloneAny(signal.Metadata),
		CreatedBySubjectID: cloneCreatedBySubjectID(signal.CreatedBySubjectID),
		CreatedAt:          createdAt,
		IdempotencyKey:     strings.TrimSpace(signal.IdempotencyKey),
		Sequence:           signal.Sequence,
	}), nil
}

func workflowRunTerminal(status gestalt.WorkflowRunStatus) bool {
	switch status {
	case gestalt.WorkflowRunStatusValueSucceeded,
		gestalt.WorkflowRunStatusValueFailed,
		gestalt.WorkflowRunStatusValueCanceled:
		return true
	default:
		return false
	}
}

func workflowRunStatusString(status gestalt.WorkflowRunStatus) string {
	switch status {
	case gestalt.WorkflowRunStatusValuePending:
		return "pending"
	case gestalt.WorkflowRunStatusValueRunning:
		return "running"
	case gestalt.WorkflowRunStatusValueSucceeded:
		return "succeeded"
	case gestalt.WorkflowRunStatusValueFailed:
		return "failed"
	case gestalt.WorkflowRunStatusValueCanceled:
		return "canceled"
	default:
		return "unspecified"
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

func nextPendingRun(ctx context.Context, store indexeddb.ObjectStore) (workflowRunRecord, bool, error) {
	runs, err := listRunRecords(ctx, store, "")
	if err != nil {
		return workflowRunRecord{}, false, err
	}
	for _, run := range runs {
		if run.Status == gestalt.WorkflowRunStatusValuePending {
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
	if run.TriggerKind == triggerKindEvent && run.Target != nil && targetHasAppStep(run.Target) {
		return 0
	}
	// Agent metadata may opt runs into a custom dispatch tier via
	// _gestalt.workflow.dispatchPriority. Priority 0 is reserved for app
	// event ingestion, so custom priorities start at 1.
	if priority, ok := workflowRunMetadataDispatchPriority(run); ok {
		return priority
	}
	if strings.TrimSpace(run.WorkflowKey) != "" {
		return 10
	}
	if run.Target != nil && targetHasAppStep(run.Target) {
		return 20
	}
	if run.TriggerKind == triggerKindEvent {
		return 30
	}
	if run.Target != nil && targetHasAgentStep(run.Target) {
		return 40
	}
	return 50
}

func workflowRunMetadataDispatchPriority(run workflowRunRecord) (int, bool) {
	if run.Target == nil {
		return 0, false
	}
	_, step := firstWorkflowAgentStep(run.Target)
	if step == nil || step.Metadata == nil {
		return 0, false
	}
	metadata := anyMap(step.Metadata)
	rawGestalt, ok := metadata[gestaltInputKey]
	if !ok {
		return 0, false
	}
	gestaltMetadata, ok := rawGestalt.(map[string]any)
	if !ok {
		return 0, false
	}
	rawWorkflow, ok := gestaltMetadata[workflowMetadataKey]
	if !ok {
		return 0, false
	}
	workflowMetadata, ok := rawWorkflow.(map[string]any)
	if !ok {
		return 0, false
	}
	priority, ok := parseWorkflowDispatchPriority(workflowMetadata[dispatchPriorityMetadataKey])
	if !ok || priority < 1 {
		return 0, false
	}
	return priority, true
}

func parseWorkflowDispatchPriority(raw any) (int, bool) {
	switch value := raw.(type) {
	case float64:
		priority := int(value)
		if float64(priority) != value {
			return 0, false
		}
		return priority, true
	case string:
		priority, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0, false
		}
		return priority, true
	default:
		return 0, false
	}
}

func claimRunnableRun(ctx context.Context, state *configuredState, run workflowRunRecord, now time.Time) (workflowRunRecord, bool, error) {
	runnable, err := workflowRunRunnable(ctx, state.signalStore, run)
	if err != nil || !runnable {
		return workflowRunRecord{}, false, err
	}
	if run.Status == gestalt.WorkflowRunStatusValuePending && strings.TrimSpace(run.WorkflowKey) == "" {
		return claimAndStartPendingRun(ctx, state.db, run, state.claimOwnerID, now, state.runClaimTTL)
	}
	claimed, err := claimWorkflowRun(ctx, state.db, run.ID, state.claimOwnerID, now, state.runClaimTTL)
	if err != nil || !claimed {
		return workflowRunRecord{}, false, err
	}
	return run, true, nil
}

func nextRunnableRun(ctx context.Context, runStore, signalStore indexeddb.ObjectStore, preferredRunID string) (workflowRunRecord, bool, error) {
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

func workflowRunRunnable(ctx context.Context, signalStore indexeddb.ObjectStore, run workflowRunRecord) (bool, error) {
	if run.Status == gestalt.WorkflowRunStatusValuePending {
		if strings.TrimSpace(run.WorkflowKey) == "" {
			return true, nil
		}
		hasPending, err := hasPendingSignals(ctx, signalStore, run.ID)
		if err != nil {
			return false, err
		}
		if hasPending {
			return true, nil
		}
		return hasClaimedSignals(ctx, signalStore, run.ID)
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

func hasPendingSignals(ctx context.Context, store indexeddb.ObjectStore, runID string) (bool, error) {
	return hasSignalsInState(ctx, store, runID, signalStatePending)
}

func hasClaimedSignals(ctx context.Context, store indexeddb.ObjectStore, runID string) (bool, error) {
	return hasSignalsInState(ctx, store, runID, signalStateClaimed)
}

func hasPendingSignalsTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	count, err := store.Index("by_run_state").Count(ctx, []any{runID, signalStatePending})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func hasSignalsInState(ctx context.Context, store indexeddb.ObjectStore, runID, state string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	count, err := store.Index("by_run_state").Count(ctx, []any{runID, strings.TrimSpace(state)})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func signalInputs(records []workflowSignalRecord) []gestalt.WorkflowSignal {
	if len(records) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowSignal, 0, len(records))
	for _, record := range records {
		out = append(out, *record.signalInput())
	}
	return out
}

func markRunSignalsFailed(ctx context.Context, store indexeddb.ObjectStore, runID string, claimed []workflowSignalRecord, failedAt time.Time, message string) error {
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

func markSignalsDelivered(ctx context.Context, store indexeddb.ObjectStore, records []workflowSignalRecord, deliveredAt time.Time) error {
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

func markSignalsDeliveredTx(ctx context.Context, store indexeddb.TransactionObjectStore, records []workflowSignalRecord, deliveredAt time.Time) error {
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

func markSignalsFailed(ctx context.Context, store indexeddb.ObjectStore, records []workflowSignalRecord, failedAt time.Time, message string) error {
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

func markRunSignalsFailedTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID string, claimed []workflowSignalRecord, failedAt time.Time, message string) error {
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

func markSignalsFailedTx(ctx context.Context, store indexeddb.TransactionObjectStore, records []workflowSignalRecord, failedAt time.Time, message string) error {
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

func activeWorkflowKeyRunInTransaction(ctx context.Context, workflowKeyStore, runStore indexeddb.TransactionObjectStore, workflowKey string) (workflowRunRecord, bool, error) {
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

func loadWorkflowKeyRecordTx(ctx context.Context, store indexeddb.TransactionObjectStore, workflowKey string) (workflowKeyRecord, bool, error) {
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

func loadRunClaimRecordTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID string) (workflowRunClaimRecord, bool, error) {
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

func deleteWorkflowKeyRecordForRun(ctx context.Context, store indexeddb.ObjectStore, workflowKey, runID string) error {
	key, found, err := loadWorkflowKeyRecord(ctx, store, workflowKey)
	if err != nil || !found {
		return err
	}
	if key.RunID != strings.TrimSpace(runID) {
		return nil
	}
	return deleteWorkflowKeyRecord(ctx, store, workflowKey)
}

func deleteWorkflowKeyRecordForRunTx(ctx context.Context, store indexeddb.TransactionObjectStore, workflowKey, runID string) error {
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

func workflowSignalID(run workflowRunRecord, signal *gestalt.WorkflowSignal) string {
	key := strings.TrimSpace(signal.IdempotencyKey)
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

func loadSignalRecordTx(ctx context.Context, store indexeddb.TransactionObjectStore, signalID string) (workflowSignalRecord, bool, error) {
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

func listSignalRecords(ctx context.Context, store indexeddb.ObjectStore, runID, state string) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case runID != "" && state != "":
		records, err = store.Index("by_run_state").GetAll(ctx, []any{runID, state})
	case runID != "":
		records, err = store.Index("by_run").GetAll(ctx, runID)
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

func listSignalRecordsTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID, state string) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	var (
		records []gestalt.Record
		err     error
	)
	switch {
	case runID != "" && state != "":
		records, err = store.Index("by_run_state").GetAll(ctx, []any{runID, state})
	case runID != "":
		records, err = store.Index("by_run").GetAll(ctx, runID)
	default:
		records, err = store.GetAll(ctx, nil)
	}
	if err != nil {
		return nil, err
	}
	return signalRecordsFromRecords(records, runID, state)
}

func listSignalRecordsLimit(ctx context.Context, store indexeddb.ObjectStore, runID, state string, limit int) ([]workflowSignalRecord, error) {
	runID = strings.TrimSpace(runID)
	state = strings.TrimSpace(state)
	if runID == "" || limit <= 0 {
		return listSignalRecords(ctx, store, runID, state)
	}

	cursor, err := store.Index("by_run_sequence").OpenKeyCursor(ctx, indexeddb.LowerBound([]any{runID}, false), gestalt.CursorNext)
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

func loadScheduleRecord(ctx context.Context, store indexeddb.ObjectStore, ownerKey, scheduleID string) (workflowScheduleRecord, bool, error) {
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

func listScheduleRecords(ctx context.Context, store indexeddb.ObjectStore, ownerKey string) ([]workflowScheduleRecord, error) {
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

type workflowDefinitionActivationMatch struct {
	Definition workflowDefinitionRecord
	Activation gestalt.WorkflowActivation
}

func listMatchingDefinitionEventActivations(ctx context.Context, store indexeddb.ObjectStore, event *gestalt.WorkflowEvent) ([]workflowDefinitionActivationMatch, error) {
	definitions, err := listDefinitionRecords(ctx, store)
	if err != nil {
		return nil, err
	}
	out := make([]workflowDefinitionActivationMatch, 0)
	for _, definition := range definitions {
		if definition.Paused {
			continue
		}
		for _, activation := range definition.Activations {
			if activation.Paused || activation.Event == nil || activation.Event.Match == nil {
				continue
			}
			if workflowEventMatches(activation.Event.Match, event) {
				out = append(out, workflowDefinitionActivationMatch{Definition: definition, Activation: activation})
			}
		}
	}
	slices.SortFunc(out, func(a, b workflowDefinitionActivationMatch) int {
		if cmp := a.Definition.CreatedAt.Compare(b.Definition.CreatedAt); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Definition.ID, b.Definition.ID); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Activation.ID, b.Activation.ID)
	})
	return out, nil
}

func workflowEventMatches(match *gestalt.WorkflowEventMatch, event *gestalt.WorkflowEvent) bool {
	if match == nil || event == nil {
		return false
	}
	if strings.TrimSpace(match.Type) == "" || strings.TrimSpace(match.Type) != strings.TrimSpace(event.Type) {
		return false
	}
	if source := strings.TrimSpace(match.Source); source != "" && source != strings.TrimSpace(event.Source) {
		return false
	}
	if subject := strings.TrimSpace(match.Subject); subject != "" && subject != strings.TrimSpace(event.Subject) {
		return false
	}
	return true
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

func loadRunRecordTx(ctx context.Context, store indexeddb.TransactionObjectStore, ownerKey, runID string) (workflowRunRecord, bool, error) {
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

func listRunRecords(ctx context.Context, store indexeddb.ObjectStore, ownerKey string) ([]workflowRunRecord, error) {
	cursor, err := store.OpenCursor(ctx, nil, gestalt.CursorNext)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var out []workflowRunRecord
	for cursor.Continue() {
		record, err := cursor.Value()
		if err != nil {
			return nil, err
		}
		run, err := runRecordFromRecord(record)
		if err != nil {
			return nil, err
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

func loadIdempotencyRecordTx(ctx context.Context, store indexeddb.TransactionObjectStore, ownerKey, key string) (workflowIdempotencyRecord, bool, error) {
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

func syncDefinitionScheduleActivations(ctx context.Context, store indexeddb.ObjectStore, definition workflowDefinitionRecord, now time.Time) error {
	existing, err := listScheduleRecords(ctx, store, "")
	if err != nil {
		return err
	}
	for _, record := range existing {
		if record.DefinitionID == definition.ID {
			if err := store.Delete(ctx, record.ID); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
				return err
			}
		}
	}
	parser := cronParser()
	for _, activation := range definition.Activations {
		if activation.Schedule == nil {
			continue
		}
		location, timezone, err := parseTimezone(activation.Schedule.Timezone)
		if err != nil {
			return fmt.Errorf("activation %q timezone: %w", activation.ID, err)
		}
		next, err := nextCronTime(parser, activation.Schedule.Cron, location, now)
		if err != nil {
			return fmt.Errorf("activation %q cron: %w", activation.ID, err)
		}
		record := workflowScheduleRecord{
			ID:                   scheduleCursorID(definition.ID, activation.ID),
			ActivationID:         activation.ID,
			Cron:                 activation.Schedule.Cron,
			Timezone:             timezone,
			Target:               cloneTarget(definition.Target),
			Paused:               definition.Paused || activation.Paused,
			CreatedAt:            now.UTC(),
			UpdatedAt:            now.UTC(),
			NextRunAt:            next,
			CreatedBySubjectID:   cloneCreatedBySubjectID(definition.CreatedBySubjectID),
			DefinitionID:         definition.ID,
			DefinitionGeneration: definition.Generation,
			RunAs:                cloneSubject(definition.RunAs),
		}
		if err := store.Put(ctx, record.toRecord()); err != nil {
			return err
		}
	}
	return nil
}

func idempotencyID(ownerKey, key string) string {
	return ownerKey + ":" + key
}

func hashScopedID(prefix string, parts ...string) string {
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		normalized = append(normalized, strings.TrimSpace(part))
	}
	sum := sha256.Sum256([]byte(strings.Join(normalized, "\x00")))
	return strings.TrimSpace(prefix) + ":" + hex.EncodeToString(sum[:16])
}

func idempotentManualRunID(ownerKey, key string) string {
	sum := sha256.Sum256([]byte(ownerKey + "\x00" + key))
	return "manual:" + hex.EncodeToString(sum[:16])
}

func eventRunID(definitionID, activationID, eventSource, eventID string) string {
	return hashScopedID("event", definitionID, activationID, eventSource, eventID)
}

func createdBySubjectIDSet(subjectID string) bool {
	return strings.TrimSpace(subjectID) != ""
}

func createdByForUpsert(existing, requested string) string {
	if isConfigManagedSubjectID(requested) {
		return cloneCreatedBySubjectID(requested)
	}
	return cloneCreatedBySubjectID(existing)
}

func isConfigManagedSubjectID(subjectID string) bool {
	return strings.TrimSpace(subjectID) == configManagedWorkflowSubject
}

func scheduleRunID(scheduleID string, scheduledFor time.Time) string {
	return "schedule:" + scheduleID + ":" + scheduledFor.UTC().Format(time.RFC3339Nano)
}

func scheduleCursorID(definitionID, activationID string) string {
	return strings.TrimSpace(definitionID) + ":" + strings.TrimSpace(activationID)
}

func cloneCreatedBySubjectID(subjectID string) string {
	return strings.TrimSpace(subjectID)
}

func cloneSubject(subject *gestalt.Subject) *gestalt.Subject {
	if subject == nil {
		return nil
	}
	return &gestalt.Subject{
		ID:                  strings.TrimSpace(subject.ID),
		CredentialSubjectID: strings.TrimSpace(subject.CredentialSubjectID),
		Email:               strings.TrimSpace(subject.Email),
	}
}

func validateWorkflowRunAs(subject *gestalt.Subject) error {
	if subject == nil || strings.TrimSpace(subject.ID) == "" {
		return errors.New("run_as.subject.id is required")
	}
	return nil
}

func validateWorkflowActivationRunAs(activations []gestalt.WorkflowActivation, runAs *gestalt.Subject) error {
	for _, activation := range activations {
		if activation.Event == nil && activation.Schedule == nil {
			continue
		}
		if err := validateWorkflowRunAs(runAs); err != nil {
			return fmt.Errorf("activation %q run_as: %w", activation.ID, err)
		}
	}
	return nil
}

func firstSubject(primary, fallback *gestalt.Subject) *gestalt.Subject {
	if primary != nil {
		return cloneSubject(primary)
	}
	return cloneSubject(fallback)
}

func workflowTargetInput(target *gestalt.BoundWorkflowTarget) *gestalt.BoundWorkflowTarget {
	return cloneTarget(target)
}

func cloneEvent(event *gestalt.WorkflowEvent) *gestalt.WorkflowEvent {
	return cloneWorkflowEvent(event)
}

func cloneSignal(signal *gestalt.WorkflowSignal) *gestalt.WorkflowSignal {
	return cloneWorkflowSignal(signal)
}

func timePtr(value time.Time) *time.Time {
	ts := value.UTC()
	return &ts
}

func workflowActivationsFromAny(value any) []gestalt.WorkflowActivation {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out []gestalt.WorkflowActivation
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return cloneWorkflowActivations(out)
}

func workflowActivationsFromRecordValue(value any) []gestalt.WorkflowActivation {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return nil
	}
	var out []gestalt.WorkflowActivation
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return cloneWorkflowActivations(out)
}

func cloneWorkflowActivations(values []gestalt.WorkflowActivation) []gestalt.WorkflowActivation {
	if len(values) == 0 {
		return nil
	}
	out := cloneJSONValue(values)
	return out
}

func workflowStepExecutionsFromAny(value any) []gestalt.WorkflowStepExecution {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out []gestalt.WorkflowStepExecution
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return cloneWorkflowStepExecutions(out)
}

func workflowStepExecutionsFromRecordValue(value any) []gestalt.WorkflowStepExecution {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return nil
	}
	var out []gestalt.WorkflowStepExecution
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return cloneWorkflowStepExecutions(out)
}

func cloneWorkflowStepExecutions(values []gestalt.WorkflowStepExecution) []gestalt.WorkflowStepExecution {
	if len(values) == 0 {
		return nil
	}
	return cloneJSONValue(values)
}

func jsonValueString(value any) string {
	if value == nil {
		return ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func anyFromJSONValueString(value any) any {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return nil
	}
	var out any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func targetJSON(target *gestalt.BoundWorkflowTarget) string {
	if target == nil {
		return ""
	}
	data, err := json.Marshal(target)
	if err != nil {
		return ""
	}
	return string(data)
}

func signalJSON(signal *gestalt.WorkflowSignal) string {
	if signal == nil {
		return ""
	}
	data, err := json.Marshal(signal)
	if err != nil {
		return ""
	}
	return string(data)
}

func signalFromRecordValue(raw any) *gestalt.WorkflowSignal {
	value := strings.TrimSpace(stringValue(raw))
	if value == "" {
		return nil
	}
	signal := &gestalt.WorkflowSignal{}
	if err := json.Unmarshal([]byte(value), signal); err == nil {
		if strings.TrimSpace(signal.ID) == "" {
			if data := jsonObject(value); data != nil {
				signal.ID = stringField(data, "id")
				signal.IdempotencyKey = stringField(data, "idempotency_key")
				signal.CreatedBySubjectID = createdByFromAny(data["created_by"])
			}
		}
		return cloneSignal(signal)
	}
	return nil
}

func targetFromRecordValue(recordKind, id string, raw any) (*gestalt.BoundWorkflowTarget, error) {
	if raw == nil {
		return nil, fmt.Errorf("%s %q missing target_json", recordKind, id)
	}
	value := strings.TrimSpace(stringValue(raw))
	if value == "" {
		return nil, fmt.Errorf("%s %q missing target_json", recordKind, id)
	}
	target := &gestalt.BoundWorkflowTarget{}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return nil, fmt.Errorf("%s %q invalid target_json: %w", recordKind, id, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%s %q invalid target_json: trailing data", recordKind, id)
	}
	target = cloneTarget(target)
	if len(target.Steps) == 0 {
		return nil, fmt.Errorf("%s %q target_json must contain steps", recordKind, id)
	}
	if targetOwnerKey(target) == "" {
		return nil, fmt.Errorf("%s %q target_json must contain app.name or agent.provider", recordKind, id)
	}
	return target, nil
}

func createdByToMap(subjectID string) map[string]any {
	subjectID = strings.TrimSpace(subjectID)
	if subjectID == "" {
		return nil
	}
	return map[string]any{"subject_id": subjectID}
}

func createdByFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		return strings.TrimSpace(stringField(typed, "subject_id"))
	default:
		return ""
	}
}

func subjectToMap(subject *gestalt.Subject) map[string]any {
	if subject == nil {
		return nil
	}
	out := map[string]any{
		"id": strings.TrimSpace(subject.ID),
	}
	if credentialSubjectID := strings.TrimSpace(subject.CredentialSubjectID); credentialSubjectID != "" {
		out["credential_subject_id"] = credentialSubjectID
	}
	if email := strings.TrimSpace(subject.Email); email != "" {
		out["email"] = email
	}
	if len(out) == 1 && out["id"] == "" {
		return nil
	}
	return out
}

func subjectFromAny(value any) *gestalt.Subject {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	subjectID := stringField(data, "id")
	if subjectID == "" {
		return nil
	}
	return &gestalt.Subject{
		ID:                  subjectID,
		CredentialSubjectID: stringField(data, "credential_subject_id"),
		Email:               stringField(data, "email"),
	}
}

func eventToMap(event *gestalt.WorkflowEvent) map[string]any {
	if event == nil {
		return nil
	}
	value := map[string]any{
		"id":              event.ID,
		"source":          event.Source,
		"spec_version":    event.SpecVersion,
		"type":            event.Type,
		"subject":         event.Subject,
		"datacontenttype": event.DataContentType,
		"data":            cloneAny(event.Data),
		"extensions":      cloneAnyMap(event.Extensions),
	}
	if !event.Time.IsZero() {
		value["time"] = event.Time.UTC().Format(time.RFC3339Nano)
	}
	return value
}

func eventFromAny(value any) *gestalt.WorkflowEvent {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	input := gestalt.WorkflowEvent{
		ID:              stringField(data, "id"),
		Source:          stringField(data, "source"),
		SpecVersion:     stringField(data, "spec_version"),
		Type:            stringField(data, "type"),
		Subject:         stringField(data, "subject"),
		DataContentType: stringField(data, "datacontenttype"),
		Data:            data["data"],
		Extensions:      anyMap(data["extensions"]),
	}
	if raw := stringField(data, "time"); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			input.Time = parsed.UTC()
		}
	}
	return cloneWorkflowEvent(&input)
}

func anyMap(value any) map[string]any {
	if value == nil {
		return nil
	}
	if out, ok := value.(map[string]any); ok {
		return cloneAnyMap(out)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return out
}

func jsonObject(raw string) map[string]any {
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func cloneAny(value any) any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

func cloneAnyMap(value map[string]any) map[string]any {
	if len(value) == 0 {
		return nil
	}
	cloned := cloneAny(value)
	out, _ := cloned.(map[string]any)
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
		"id":                    r.ID,
		"activation_id":         r.ActivationID,
		"cron":                  r.Cron,
		"timezone":              r.Timezone,
		"target_json":           targetJSON(r.Target),
		"paused":                r.Paused,
		"created_at":            r.CreatedAt.UTC(),
		"updated_at":            r.UpdatedAt.UTC(),
		"created_by":            createdByToMap(r.CreatedBySubjectID),
		"definition_id":         r.DefinitionID,
		"definition_generation": r.DefinitionGeneration,
		"run_as":                subjectToMap(r.RunAs),
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
		ID:                   id,
		ActivationID:         stringField(value, "activation_id"),
		Cron:                 stringField(value, "cron"),
		Timezone:             stringField(value, "timezone"),
		Target:               target,
		Paused:               boolField(value, "paused"),
		CreatedBySubjectID:   createdByFromAny(value["created_by"]),
		DefinitionID:         stringField(value, "definition_id"),
		DefinitionGeneration: intField(value, "definition_generation"),
		RunAs:                subjectFromAny(value["run_as"]),
	}
	if out.ActivationID == "" {
		out.ActivationID = out.ID
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

func (r workflowRunRecord) toRecord() gestalt.Record {
	record := gestalt.Record{
		"id":                       r.ID,
		"provider_name":            strings.TrimSpace(r.ProviderName),
		"status":                   int64(r.Status),
		"target_json":              targetJSON(r.Target),
		"trigger_kind":             r.TriggerKind,
		"trigger_schedule_id":      r.TriggerScheduleID,
		"trigger_event_trigger_id": r.TriggerEventTriggerID,
		"trigger_event":            eventToMap(r.TriggerEvent),
		"created_at":               r.CreatedAt.UTC(),
		"status_message":           r.StatusMessage,
		"output_json":              jsonValueString(r.Output),
		"created_by":               createdByToMap(r.CreatedBySubjectID),
		"definition_id":            r.DefinitionID,
		"definition_generation":    r.DefinitionGeneration,
		"input_json":               jsonValueString(r.Input),
		"current_step_id":          r.CurrentStepID,
		"steps_json":               jsonValueString(r.Steps),
		"run_as":                   subjectToMap(r.RunAs),
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
		ProviderName:          stringField(value, "provider_name"),
		Status:                gestalt.WorkflowRunStatus(intField(value, "status")),
		Target:                target,
		TriggerKind:           stringField(value, "trigger_kind"),
		TriggerScheduleID:     stringField(value, "trigger_schedule_id"),
		TriggerEventTriggerID: stringField(value, "trigger_event_trigger_id"),
		TriggerEvent:          eventFromAny(value["trigger_event"]),
		StatusMessage:         stringField(value, "status_message"),
		Output:                anyFromJSONValueString(value["output_json"]),
		CreatedBySubjectID:    createdByFromAny(value["created_by"]),
		DefinitionID:          stringField(value, "definition_id"),
		DefinitionGeneration:  intField(value, "definition_generation"),
		Input:                 cloneAnyMap(anyMap(anyFromJSONValueString(value["input_json"]))),
		CurrentStepID:         stringField(value, "current_step_id"),
		Steps:                 workflowStepExecutionsFromRecordValue(value["steps_json"]),
		RunAs:                 subjectFromAny(value["run_as"]),
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

func (r workflowRunRecord) toInput() (*gestalt.WorkflowRun, error) {
	return cloneWorkflowRun(&gestalt.WorkflowRun{
		ID:                   r.ID,
		ProviderName:         strings.TrimSpace(r.ProviderName),
		Status:               r.Status,
		Target:               workflowTargetInput(r.Target),
		Trigger:              r.triggerInput(),
		CreatedAt:            r.CreatedAt,
		StartedAt:            r.StartedAt,
		CompletedAt:          r.CompletedAt,
		StatusMessage:        r.StatusMessage,
		Output:               cloneAny(r.Output),
		CreatedBySubjectID:   cloneCreatedBySubjectID(r.CreatedBySubjectID),
		RunAs:                cloneSubject(r.RunAs),
		WorkflowKey:          r.WorkflowKey,
		DefinitionID:         r.DefinitionID,
		Input:                cloneAnyMap(r.Input),
		DefinitionGeneration: r.DefinitionGeneration,
		CurrentStepID:        r.CurrentStepID,
		Steps:                cloneWorkflowStepExecutions(r.Steps),
	}), nil
}

func (r workflowRunRecord) triggerInput() *gestalt.WorkflowRunTrigger {
	switch r.TriggerKind {
	case triggerKindSchedule:
		var scheduledFor *time.Time
		if r.TriggerScheduledFor != nil {
			scheduledFor = timePtr(r.TriggerScheduledFor.UTC())
		}
		return &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
			ActivationID: r.TriggerScheduleID,
			ScheduledFor: scheduledFor,
		}}
	case triggerKindEvent:
		var event *gestalt.WorkflowEvent
		if r.TriggerEvent != nil {
			event = cloneWorkflowEvent(r.TriggerEvent)
		}
		return &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			ActivationID: r.TriggerEventTriggerID,
			Event:        event,
		}}
	default:
		return &gestalt.WorkflowRunTrigger{Manual: true}
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
		"signal_json":     signalJSON(r.signalInput()),
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

func (r workflowSignalRecord) signalInput() *gestalt.WorkflowSignal {
	input := cloneWorkflowSignal(r.Signal)
	if input == nil {
		input = &gestalt.WorkflowSignal{}
	}
	if input.ID == "" {
		input.ID = r.ID
	}
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = r.IdempotencyKey
	}
	if input.Sequence == 0 {
		input.Sequence = r.Sequence
	}
	if input.CreatedAt.IsZero() && !r.CreatedAt.IsZero() {
		input.CreatedAt = r.CreatedAt
	}
	return input
}

func cloneWorkflowTarget(target *gestalt.BoundWorkflowTarget) *gestalt.BoundWorkflowTarget {
	if target == nil {
		return nil
	}
	out := cloneJSONValue(*target)
	return &out
}

func cloneWorkflowRun(run *gestalt.WorkflowRun) *gestalt.WorkflowRun {
	if run == nil {
		return nil
	}
	out := cloneJSONValue(*run)
	return &out
}

func cloneWorkflowEvent(event *gestalt.WorkflowEvent) *gestalt.WorkflowEvent {
	if event == nil {
		return nil
	}
	out := cloneJSONValue(*event)
	return &out
}

func cloneWorkflowSignal(signal *gestalt.WorkflowSignal) *gestalt.WorkflowSignal {
	if signal == nil {
		return nil
	}
	out := cloneJSONValue(*signal)
	return &out
}

func cloneJSONValue[T any](value T) T {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

func cloneTarget(target *gestalt.BoundWorkflowTarget) *gestalt.BoundWorkflowTarget {
	return cloneWorkflowTarget(target)
}

func indexQueryKey(values ...any) any {
	if len(values) == 0 {
		return nil
	}
	if len(values) == 1 {
		return values[0]
	}
	return values
}

var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Starter = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
