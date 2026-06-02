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

	storeSchedules     = "schedules"
	storeEventTriggers = "event_triggers"
	storeDefinitions   = "definitions"
	storeRuns          = "runs"
	storeRunClaims     = "workflow_run_claims"
	storeIdempotency   = "idempotency"
	storeWorkflowKeys  = "workflow_keys"
	storeSignals       = "workflow_signals"

	indexByEventTriggerMatch = "by_match"

	triggerKindManual   = "manual"
	triggerKindSchedule = "schedule"
	triggerKindEvent    = "event"

	gestaltInputKey                    = "_gestalt"
	configManagedWorkflowSubject       = "system:config"
	configManagedWorkflowAuth          = "config"
	configManagedWorkflowKind          = "system"
	workflowMetadataKey                = "workflow"
	workflowInvokeMetadataWorkflowKey  = "workflow_key"
	workflowInvokeMetadataDefinitionID = "definition_id"
	dispatchPriorityMetadataKey        = "dispatchPriority"
	staleRunStatusMessage              = "workflow provider restarted while run was in progress"

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
	// publishMu serializes event publication so deterministic event run IDs stay
	// consistent across duplicate publishes.
	publishMu sync.Mutex

	name              string
	cfg               config
	db                indexeddb.Database
	stepExecutor      gestaltworkflow.StepExecutor
	scheduleStore     indexeddb.ObjectStore
	eventTriggerStore indexeddb.ObjectStore
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
	ID           string
	Cron         string
	Timezone     string
	Target       *gestalt.BoundWorkflowTarget
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	NextRunAt    *time.Time
	CreatedBy    *gestalt.WorkflowActor
	DefinitionID string
	RunAs        *gestalt.Subject
}

type workflowEventTriggerRecord struct {
	ID           string
	MatchType    string
	MatchSource  string
	MatchSubject string
	Target       *gestalt.BoundWorkflowTarget
	Paused       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CreatedBy    *gestalt.WorkflowActor
	DefinitionID string
	RunAs        *gestalt.Subject
}

type workflowRunRecord struct {
	ID                    string
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
	ResultBody            string
	CreatedBy             *gestalt.WorkflowActor
	DefinitionID          string
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

	runStore := db.ObjectStore(storeRuns)
	runClaimStore := db.ObjectStore(storeRunClaims)
	workflowKeyStore := db.ObjectStore(storeWorkflowKeys)
	signalStore := db.ObjectStore(storeSignals)
	eventTriggerStore := db.ObjectStore(storeEventTriggers)
	if err := validateWorkflowEventTriggerIndexes(ctx, eventTriggerStore); err != nil {
		cleanup()
		return fmt.Errorf("indexeddb workflow: validate event trigger indexes: %w", err)
	}
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
	p.eventTriggerStore = eventTriggerStore
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
	p.eventTriggerStore = nil
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

func (p *Provider) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	actor := cloneActor(req.CreatedBy)
	key := strings.TrimSpace(req.IdempotencyKey)
	workflowKey := strings.TrimSpace(req.WorkflowKey)

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
		runID = idempotentManualRunID(target.OwnerKey, key)
		run, found, err := loadRunRecord(ctx, state.runStore, target.OwnerKey, runID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotent run by deterministic id: %v", err)
		}
		if found {
			_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key, run.ID, now)
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
		ID:                 runID,
		Status:             gestalt.WorkflowRunStatusValuePending,
		Target:             cloneTarget(target.Target),
		TriggerKind:        triggerKindManual,
		CreatedAt:          now,
		CreatedBy:          actor,
		DefinitionID:       strings.TrimSpace(req.DefinitionID),
		RunAs:              cloneSubject(req.RunAs),
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
		_ = storeIdempotencyRecord(ctx, state.idempotencyStore, target.OwnerKey, key, run.ID, now)
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

func (p *Provider) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
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
	resp := &gestalt.ListWorkflowProviderRunsResponse{Runs: make([]gestalt.BoundWorkflowRun, 0, len(runs))}
	for _, run := range runs {
		pbRun, err := run.toInput()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build run response: %v", err)
		}
		resp.Runs = append(resp.Runs, *pbRun)
	}
	return resp, nil
}

func (p *Provider) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
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
	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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

func (p *Provider) UpsertSchedule(ctx context.Context, req *gestalt.UpsertWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	scheduleID := strings.TrimSpace(req.ScheduleID)
	if scheduleID == "" {
		scheduleID = uuid.NewString()
	}
	cronSpec := strings.TrimSpace(req.Cron)
	if cronSpec == "" {
		return nil, status.Error(codes.InvalidArgument, "cron is required")
	}
	location, timezone, err := parseTimezone(req.Timezone)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	parser := cronParser()
	if _, err := parser.Parse(cronSpec); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid cron: %v", err)
	}
	requestedBy := cloneActor(req.RequestedBy)

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
		Paused:       req.Paused,
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
		DefinitionID: strings.TrimSpace(req.DefinitionID),
		RunAs:        cloneSubject(req.RunAs),
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
	resp, err := record.toInput()
	p.signalWorkerLocked("")
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetSchedule(ctx context.Context, req *gestalt.GetWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := ""
	scheduleID := strings.TrimSpace(req.ScheduleID)
	if scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadScheduleRecord(ctx, state.scheduleStore, appName, scheduleID)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get schedule: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	resp, err := record.toInput()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListSchedules(ctx context.Context, req *gestalt.ListWorkflowProviderSchedulesRequest) (*gestalt.ListWorkflowProviderSchedulesResponse, error) {
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
	records, err := listScheduleRecords(ctx, state.scheduleStore, appName)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list schedules: %v", err)
	}
	resp := &gestalt.ListWorkflowProviderSchedulesResponse{Schedules: make([]gestalt.BoundWorkflowSchedule, 0, len(records))}
	for _, record := range records {
		pbSchedule, err := record.toInput()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
		}
		resp.Schedules = append(resp.Schedules, *pbSchedule)
	}
	return resp, nil
}

func (p *Provider) DeleteSchedule(ctx context.Context, req *gestalt.DeleteWorkflowProviderScheduleRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	appName := ""
	scheduleID := strings.TrimSpace(req.ScheduleID)
	if scheduleID == "" {
		return status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	_, found, err := loadScheduleRecord(ctx, state.scheduleStore, appName, scheduleID)
	if err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "load schedule: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return status.Errorf(codes.NotFound, "workflow schedule %q not found", scheduleID)
	}
	if err := state.scheduleStore.Delete(ctx, scheduleID); err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "delete schedule: %v", err)
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) PauseSchedule(ctx context.Context, req *gestalt.PauseWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, "", strings.TrimSpace(req.ScheduleID), true)
}

func (p *Provider) ResumeSchedule(ctx context.Context, req *gestalt.ResumeWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateSchedulePaused(ctx, "", strings.TrimSpace(req.ScheduleID), false)
}

func (p *Provider) UpsertEventTrigger(ctx context.Context, req *gestalt.UpsertWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	if triggerID == "" {
		triggerID = uuid.NewString()
	}
	matchType := strings.TrimSpace(req.Match.Type)
	if matchType == "" {
		return nil, status.Error(codes.InvalidArgument, "match.type is required")
	}
	matchSource := strings.TrimSpace(req.Match.Source)
	matchSubject := strings.TrimSpace(req.Match.Subject)
	requestedBy := cloneActor(req.RequestedBy)

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
		Paused:       req.Paused,
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    requestedBy,
		DefinitionID: strings.TrimSpace(req.DefinitionID),
		RunAs:        cloneSubject(req.RunAs),
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		record.CreatedBy = createdByForUpsert(existing.CreatedBy, requestedBy)
	}
	if err := state.eventTriggerStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "upsert event trigger: %v", err)
	}
	resp, err := record.toInput()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
}

func (p *Provider) GetEventTrigger(ctx context.Context, req *gestalt.GetWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	if triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, "", triggerID)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get event trigger: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	resp, err := record.toInput()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
}

func (p *Provider) ListEventTriggers(ctx context.Context, req *gestalt.ListWorkflowProviderEventTriggersRequest) (*gestalt.ListWorkflowProviderEventTriggersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listEventTriggerRecords(ctx, state.eventTriggerStore)
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list event triggers: %v", err)
	}
	resp := &gestalt.ListWorkflowProviderEventTriggersResponse{Triggers: make([]gestalt.BoundWorkflowEventTrigger, 0, len(records))}
	for _, record := range records {
		pbTrigger, err := record.toInput()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
		}
		resp.Triggers = append(resp.Triggers, *pbTrigger)
	}
	return resp, nil
}

func (p *Provider) DeleteEventTrigger(ctx context.Context, req *gestalt.DeleteWorkflowProviderEventTriggerRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	if triggerID == "" {
		return status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	_, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, "", triggerID)
	if err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "load event trigger: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	if err := state.eventTriggerStore.Delete(ctx, triggerID); err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "delete event trigger: %v", err)
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) PauseEventTrigger(ctx context.Context, req *gestalt.PauseWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, "", strings.TrimSpace(req.TriggerID), true)
}

func (p *Provider) ResumeEventTrigger(ctx context.Context, req *gestalt.ResumeWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	return p.updateEventTriggerPaused(ctx, "", strings.TrimSpace(req.TriggerID), false)
}

func (p *Provider) PublishEvent(ctx context.Context, req *gestalt.PublishWorkflowProviderEventRequest) (*gestalt.WorkflowEvent, error) {
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

	p.publishMu.Lock()
	defer p.publishMu.Unlock()

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	triggers, err := listMatchingEventTriggerRecords(ctx, state.eventTriggerStore, event)
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "list matching event triggers: %v", err)
	}
	now := p.clock().UTC()
	publishedBy := cloneActor(req.PublishedBy)
	enqueued := false
	preferredRunID := ""
	for _, trigger := range triggers {
		runID := uuid.NewString()
		if strings.TrimSpace(event.ID) != "" {
			runID = eventRunID(trigger.ID, event.Source, event.ID)
		}
		if _, found, err := loadRunRecord(ctx, state.runStore, trigger.ownerKey(), runID); err != nil {
			p.mu.RUnlock()
			return nil, status.Errorf(codes.Internal, "load event run: %v", err)
		} else if found {
			continue
		}
		createdBy := cloneActor(trigger.CreatedBy)
		if actorHasSubject(publishedBy) {
			createdBy = cloneActor(publishedBy)
		}
		run := workflowRunRecord{
			ID:                    runID,
			Status:                gestalt.WorkflowRunStatusValuePending,
			Target:                cloneTarget(trigger.Target),
			TriggerKind:           triggerKindEvent,
			TriggerEventTriggerID: trigger.ID,
			TriggerEvent:          cloneEvent(event),
			CreatedAt:             now,
			CreatedBy:             createdBy,
			DefinitionID:          trigger.DefinitionID,
			RunAs:                 cloneSubject(trigger.RunAs),
			NextSignalSequence:    1,
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

func signalOrStartRunInTransaction(ctx context.Context, stores workflowSignalOrStartTxStores, target scopedTarget, req *gestalt.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *gestalt.WorkflowSignal, now time.Time, runClaimTTL time.Duration) (*gestalt.SignalWorkflowRunResponse, string, error) {
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
			ID:                 uuid.NewString(),
			Status:             gestalt.WorkflowRunStatusValuePending,
			Target:             cloneTarget(target.Target),
			TriggerKind:        triggerKindManual,
			CreatedAt:          now,
			CreatedBy:          cloneActor(req.CreatedBy),
			DefinitionID:       strings.TrimSpace(req.DefinitionID),
			RunAs:              cloneSubject(req.RunAs),
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

func (p *Provider) updateSchedulePaused(ctx context.Context, appName, scheduleID string, paused bool) (*gestalt.BoundWorkflowSchedule, error) {
	if scheduleID == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadScheduleRecord(ctx, state.scheduleStore, appName, scheduleID)
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
	resp, err := record.toInput()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build schedule response: %v", err)
	}
	return resp, nil
}

func (p *Provider) updateEventTriggerPaused(ctx context.Context, appName, triggerID string, paused bool) (*gestalt.BoundWorkflowEventTrigger, error) {
	if triggerID == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadEventTriggerRecord(ctx, state.eventTriggerStore, appName, triggerID)
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
	resp, err := record.toInput()
	p.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build event trigger response: %v", err)
	}
	return resp, nil
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
			Status:              gestalt.WorkflowRunStatusValuePending,
			Target:              cloneTarget(schedule.Target),
			TriggerKind:         triggerKindSchedule,
			TriggerScheduleID:   schedule.ID,
			TriggerScheduledFor: timePtr(latestDue),
			CreatedAt:           now,
			CreatedBy:           cloneActor(schedule.CreatedBy),
			DefinitionID:        schedule.DefinitionID,
			RunAs:               cloneSubject(schedule.RunAs),
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
	if pending.Status != gestalt.WorkflowRunStatusValueRunning {
		pending.Status = gestalt.WorkflowRunStatusValueRunning
		pending.StartedAt = &now
		pending.CompletedAt = nil
		pending.StatusMessage = ""
		if err := state.runStore.Put(ctx, pending.toRecord()); err != nil {
			err = releaseUninvokedClaim(err)
			releaseWorker()
			return false, err
		}
	}
	executor := state.stepExecutor
	providerName := state.providerName
	releaseWorker()

	var targetInput *gestalt.BoundWorkflowTarget
	if pending.Target != nil {
		targetInput = cloneTarget(pending.Target)
	}
	signals := signalInputs(claimedSignals)
	stopRenewingClaim := p.startRunClaimRenewal(ctx, pending.ID, claimOwnerID, runClaimRenewEvery)
	resp, invokeErr := executor.Execute(ctx, gestaltworkflow.Request{
		ProviderName: providerName,
		RunID:        pending.ID,
		Target:       targetInput,
		Trigger:      pending.triggerInput(),
		Metadata:     workflowInvokeMetadataInput(pending.WorkflowKey, pending.DefinitionID),
		CreatedBy:    workflowActorInput(pending.CreatedBy),
		RunAs:        cloneSubject(pending.RunAs),
		Signals:      signals,
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

func (p *Provider) completeRunAfterInvoke(ctx context.Context, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, resp *gestaltworkflow.Response, invokeErr error) error {
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

func completeRunInTransaction(ctx context.Context, stores workflowRunCompletionTxStores, pending workflowRunRecord, claimedSignals []workflowSignalRecord, claimOwnerID string, resp *gestaltworkflow.Response, invokeErr error, completedAt time.Time) error {
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
	current.CompletedAt = &completedAt
	if invokeErr != nil {
		current.Status = gestalt.WorkflowRunStatusValueFailed
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
		if resp == nil {
			resp = &gestaltworkflow.Response{}
		}
		current.ResultBody = resp.Body
		if resp.Status >= http.StatusBadRequest {
			current.Status = gestalt.WorkflowRunStatusValueFailed
			current.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.Status)
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
				current.Status = gestalt.WorkflowRunStatusValuePending
				current.CompletedAt = nil
			} else {
				current.Status = gestalt.WorkflowRunStatusValueSucceeded
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
	if p.db == nil || p.runStore == nil || p.runClaimStore == nil || p.scheduleStore == nil || p.eventTriggerStore == nil || p.definitionStore == nil || p.idempotencyStore == nil || p.workflowKeyStore == nil || p.signalStore == nil || p.stepExecutor == nil {
		return nil, errors.New("indexeddb workflow: provider is not configured")
	}
	return &configuredState{
		providerName:       strings.TrimSpace(p.name),
		db:                 p.db,
		stepExecutor:       p.stepExecutor,
		scheduleStore:      p.scheduleStore,
		eventTriggerStore:  p.eventTriggerStore,
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
	eventTriggerStore  indexeddb.ObjectStore
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
		if _, err := store.Index(probe.name).Count(ctx, nil, probe.values...); err != nil {
			return fmt.Errorf("%s: %w", probe.name, err)
		}
	}
	return nil
}

func validateWorkflowEventTriggerIndexes(ctx context.Context, store indexeddb.ObjectStore) error {
	if _, err := store.Index(indexByEventTriggerMatch).Count(ctx, nil, "__workflow_schema_probe__", "", ""); err != nil {
		return fmt.Errorf("%s: %w", indexByEventTriggerMatch, err)
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

func workflowInvokeMetadataInput(workflowKey, definitionID string) map[string]any {
	workflowKey = strings.TrimSpace(workflowKey)
	definitionID = strings.TrimSpace(definitionID)
	if workflowKey == "" && definitionID == "" {
		return nil
	}
	metadata := map[string]any{}
	if workflowKey != "" {
		metadata[workflowInvokeMetadataWorkflowKey] = workflowKey
	}
	if definitionID != "" {
		metadata[workflowInvokeMetadataDefinitionID] = definitionID
	}
	return metadata
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
		ID:             strings.TrimSpace(signal.ID),
		Name:           name,
		Payload:        cloneAny(signal.Payload),
		Metadata:       cloneAny(signal.Metadata),
		CreatedBy:      workflowActorInput(signal.CreatedBy),
		CreatedAt:      createdAt,
		IdempotencyKey: strings.TrimSpace(signal.IdempotencyKey),
		Sequence:       signal.Sequence,
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

func hasPendingSignals(ctx context.Context, store indexeddb.ObjectStore, runID string) (bool, error) {
	return hasSignalsInState(ctx, store, runID, signalStatePending)
}

func hasPendingSignalsTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID string) (bool, error) {
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

func hasSignalsInState(ctx context.Context, store indexeddb.ObjectStore, runID, state string) (bool, error) {
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

func listSignalRecordsTx(ctx context.Context, store indexeddb.TransactionObjectStore, runID, state string) ([]workflowSignalRecord, error) {
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

func listSignalRecordsLimit(ctx context.Context, store indexeddb.ObjectStore, runID, state string, limit int) ([]workflowSignalRecord, error) {
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

func loadEventTriggerRecord(ctx context.Context, store indexeddb.ObjectStore, ownerKey, triggerID string) (workflowEventTriggerRecord, bool, error) {
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

func listEventTriggerRecords(ctx context.Context, store indexeddb.ObjectStore) ([]workflowEventTriggerRecord, error) {
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

func listMatchingEventTriggerRecords(ctx context.Context, store indexeddb.ObjectStore, event *gestalt.WorkflowEvent) ([]workflowEventTriggerRecord, error) {
	seen := map[string]struct{}{}
	out := make([]workflowEventTriggerRecord, 0)
	for _, values := range eventLookupValues(event) {
		records, err := store.Index(indexByEventTriggerMatch).GetAll(ctx, nil, values...)
		if errors.Is(err, gestalt.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			trigger, err := eventTriggerRecordFromRecord(record)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[trigger.ID]; ok {
				continue
			}
			if trigger.Paused {
				continue
			}
			seen[trigger.ID] = struct{}{}
			out = append(out, trigger)
		}
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

func eventLookupValues(event *gestalt.WorkflowEvent) [][]any {
	if event == nil {
		return nil
	}
	typ := strings.TrimSpace(event.Type)
	source := strings.TrimSpace(event.Source)
	subject := strings.TrimSpace(event.Subject)
	if typ == "" {
		return nil
	}
	values := [][]any{
		{typ, "", ""},
	}
	if source != "" {
		values = append(values, []any{typ, source, ""})
	}
	if subject != "" {
		values = append(values, []any{typ, "", subject})
		if source != "" {
			values = append(values, []any{typ, source, subject})
		}
	}
	return values
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

func eventRunID(triggerID, eventSource, eventID string) string {
	return hashScopedID("event", triggerID, eventSource, eventID)
}

func actorHasSubject(actor *gestalt.WorkflowActor) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.SubjectID) != ""
}

func createdByForUpsert(existing, requested *gestalt.WorkflowActor) *gestalt.WorkflowActor {
	if isConfigManagedActor(requested) {
		return cloneActor(requested)
	}
	return cloneActor(existing)
}

func isConfigManagedActor(actor *gestalt.WorkflowActor) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.SubjectID) == configManagedWorkflowSubject &&
		strings.TrimSpace(actor.SubjectKind) == configManagedWorkflowKind &&
		strings.TrimSpace(actor.AuthSource) == configManagedWorkflowAuth
}

func scheduleRunID(scheduleID string, scheduledFor time.Time) string {
	return "schedule:" + scheduleID + ":" + scheduledFor.UTC().Format(time.RFC3339Nano)
}

func cloneActor(actor *gestalt.WorkflowActor) *gestalt.WorkflowActor {
	if actor == nil {
		return nil
	}
	return &gestalt.WorkflowActor{
		SubjectID:   actor.SubjectID,
		SubjectKind: actor.SubjectKind,
		DisplayName: actor.DisplayName,
		AuthSource:  actor.AuthSource,
	}
}

func cloneSubject(subject *gestalt.Subject) *gestalt.Subject {
	if subject == nil {
		return nil
	}
	return &gestalt.Subject{
		ID:                  subject.ID,
		Kind:                subject.Kind,
		CredentialSubjectID: subject.CredentialSubjectID,
		DisplayName:         subject.DisplayName,
		AuthSource:          subject.AuthSource,
		Email:               subject.Email,
	}
}

func workflowActorInput(actor *gestalt.WorkflowActor) *gestalt.WorkflowActor {
	return cloneActor(actor)
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

func appTargetInput(target *gestalt.BoundWorkflowTarget) map[string]any {
	app := firstWorkflowAppStep(target)
	if app == nil {
		return nil
	}
	if app.Input.Object != nil {
		out := make(map[string]any, len(app.Input.Object))
		for key, value := range app.Input.Object {
			out[key] = workflowValueToAny(value)
		}
		return out
	}
	if app.Input.LiteralSet {
		return anyMap(app.Input.Literal)
	}
	return nil
}

func workflowValueToAny(value gestalt.WorkflowValue) any {
	switch {
	case value.LiteralSet:
		return value.Literal
	case value.Object != nil:
		out := make(map[string]any, len(value.Object))
		for key, nested := range value.Object {
			out[key] = workflowValueToAny(nested)
		}
		return out
	case value.Array != nil:
		out := make([]any, 0, len(value.Array))
		for _, nested := range value.Array {
			out = append(out, workflowValueToAny(nested))
		}
		return out
	default:
		return nil
	}
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
				signal.CreatedBy = actorFromAny(data["created_by"])
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

func actorToMap(actor *gestalt.WorkflowActor) map[string]any {
	if actor == nil {
		return nil
	}
	return map[string]any{
		"subject_id":   actor.SubjectID,
		"subject_kind": actor.SubjectKind,
		"display_name": actor.DisplayName,
		"auth_source":  actor.AuthSource,
	}
}

func actorFromAny(value any) *gestalt.WorkflowActor {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	return &gestalt.WorkflowActor{
		SubjectID:   stringField(data, "subject_id"),
		SubjectKind: stringField(data, "subject_kind"),
		DisplayName: stringField(data, "display_name"),
		AuthSource:  stringField(data, "auth_source"),
	}
}

func subjectToMap(subject *gestalt.Subject) map[string]any {
	if subject == nil {
		return nil
	}
	return map[string]any{
		"id":                    subject.ID,
		"kind":                  subject.Kind,
		"credential_subject_id": subject.CredentialSubjectID,
		"display_name":          subject.DisplayName,
		"auth_source":           subject.AuthSource,
		"email":                 subject.Email,
	}
}

func subjectFromAny(value any) *gestalt.Subject {
	data, ok := value.(map[string]any)
	if !ok || len(data) == 0 {
		return nil
	}
	return &gestalt.Subject{
		ID:                  stringField(data, "id"),
		Kind:                stringField(data, "kind"),
		CredentialSubjectID: stringField(data, "credential_subject_id"),
		DisplayName:         stringField(data, "display_name"),
		AuthSource:          stringField(data, "auth_source"),
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
		"id":            r.ID,
		"cron":          r.Cron,
		"timezone":      r.Timezone,
		"target_json":   targetJSON(r.Target),
		"paused":        r.Paused,
		"created_at":    r.CreatedAt.UTC(),
		"updated_at":    r.UpdatedAt.UTC(),
		"created_by":    actorToMap(r.CreatedBy),
		"definition_id": r.DefinitionID,
		"run_as":        subjectToMap(r.RunAs),
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
		DefinitionID: stringField(value, "definition_id"),
		RunAs:        subjectFromAny(value["run_as"]),
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

func (r workflowScheduleRecord) toInput() (*gestalt.BoundWorkflowSchedule, error) {
	return cloneWorkflowSchedule(&gestalt.BoundWorkflowSchedule{
		ID:           r.ID,
		Cron:         r.Cron,
		Timezone:     r.Timezone,
		Target:       workflowTargetInput(r.Target),
		Paused:       r.Paused,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
		NextRunAt:    r.NextRunAt,
		CreatedBy:    workflowActorInput(r.CreatedBy),
		RunAs:        cloneSubject(r.RunAs),
		DefinitionID: r.DefinitionID,
	}), nil
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
		"definition_id": r.DefinitionID,
		"run_as":        subjectToMap(r.RunAs),
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
		DefinitionID: stringField(value, "definition_id"),
		RunAs:        subjectFromAny(value["run_as"]),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := timeField(value, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	return out, nil
}

func (r workflowEventTriggerRecord) toInput() (*gestalt.BoundWorkflowEventTrigger, error) {
	return cloneWorkflowEventTrigger(&gestalt.BoundWorkflowEventTrigger{
		ID: r.ID,
		Match: &gestalt.WorkflowEventMatch{
			Type:    r.MatchType,
			Source:  r.MatchSource,
			Subject: r.MatchSubject,
		},
		Target:       workflowTargetInput(r.Target),
		Paused:       r.Paused,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
		CreatedBy:    workflowActorInput(r.CreatedBy),
		RunAs:        cloneSubject(r.RunAs),
		DefinitionID: r.DefinitionID,
	}), nil
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
		"definition_id":            r.DefinitionID,
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
		Status:                gestalt.WorkflowRunStatus(intField(value, "status")),
		Target:                target,
		TriggerKind:           stringField(value, "trigger_kind"),
		TriggerScheduleID:     stringField(value, "trigger_schedule_id"),
		TriggerEventTriggerID: stringField(value, "trigger_event_trigger_id"),
		TriggerEvent:          eventFromAny(value["trigger_event"]),
		StatusMessage:         stringField(value, "status_message"),
		ResultBody:            stringField(value, "result_body"),
		CreatedBy:             actorFromAny(value["created_by"]),
		DefinitionID:          stringField(value, "definition_id"),
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

func (r workflowRunRecord) toInput() (*gestalt.BoundWorkflowRun, error) {
	return cloneWorkflowRun(&gestalt.BoundWorkflowRun{
		ID:            r.ID,
		Status:        r.Status,
		Target:        workflowTargetInput(r.Target),
		Trigger:       r.triggerInput(),
		CreatedAt:     r.CreatedAt,
		StartedAt:     r.StartedAt,
		CompletedAt:   r.CompletedAt,
		StatusMessage: r.StatusMessage,
		ResultBody:    r.ResultBody,
		CreatedBy:     workflowActorInput(r.CreatedBy),
		RunAs:         cloneSubject(r.RunAs),
		WorkflowKey:   r.WorkflowKey,
		DefinitionID:  r.DefinitionID,
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
			ScheduleID:   r.TriggerScheduleID,
			ScheduledFor: scheduledFor,
		}}
	case triggerKindEvent:
		var event *gestalt.WorkflowEvent
		if r.TriggerEvent != nil {
			event = cloneWorkflowEvent(r.TriggerEvent)
		}
		return &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			TriggerID: r.TriggerEventTriggerID,
			Event:     event,
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

func cloneWorkflowSchedule(schedule *gestalt.BoundWorkflowSchedule) *gestalt.BoundWorkflowSchedule {
	if schedule == nil {
		return nil
	}
	out := cloneJSONValue(*schedule)
	return &out
}

func cloneWorkflowEventTrigger(trigger *gestalt.BoundWorkflowEventTrigger) *gestalt.BoundWorkflowEventTrigger {
	if trigger == nil {
		return nil
	}
	out := cloneJSONValue(*trigger)
	return &out
}

func cloneWorkflowRun(run *gestalt.BoundWorkflowRun) *gestalt.BoundWorkflowRun {
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

var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Starter = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
