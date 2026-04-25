package externalcredentials

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const providerVersion = "0.0.1-alpha.2"

type Provider struct {
	mu             sync.RWMutex
	cfg            config
	store          *store
	backfillCancel context.CancelFunc
	backfillDone   chan struct{}
	now            func() time.Time
}

func New() *Provider {
	return &Provider{now: time.Now}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("default external credentials: %w", err)
	}

	st, err := openStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("default external credentials: %w", err)
	}

	backfillCtx, backfillCancel := context.WithCancel(context.Background())
	backfillDone := startLegacyBackfill(backfillCtx, st)

	p.mu.Lock()
	oldStore := p.store
	oldBackfillCancel := p.backfillCancel
	oldBackfillDone := p.backfillDone
	p.cfg = cfg
	p.store = st
	p.backfillCancel = backfillCancel
	p.backfillDone = backfillDone
	p.mu.Unlock()

	stopLegacyBackfill(oldBackfillCancel, oldBackfillDone)
	if oldStore != nil {
		_ = oldStore.Close()
	}
	return nil
}

func startLegacyBackfill(ctx context.Context, st *store) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		started := time.Now()
		if err := st.backfillLegacyCredentials(ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.WarnContext(ctx, "legacy external credentials backfill failed", "error", err, "duration", time.Since(started).String())
			}
			return
		}
		slog.InfoContext(ctx, "legacy external credentials backfill finished", "duration", time.Since(started).String())
	}()
	return done
}

func stopLegacyBackfill(cancel context.CancelFunc, done <-chan struct{}) {
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindExternalCredential,
		Name:        "default",
		DisplayName: "Default External Credentials",
		Description: "External credentials provider backed by the host IndexedDB service.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	st, err := p.configuredStore()
	if err != nil {
		return err
	}
	_, err = st.credentials.Count(ctx, nil)
	return err
}

func (p *Provider) Close() error {
	p.mu.Lock()
	st := p.store
	cancel := p.backfillCancel
	done := p.backfillDone
	p.store = nil
	p.backfillCancel = nil
	p.backfillDone = nil
	p.mu.Unlock()

	stopLegacyBackfill(cancel, done)
	if st == nil {
		return nil
	}
	return st.Close()
}

func (p *Provider) UpsertCredential(ctx context.Context, req *gestalt.UpsertExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	if req == nil || req.GetCredential() == nil {
		return nil, status.Error(codes.InvalidArgument, "credential is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	return st.upsertCredential(ctx, req.GetCredential(), req.GetPreserveTimestamps(), p.now().UTC())
}

func (p *Provider) GetCredential(ctx context.Context, req *gestalt.GetExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	if req == nil || req.GetLookup() == nil {
		return nil, status.Error(codes.InvalidArgument, "lookup is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}

	lookup := req.GetLookup()
	return st.getCredential(
		ctx,
		strings.TrimSpace(lookup.GetSubjectId()),
		strings.TrimSpace(lookup.GetIntegration()),
		strings.TrimSpace(lookup.GetConnection()),
		strings.TrimSpace(lookup.GetInstance()),
	)
}

func (p *Provider) ListCredentials(ctx context.Context, req *gestalt.ListExternalCredentialsRequest) (*gestalt.ListExternalCredentialsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	subjectID := strings.TrimSpace(req.GetSubjectId())
	if subjectID == "" {
		return nil, status.Error(codes.InvalidArgument, "subject_id is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}

	credentials, err := st.listCredentials(
		ctx,
		subjectID,
		strings.TrimSpace(req.GetIntegration()),
		strings.TrimSpace(req.GetConnection()),
		strings.TrimSpace(req.GetInstance()),
	)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListExternalCredentialsResponse{Credentials: credentials}, nil
}

func (p *Provider) DeleteCredential(ctx context.Context, req *gestalt.DeleteExternalCredentialRequest) error {
	if req == nil || strings.TrimSpace(req.GetId()) == "" {
		return status.Error(codes.InvalidArgument, "credential id is required")
	}

	st, err := p.configuredStore()
	if err != nil {
		return err
	}
	return st.deleteCredential(ctx, strings.TrimSpace(req.GetId()))
}

func (p *Provider) configuredStore() (*store, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "default external credentials: provider is not configured")
	}
	return p.store, nil
}
