package externalcredentials

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const providerVersion = "0.0.1-alpha.11"

type Provider struct {
	mu            sync.RWMutex
	cfg           config
	store         *store
	refreshCancel context.CancelFunc
	refreshDone   <-chan struct{}
	now           func() time.Time
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

	p.mu.Lock()
	oldCancel := p.refreshCancel
	oldDone := p.refreshDone
	oldStore := p.store
	p.refreshCancel = nil
	p.refreshDone = nil
	p.mu.Unlock()

	waitCredentialRefreshLoop(oldCancel, oldDone)

	p.mu.Lock()
	p.cfg = cfg
	p.store = st
	if len(cfg.RefreshTargets) > 0 {
		refreshCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		p.refreshCancel = cancel
		p.refreshDone = done
		go p.credentialRefreshLoop(refreshCtx, st, cfg.RefreshTargets, done)
	}
	p.mu.Unlock()

	if oldStore != nil {
		_ = oldStore.Close()
	}
	return nil
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
	if st.requireTenant {
		return nil
	}
	_, err = st.credentials.Count(ctx, nil)
	return err
}

func (p *Provider) Close() error {
	p.mu.Lock()
	cancel := p.refreshCancel
	done := p.refreshDone
	p.refreshCancel = nil
	p.refreshDone = nil
	st := p.store
	p.store = nil
	p.mu.Unlock()

	waitCredentialRefreshLoop(cancel, done)

	if st == nil {
		return nil
	}
	return st.Close()
}

func waitCredentialRefreshLoop(cancel context.CancelFunc, done <-chan struct{}) {
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (p *Provider) credentialRefreshLoop(ctx context.Context, st *store, targets []credentialRefreshTarget, done chan<- struct{}) {
	defer close(done)
	interval := minCredentialRefreshInterval(targets)
	if interval <= 0 {
		return
	}
	p.logCredentialRefreshStats(p.runCredentialRefreshOnceWith(ctx, st, targets))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.logCredentialRefreshStats(p.runCredentialRefreshOnceWith(ctx, st, targets))
		}
	}
}

func (p *Provider) logCredentialRefreshStats(stats credentialRefreshStats) {
	if stats.Errors > 0 {
		slog.Warn("external credential refresh completed with errors", "checked", stats.Checked, "refreshed", stats.Refreshed, "deleted", stats.Deleted, "errors", stats.Errors)
	} else if stats.Checked > 0 || stats.Refreshed > 0 || stats.Deleted > 0 {
		slog.Info("external credential refresh completed", "checked", stats.Checked, "refreshed", stats.Refreshed, "deleted", stats.Deleted)
	}
}

func minCredentialRefreshInterval(targets []credentialRefreshTarget) time.Duration {
	var min time.Duration
	for _, target := range targets {
		interval := target.RefreshIntervalDuration
		if interval <= 0 {
			continue
		}
		if min == 0 || interval < min {
			min = interval
		}
	}
	return min
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
		strings.TrimSpace(lookup.GetConnectionId()),
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
		strings.TrimSpace(req.GetConnectionId()),
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

var _ gestalt.ExternalCredentialProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
