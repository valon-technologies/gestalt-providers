package externalcredentials

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const providerVersion = "0.0.1-alpha.1"

type Provider struct {
	mu    sync.RWMutex
	cfg   config
	store *store
	now   func() time.Time
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
	oldStore := p.store
	p.cfg = cfg
	p.store = st
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
	_, err = st.credentials.Count(ctx, nil)
	return err
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.store == nil {
		return nil
	}
	err := p.store.Close()
	p.store = nil
	return err
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
