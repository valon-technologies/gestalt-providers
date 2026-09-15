package relationaldb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
	"github.com/valon-technologies/gestalt-providers/secrets/relationaldb/internal/secretstore"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	DSN    string `yaml:"dsn"`
	Schema string `yaml:"schema"`
	KMSKey string `yaml:"kmsKey"`
}

type Provider struct {
	mu    sync.RWMutex
	name  string
	store *secretstore.Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("relationaldb secrets: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	store, err := secretstore.Open(ctx, secretstore.Config{
		DSN:    cfg.DSN,
		Schema: cfg.Schema,
		KMSKey: cfg.KMSKey,
	})
	if err != nil {
		return fmt.Errorf("relationaldb secrets: %w", err)
	}

	p.mu.Lock()
	previous := p.store
	p.name = strings.TrimSpace(name)
	p.store = store
	p.mu.Unlock()
	if previous != nil {
		_ = previous.Close()
	}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "Relational Database Secrets",
		Description: "Resolves KMS-encrypted deployment secrets from a relational database.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	p.mu.RLock()
	store := p.store
	p.mu.RUnlock()
	if store == nil {
		return "", fmt.Errorf("relationaldb secrets: provider is not configured")
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	return store.Get(ctx, name)
}

func (p *Provider) Close() error {
	p.mu.Lock()
	store := p.store
	p.store = nil
	p.mu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

var _ gestalt.SecretsProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
