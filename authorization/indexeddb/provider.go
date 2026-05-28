package indexeddb

import (
	"context"
	"fmt"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type Provider struct {
	mu sync.Mutex
	db indexeddb.Database
}

type openIndexedDBFunc func(context.Context, ...string) (indexeddb.Database, error)

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	return configure(ctx, raw, gestalt.IndexedDB, p)
}

func configure(ctx context.Context, raw map[string]any, openIndexedDB openIndexedDBFunc, provider *Provider) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return newAuthorizationProviderError(err)
	}
	if provider == nil {
		return newAuthorizationProviderError(fmt.Errorf("provider is required"))
	}
	stores := getStoreNames()

	var db indexeddb.Database
	if cfg.IndexedDB != "" {
		db, err = openIndexedDB(ctx, cfg.IndexedDB)
	} else {
		db, err = openIndexedDB(ctx)
	}
	if err != nil {
		return newAuthorizationProviderError(fmt.Errorf("connect indexeddb: %w", err))
	}

	if err := ensureAuthorizationStores(ctx, db, stores); err != nil {
		_ = db.Close()
		return newAuthorizationProviderError(err)
	}

	provider.configureDatabase(db)
	return nil
}

func (p *Provider) configureDatabase(db indexeddb.Database) {
	p.mu.Lock()
	oldDB := p.db
	p.db = db
	p.mu.Unlock()

	if oldDB != nil {
		_ = oldDB.Close()
	}
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuthorization,
		Name:        "indexeddb",
		DisplayName: "IndexedDB Authorization",
		Description: "Stub authorization provider.",
		Version:     "0.0.1-alpha.2",
	}
}

func (p *Provider) HealthCheck(context.Context) error {
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

var _ AuthorizationProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
