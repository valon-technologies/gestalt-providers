package firestore

import (
	"context"
	"fmt"

	"github.com/valon-technologies/gestalt-providers/datastore/internal/configutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const providerVersion = "0.0.1-alpha.1"

type config struct {
	ProjectID string `yaml:"projectId"`
	Database  string `yaml:"database"`
}

type Provider struct {
	*Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("firestore datastore: %w", err)
	}
	store, err := NewStore(cfg.ProjectID, cfg.Database)
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindDatastore,
		Name:        "firestore",
		DisplayName: "Firestore",
		Description: "Google Cloud Firestore datastore provider.",
		Version:     providerVersion,
	}
}
