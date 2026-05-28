package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type Provider struct{}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(context.Context, string, map[string]any) error {
	return nil
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
	return nil
}

var _ AuthorizationProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
