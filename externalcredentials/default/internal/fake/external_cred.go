package fake

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type ProviderExternalCredClient struct {
	Provider gestalt.ExternalCredentialProvider
}

func NewProviderExternalCredClient(provider gestalt.ExternalCredentialProvider) ProviderExternalCredClient {
	return ProviderExternalCredClient{Provider: provider}
}

func (c ProviderExternalCredClient) UpsertCredential(ctx context.Context, req *gestalt.UpsertExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	return c.Provider.UpsertCredential(ctx, req)
}

func (c ProviderExternalCredClient) GetCredential(ctx context.Context, req *gestalt.GetExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	return c.Provider.GetCredential(ctx, req)
}

func (c ProviderExternalCredClient) ListCredentials(ctx context.Context, req *gestalt.ListExternalCredentialsRequest) (*gestalt.ListExternalCredentialsResponse, error) {
	return c.Provider.ListCredentials(ctx, req)
}

func (c ProviderExternalCredClient) DeleteCredential(ctx context.Context, req *gestalt.DeleteExternalCredentialRequest) error {
	return c.Provider.DeleteCredential(ctx, req)
}

func (c ProviderExternalCredClient) ValidateCredentialConfig(ctx context.Context, req *gestalt.ValidateExternalCredentialConfigRequest) error {
	return c.Provider.ValidateCredentialConfig(ctx, req)
}

func (c ProviderExternalCredClient) ResolveCredential(ctx context.Context, req *gestalt.ResolveExternalCredentialRequest) (*gestalt.ResolveExternalCredentialResponse, error) {
	return c.Provider.ResolveCredential(ctx, req)
}

func (c ProviderExternalCredClient) ExchangeCredential(ctx context.Context, req *gestalt.ExchangeExternalCredentialRequest) (*gestalt.ExchangeExternalCredentialResponse, error) {
	return c.Provider.ExchangeCredential(ctx, req)
}

func (c ProviderExternalCredClient) Close() error { return nil }
