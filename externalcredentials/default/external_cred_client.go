package externalcredentials

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type externalCredClient interface {
	UpsertCredential(ctx context.Context, req *gestalt.UpsertExternalCredentialRequest) (*gestalt.ExternalCredential, error)
	GetCredential(ctx context.Context, req *gestalt.GetExternalCredentialRequest) (*gestalt.ExternalCredential, error)
	ListCredentials(ctx context.Context, req *gestalt.ListExternalCredentialsRequest) (*gestalt.ListExternalCredentialsResponse, error)
	DeleteCredential(ctx context.Context, req *gestalt.DeleteExternalCredentialRequest) error
	ValidateCredentialConfig(ctx context.Context, req *gestalt.ValidateExternalCredentialConfigRequest) error
	ResolveCredential(ctx context.Context, req *gestalt.ResolveExternalCredentialRequest) (*gestalt.ResolveExternalCredentialResponse, error)
	ExchangeCredential(ctx context.Context, req *gestalt.ExchangeExternalCredentialRequest) (*gestalt.ExchangeExternalCredentialResponse, error)
	Close() error
}

var connectExternalCredentials = func() (externalCredClient, error) {
	client, err := gestalt.ExternalCredentials()
	return sdkExternalCred{client}, err
}

type sdkExternalCred struct {
	*gestalt.ExternalCredentialClient
}

type providerCredClient struct {
	provider *Provider
}

func (c providerCredClient) UpsertCredential(ctx context.Context, req *gestalt.UpsertExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	return c.provider.UpsertCredential(ctx, req)
}

func (c providerCredClient) GetCredential(ctx context.Context, req *gestalt.GetExternalCredentialRequest) (*gestalt.ExternalCredential, error) {
	return c.provider.GetCredential(ctx, req)
}

func (c providerCredClient) ListCredentials(ctx context.Context, req *gestalt.ListExternalCredentialsRequest) (*gestalt.ListExternalCredentialsResponse, error) {
	return c.provider.ListCredentials(ctx, req)
}

func (c providerCredClient) DeleteCredential(ctx context.Context, req *gestalt.DeleteExternalCredentialRequest) error {
	return c.provider.DeleteCredential(ctx, req)
}

func (c providerCredClient) ValidateCredentialConfig(ctx context.Context, req *gestalt.ValidateExternalCredentialConfigRequest) error {
	return c.provider.ValidateCredentialConfig(ctx, req)
}

func (c providerCredClient) ResolveCredential(ctx context.Context, req *gestalt.ResolveExternalCredentialRequest) (*gestalt.ResolveExternalCredentialResponse, error) {
	return c.provider.ResolveCredential(ctx, req)
}

func (c providerCredClient) ExchangeCredential(ctx context.Context, req *gestalt.ExchangeExternalCredentialRequest) (*gestalt.ExchangeExternalCredentialResponse, error) {
	return c.provider.ExchangeCredential(ctx, req)
}

func (c providerCredClient) Close() error { return nil }
