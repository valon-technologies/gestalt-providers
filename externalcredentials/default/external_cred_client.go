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
