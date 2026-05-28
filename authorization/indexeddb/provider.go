package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const providerVersion = "0.0.1-alpha.2"

// AuthorizationProvider is the proposed authorization provider shape.
type AuthorizationProvider interface {
	CheckAccess(context.Context, *CheckAccessRequest) (*CheckAccessResponse, error)
	CheckAccessMany(context.Context, *CheckAccessManyRequest) (*CheckAccessManyResponse, error)
	ListRelationships(context.Context, *ListRelationshipsRequest) (*ListRelationshipsResponse, error)
	AddRelationship(context.Context, *AddRelationshipRequest) (*AddRelationshipResponse, error)
	DeleteRelationship(context.Context, *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error)
	SetRelationships(context.Context, *SetRelationshipsRequest) (*SetRelationshipsResponse, error)
	GetActiveModel(context.Context, *emptypb.Empty) (*GetActiveModelResponse, error)
	SetActiveModel(context.Context, *SetActiveModelRequest) (*SetActiveModelResponse, error)
	ListModelResourceTypes(context.Context, *ListModelResourceTypesRequest) (*ListModelResourceTypesResponse, error)
}

type Provider struct{}

type CheckAccessRequest struct{}
type CheckAccessResponse struct{}

type CheckAccessManyRequest struct{}
type CheckAccessManyResponse struct{}

type ListRelationshipsRequest struct{}
type ListRelationshipsResponse struct{}

type AddRelationshipRequest struct{}
type AddRelationshipResponse struct{}

type DeleteRelationshipRequest struct{}
type DeleteRelationshipResponse struct{}

type SetRelationshipsRequest struct{}
type SetRelationshipsResponse struct{}

type GetActiveModelResponse struct{}

type SetActiveModelRequest struct{}
type SetActiveModelResponse struct{}

type ListModelResourceTypesRequest struct{}
type ListModelResourceTypesResponse struct{}

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
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(context.Context) error {
	return nil
}

func (p *Provider) Close() error {
	return nil
}

func (p *Provider) CheckAccess(context.Context, *CheckAccessRequest) (*CheckAccessResponse, error) {
	return nil, unimplemented("CheckAccess")
}

func (p *Provider) CheckAccessMany(context.Context, *CheckAccessManyRequest) (*CheckAccessManyResponse, error) {
	return nil, unimplemented("CheckAccessMany")
}

func (p *Provider) ListRelationships(context.Context, *ListRelationshipsRequest) (*ListRelationshipsResponse, error) {
	return nil, unimplemented("ListRelationships")
}

func (p *Provider) AddRelationship(context.Context, *AddRelationshipRequest) (*AddRelationshipResponse, error) {
	return nil, unimplemented("AddRelationship")
}

func (p *Provider) DeleteRelationship(context.Context, *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error) {
	return nil, unimplemented("DeleteRelationship")
}

func (p *Provider) SetRelationships(context.Context, *SetRelationshipsRequest) (*SetRelationshipsResponse, error) {
	return nil, unimplemented("SetRelationships")
}

func (p *Provider) GetActiveModel(context.Context, *emptypb.Empty) (*GetActiveModelResponse, error) {
	return nil, unimplemented("GetActiveModel")
}

func (p *Provider) SetActiveModel(context.Context, *SetActiveModelRequest) (*SetActiveModelResponse, error) {
	return nil, unimplemented("SetActiveModel")
}

func (p *Provider) ListModelResourceTypes(context.Context, *ListModelResourceTypesRequest) (*ListModelResourceTypesResponse, error) {
	return nil, unimplemented("ListModelResourceTypes")
}

func unimplemented(method string) error {
	return status.Error(codes.Unimplemented, method+" is not implemented")
}

var _ AuthorizationProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
