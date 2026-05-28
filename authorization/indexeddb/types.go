package indexeddb

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"
)

// AuthorizationProvider is the proposed authorization provider shape.
type AuthorizationProvider interface {
	CheckAccess(context.Context, *CheckAccessRequest) (*CheckAccessResponse, error)
	CheckAccessMany(context.Context, *CheckAccessManyRequest) (*CheckAccessManyResponse, error)
	ListRelationships(context.Context, *ListRelationshipsRequest) (*ListRelationshipsResponse, error)
	AddRelationship(context.Context, *AddRelationshipRequest) (*AddRelationshipResponse, error)
	DeleteRelationship(context.Context, *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error)
	SetRelationships(context.Context, *SetRelationshipsRequest) (*SetRelationshipsResponse, error)
	GetActiveModelRef(context.Context, *emptypb.Empty) (*GetActiveModelRefResponse, error)
	SetActiveModel(context.Context, *SetActiveModelRequest) (*SetActiveModelResponse, error)
	ListActiveModelResourceTypes(context.Context, *ListActiveModelResourceTypesRequest) (*ListActiveModelResourceTypesResponse, error)
}

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

type GetActiveModelRefResponse struct{}

type SetActiveModelRequest struct{}
type SetActiveModelResponse struct{}

type ListActiveModelResourceTypesRequest struct{}
type ListActiveModelResourceTypesResponse struct{}
