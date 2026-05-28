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

// AuthorizationModel is currently stored with a shape like:
//
//	{
//	  "id": "model-1",
//	  "version": "v1",
//	  "resource_types": [
//	    {"name": "document"},
//	    {"name": "folder"}
//	  ]
//	}
type AuthorizationModel struct {
	Id            string                            `json:"id"`
	Version       string                            `json:"version"`
	ResourceTypes []*AuthorizationModelResourceType `json:"resource_types"`
}

type AuthorizationModelRef struct {
	Id      string `json:"id"`
	Version string `json:"version"`
}

type AuthorizationModelResourceType struct {
	Name string `json:"name"`
}

type GetActiveModelRefResponse struct {
	Model *AuthorizationModelRef
}

type SetActiveModelRequest struct {
	Model *AuthorizationModel
}

type SetActiveModelResponse struct {
	Model *AuthorizationModelRef
}

type ListActiveModelResourceTypesRequest struct {
	ModelID string
}

type ListActiveModelResourceTypesResponse struct {
	ResourceTypes []*AuthorizationModelResourceType
}
