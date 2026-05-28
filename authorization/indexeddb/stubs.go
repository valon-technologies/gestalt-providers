package indexeddb

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

func (p *Provider) GetActiveModelRef(context.Context, *emptypb.Empty) (*GetActiveModelRefResponse, error) {
	return nil, unimplemented("GetActiveModelRef")
}

func (p *Provider) SetActiveModel(context.Context, *SetActiveModelRequest) (*SetActiveModelResponse, error) {
	return nil, unimplemented("SetActiveModel")
}

func (p *Provider) ListActiveModelResourceTypes(context.Context, *ListActiveModelResourceTypesRequest) (*ListActiveModelResourceTypesResponse, error) {
	return nil, unimplemented("ListActiveModelResourceTypes")
}

func unimplemented(method string) error {
	return status.Error(codes.Unimplemented, method+" is not implemented")
}
