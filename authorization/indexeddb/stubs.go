package indexeddb

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) CheckAccess(context.Context, *CheckAccessRequest) (*CheckAccessResponse, error) {
	return nil, unimplemented("CheckAccess")
}

func (p *Provider) CheckAccessMany(context.Context, *CheckAccessManyRequest) (*CheckAccessManyResponse, error) {
	return nil, unimplemented("CheckAccessMany")
}

func (p *Provider) AddRelationship(context.Context, *AddRelationshipRequest) (*AddRelationshipResponse, error) {
	return nil, unimplemented("AddRelationship")
}

func (p *Provider) DeleteRelationship(context.Context, *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error) {
	return nil, unimplemented("DeleteRelationship")
}

func unimplemented(method string) error {
	return status.Error(codes.Unimplemented, method+" is not implemented")
}
