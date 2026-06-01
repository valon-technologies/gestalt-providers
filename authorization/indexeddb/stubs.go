package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) Evaluate(context.Context, *gestalt.AccessEvaluationRequest) (*gestalt.AccessDecision, error) {
	return nil, unimplemented("Evaluate")
}

func (p *Provider) EvaluateMany(context.Context, *gestalt.AccessEvaluationsRequest) (*gestalt.AccessEvaluationsResponse, error) {
	return nil, unimplemented("EvaluateMany")
}

func (p *Provider) SearchResources(context.Context, *gestalt.ResourceSearchRequest) (*gestalt.ResourceSearchResponse, error) {
	return nil, unimplemented("SearchResources")
}

func (p *Provider) SearchSubjects(context.Context, *gestalt.SubjectSearchRequest) (*gestalt.SubjectSearchResponse, error) {
	return nil, unimplemented("SearchSubjects")
}

func (p *Provider) SearchActions(context.Context, *gestalt.ActionSearchRequest) (*gestalt.ActionSearchResponse, error) {
	return nil, unimplemented("SearchActions")
}

func (p *Provider) GetMetadata(context.Context) (*gestalt.AuthorizationMetadata, error) {
	return nil, unimplemented("GetMetadata")
}

func (p *Provider) ReadRelationships(context.Context, *gestalt.ReadRelationshipsRequest) (*gestalt.ReadRelationshipsResponse, error) {
	return nil, unimplemented("ReadRelationships")
}

func (p *Provider) WriteRelationships(context.Context, *gestalt.WriteRelationshipsRequest) error {
	return unimplemented("WriteRelationships")
}

func (p *Provider) GetActiveModel(context.Context) (*gestalt.GetActiveModelResponse, error) {
	return nil, unimplemented("GetActiveModel")
}

func (p *Provider) ListModels(context.Context, *gestalt.ListModelsRequest) (*gestalt.ListModelsResponse, error) {
	return nil, unimplemented("ListModels")
}

func (p *Provider) WriteModel(context.Context, *gestalt.WriteModelRequest) (*gestalt.AuthorizationModelRef, error) {
	return nil, unimplemented("WriteModel")
}

func unimplemented(method string) error {
	return status.Error(codes.Unimplemented, method+" is not implemented")
}
