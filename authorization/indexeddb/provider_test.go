package indexeddb

import (
	"context"
	"strings"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestProviderLifecycleStubs(t *testing.T) {
	provider := New()

	if err := provider.Configure(context.Background(), "test", map[string]any{"ignored": true}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if err := provider.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	metadata := provider.Metadata()
	if metadata.Kind != gestalt.ProviderKindAuthorization {
		t.Fatalf("Metadata().Kind = %q, want %q", metadata.Kind, gestalt.ProviderKindAuthorization)
	}
	if metadata.Name != "indexeddb" {
		t.Fatalf("Metadata().Name = %q, want indexeddb", metadata.Name)
	}
}

func TestProposedAuthorizationProviderStubs(t *testing.T) {
	ctx := context.Background()
	provider := New()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "CheckAccess",
			call: func() error {
				_, err := provider.CheckAccess(ctx, &CheckAccessRequest{})
				return err
			},
		},
		{
			name: "CheckAccessMany",
			call: func() error {
				_, err := provider.CheckAccessMany(ctx, &CheckAccessManyRequest{})
				return err
			},
		},
		{
			name: "ListRelationships",
			call: func() error {
				_, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{})
				return err
			},
		},
		{
			name: "AddRelationship",
			call: func() error {
				_, err := provider.AddRelationship(ctx, &AddRelationshipRequest{})
				return err
			},
		},
		{
			name: "DeleteRelationship",
			call: func() error {
				_, err := provider.DeleteRelationship(ctx, &DeleteRelationshipRequest{})
				return err
			},
		},
		{
			name: "SetRelationships",
			call: func() error {
				_, err := provider.SetRelationships(ctx, &SetRelationshipsRequest{})
				return err
			},
		},
		{
			name: "GetActiveModel",
			call: func() error {
				_, err := provider.GetActiveModel(ctx, &emptypb.Empty{})
				return err
			},
		},
		{
			name: "SetActiveModel",
			call: func() error {
				_, err := provider.SetActiveModel(ctx, &SetActiveModelRequest{})
				return err
			},
		},
		{
			name: "ListModelResourceTypes",
			call: func() error {
				_, err := provider.ListModelResourceTypes(ctx, &ListModelResourceTypesRequest{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if status.Code(err) != codes.Unimplemented {
				t.Fatalf("%s() code = %v, want %v", tt.name, status.Code(err), codes.Unimplemented)
			}
			if !strings.Contains(err.Error(), tt.name) {
				t.Fatalf("%s() error = %q, want method name", tt.name, err.Error())
			}
		})
	}
}
