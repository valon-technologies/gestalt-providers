package indexeddb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestProviderConfigureAndClose(t *testing.T) {
	ctx := context.Background()
	raw := map[string]any{"indexeddb": "test-db"}
	provider := New()
	fakeDB := &fakeIndexedDB{}
	successfulOpenIndexedDB := func(ctx context.Context, name ...string) (indexeddb.Database, error) {
		if len(name) != 1 || name[0] != "test-db" {
			t.Fatalf("IndexedDB binding = %v, want test-db", name)
		}
		return fakeDB, nil
	}

	err := configure(ctx, raw, successfulOpenIndexedDB, provider)
	if err != nil {
		t.Fatalf("configure() error = %v", err)
	}

	wantStores := getStoreNames().all()
	if !reflect.DeepEqual(fakeDB.createdStores, wantStores) {
		t.Fatalf("created stores = %#v, want %#v", fakeDB.createdStores, wantStores)
	}
	if fakeDB.closed {
		t.Fatalf("database closed = true, want false")
	}

	err = provider.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}

	err = provider.Close()
	if err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !fakeDB.closed {
		t.Fatalf("database closed = false, want true")
	}

	wantErr := errors.New("connection failed")
	failingOpenIndexedDB := func(context.Context, ...string) (indexeddb.Database, error) {
		return nil, wantErr
	}

	err = configure(ctx, nil, failingOpenIndexedDB, New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("configure() error = %v, want wrapped %v", err, wantErr)
	}
	if got, want := err.Error(), "authorization: connect indexeddb: connection failed"; got != want {
		t.Fatalf("configure() error = %q, want %q", got, want)
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
			name: "GetActiveModelRef",
			call: func() error {
				_, err := provider.GetActiveModelRef(ctx, &emptypb.Empty{})
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
			name: "ListActiveModelResourceTypes",
			call: func() error {
				_, err := provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{})
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
