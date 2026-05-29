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

func TestProviderSetAndGetActiveModel(t *testing.T) {
	ctx := context.Background()
	provider := New()
	fakeDB := &fakeIndexedDB{}
	provider.configureDatabase(fakeDB)
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	model := &AuthorizationModel{
		Id:      "model-1",
		Version: "v1",
		ResourceTypes: []*AuthorizationModelResourceType{
			{Name: "document"},
			{Name: "folder"},
		},
	}

	setResp, err := provider.SetActiveModel(ctx, &SetActiveModelRequest{Model: model})
	if err != nil {
		t.Fatalf("SetActiveModel() error = %v", err)
	}
	if setResp.Model.Id != "model-1" {
		t.Fatalf("SetActiveModel().Model.Id = %q, want model-1", setResp.Model.Id)
	}
	if setResp.Model.Version != "v1" {
		t.Fatalf("SetActiveModel().Model.Version = %q, want v1", setResp.Model.Version)
	}

	refResp, err := provider.GetActiveModelRef(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetActiveModelRef() error = %v", err)
	}
	if !reflect.DeepEqual(refResp.Model, setResp.Model) {
		t.Fatalf("GetActiveModelRef().Model = %#v, want %#v", refResp.Model, setResp.Model)
	}

	listResp, err := provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(active) error = %v", err)
	}
	if !reflect.DeepEqual(listResp.ResourceTypes, model.ResourceTypes) {
		t.Fatalf("ListActiveModelResourceTypes(active) = %#v, want %#v", listResp.ResourceTypes, model.ResourceTypes)
	}

	listResp, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{ModelID: "model-1"})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(model-1) error = %v", err)
	}
	if !reflect.DeepEqual(listResp.ResourceTypes, model.ResourceTypes) {
		t.Fatalf("ListActiveModelResourceTypes(model-1) = %#v, want %#v", listResp.ResourceTypes, model.ResourceTypes)
	}
}

func TestProviderSetAndListRelationships(t *testing.T) {
	ctx := context.Background()
	provider := New()
	fakeDB := &fakeIndexedDB{}
	provider.configureDatabase(fakeDB)
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	relationships := []*Relationship{
		{
			Tuple: &RelationshipTuple{
				Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
				Relation: "member",
				Resource: &Resource{Type: "group", Id: "engineering"},
			},
			SourceLayer: SourceLayerStaticConfig,
		},
		{
			Tuple: &RelationshipTuple{
				Target: &RelationshipTarget{
					SubjectSet: &SubjectSet{
						Resource: &Resource{Type: "group", Id: "engineering"},
						Relation: "member",
					},
				},
				Relation: "reader",
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			SourceLayer: SourceLayerRuntime,
		},
		{
			Tuple: &RelationshipTuple{
				Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:bob"}},
				Relation: "maintainer",
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			Properties:  map[string]any{"reason": "break-glass"},
			SourceLayer: SourceLayerRuntime,
		},
	}

	setResp, err := provider.SetRelationships(ctx, &SetRelationshipsRequest{Relationships: relationships})
	if err != nil {
		t.Fatalf("SetRelationships() error = %v", err)
	}
	if !sameRelationshipSet(setResp.Relationships, relationships) {
		t.Fatalf("SetRelationships().Relationships = %#v, want %#v", setResp.Relationships, relationships)
	}

	listResp, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{})
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	if !sameRelationshipSet(listResp.Relationships, relationships) {
		t.Fatalf("ListRelationships().Relationships = %#v, want %#v", listResp.Relationships, relationships)
	}

	firstPage, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{PageSize: 2})
	if err != nil {
		t.Fatalf("ListRelationships(first page) error = %v", err)
	}
	if len(firstPage.Relationships) != 2 {
		t.Fatalf("ListRelationships(first page) count = %d, want 2", len(firstPage.Relationships))
	}
	if firstPage.NextPageToken == "" {
		t.Fatalf("ListRelationships(first page) next page token is empty")
	}

	secondPage, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{PageSize: 2, PageToken: firstPage.NextPageToken})
	if err != nil {
		t.Fatalf("ListRelationships(second page) error = %v", err)
	}
	if len(secondPage.Relationships) != 1 {
		t.Fatalf("ListRelationships(second page) count = %d, want 1", len(secondPage.Relationships))
	}
	if secondPage.NextPageToken != "" {
		t.Fatalf("ListRelationships(second page) next page token = %q, want empty", secondPage.NextPageToken)
	}
	if !sameRelationshipSet(append(firstPage.Relationships, secondPage.Relationships...), relationships) {
		t.Fatalf("paged relationships = %#v, want %#v", append(firstPage.Relationships, secondPage.Relationships...), relationships)
	}

	runtimeResp, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{
		Filter: &RelationshipFilter{
			ResourceType: "repository",
			SourceLayer:  SourceLayerRuntime,
		},
	})
	if err != nil {
		t.Fatalf("ListRelationships(runtime repository) error = %v", err)
	}
	if len(runtimeResp.Relationships) != 2 {
		t.Fatalf("ListRelationships(runtime repository) count = %d, want 2", len(runtimeResp.Relationships))
	}

	subjectSetResp, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{
		Filter: &RelationshipFilter{
			TargetType:       RelationshipTargetTypeSubjectSet,
			TargetEntityType: "group",
			Relation:         "reader",
			Resource:         &Resource{Type: "repository", Id: "valon-tools"},
		},
	})
	if err != nil {
		t.Fatalf("ListRelationships(subject set) error = %v", err)
	}
	if len(subjectSetResp.Relationships) != 1 {
		t.Fatalf("ListRelationships(subject set) count = %d, want 1", len(subjectSetResp.Relationships))
	}
	if !reflect.DeepEqual(subjectSetResp.Relationships[0], relationships[1]) {
		t.Fatalf("ListRelationships(subject set) = %#v, want %#v", subjectSetResp.Relationships[0], relationships[1])
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

func sameRelationshipSet(a, b []*Relationship) bool {
	if len(a) != len(b) {
		return false
	}
	aByID := make(map[string]*Relationship, len(a))
	for _, relationship := range a {
		aByID[relationshipID(relationship.Tuple)] = relationship
	}
	for _, relationship := range b {
		got, ok := aByID[relationshipID(relationship.Tuple)]
		if !ok || !reflect.DeepEqual(got, relationship) {
			return false
		}
	}
	return true
}
