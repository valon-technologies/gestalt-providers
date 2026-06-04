package indexeddb

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			{
				Name:        "document",
				SourceLayer: SourceLayerStaticConfig,
				Relations: []*AuthorizationModelRelation{
					{
						Name: "reader",
						AllowedTargets: []*AuthorizationModelAllowedTarget{
							{SubjectType: "subject"},
						},
					},
				},
			},
			{
				Name:                "folder",
				DefaultAccessPolicy: DefaultAccessPolicyAllow,
				SourceLayer:         SourceLayerRuntime,
				Relations: []*AuthorizationModelRelation{
					{
						Name: "parent",
						AllowedTargets: []*AuthorizationModelAllowedTarget{
							{ResourceType: "folder"},
						},
					},
					{
						Name: "member",
						AllowedTargets: []*AuthorizationModelAllowedTarget{
							{SubjectSetType: &SubjectSetType{ResourceType: "group", Relation: "member"}},
						},
					},
				},
			},
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
	if setResp.Model.CreatedAt.IsZero() {
		t.Fatalf("SetActiveModel().Model.CreatedAt is zero")
	}

	refResp, err := provider.GetActiveModelRef(ctx)
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
	if listResp.ModelId != "model-1" {
		t.Fatalf("ListActiveModelResourceTypes(active).ModelId = %q, want model-1", listResp.ModelId)
	}
	if listResp.NextPageToken != "" {
		t.Fatalf("ListActiveModelResourceTypes(active).NextPageToken = %q, want empty", listResp.NextPageToken)
	}

	firstPage, err := provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{PageSize: 1})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(first page) error = %v", err)
	}
	if !reflect.DeepEqual(firstPage.ResourceTypes, model.ResourceTypes[:1]) {
		t.Fatalf("ListActiveModelResourceTypes(first page) = %#v, want %#v", firstPage.ResourceTypes, model.ResourceTypes[:1])
	}
	if firstPage.NextPageToken == "" {
		t.Fatalf("ListActiveModelResourceTypes(first page).NextPageToken is empty")
	}
	if firstPage.ModelId != "model-1" {
		t.Fatalf("ListActiveModelResourceTypes(first page).ModelId = %q, want model-1", firstPage.ModelId)
	}

	secondPage, err := provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{
		PageSize:  1,
		PageToken: firstPage.NextPageToken,
	})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(second page) error = %v", err)
	}
	if !reflect.DeepEqual(secondPage.ResourceTypes, model.ResourceTypes[1:]) {
		t.Fatalf("ListActiveModelResourceTypes(second page) = %#v, want %#v", secondPage.ResourceTypes, model.ResourceTypes[1:])
	}
	if secondPage.NextPageToken != "" {
		t.Fatalf("ListActiveModelResourceTypes(second page).NextPageToken = %q, want empty", secondPage.NextPageToken)
	}
	if secondPage.ModelId != "model-1" {
		t.Fatalf("ListActiveModelResourceTypes(second page).ModelId = %q, want model-1", secondPage.ModelId)
	}

	listResp, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{
		Filter: &AuthorizationModelResourceTypeFilter{SourceLayer: SourceLayerStaticConfig},
	})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(static config) error = %v", err)
	}
	if !reflect.DeepEqual(listResp.ResourceTypes, model.ResourceTypes[:1]) {
		t.Fatalf("ListActiveModelResourceTypes(static config) = %#v, want %#v", listResp.ResourceTypes, model.ResourceTypes[:1])
	}

	listResp, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{
		Filter: &AuthorizationModelResourceTypeFilter{Name: "folder", SourceLayer: SourceLayerRuntime},
	})
	if err != nil {
		t.Fatalf("ListActiveModelResourceTypes(runtime folder) error = %v", err)
	}
	if !reflect.DeepEqual(listResp.ResourceTypes, model.ResourceTypes[1:]) {
		t.Fatalf("ListActiveModelResourceTypes(runtime folder) = %#v, want %#v", listResp.ResourceTypes, model.ResourceTypes[1:])
	}

	_, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{PageSize: -1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListActiveModelResourceTypes(negative page size) error = %v, want InvalidArgument", err)
	}

	_, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{PageToken: "bogus"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListActiveModelResourceTypes(invalid page token) error = %v, want InvalidArgument", err)
	}

	_, err = provider.ListActiveModelResourceTypes(ctx, &ListActiveModelResourceTypesRequest{PageToken: "10"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListActiveModelResourceTypes(out-of-range page token) error = %v, want InvalidArgument", err)
	}
}

func TestProviderSetActiveModelRejectsInvalidAllowedTargets(t *testing.T) {
	ctx := context.Background()
	provider := New()
	fakeDB := &fakeIndexedDB{}
	provider.configureDatabase(fakeDB)
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	tests := []struct {
		name          string
		allowedTarget *AuthorizationModelAllowedTarget
	}{
		{
			name:          "missing kind",
			allowedTarget: &AuthorizationModelAllowedTarget{},
		},
		{
			name: "multiple kinds",
			allowedTarget: &AuthorizationModelAllowedTarget{
				SubjectType:    "subject",
				SubjectSetType: &SubjectSetType{ResourceType: "group", Relation: "member"},
			},
		},
		{
			name: "invalid subject set type",
			allowedTarget: &AuthorizationModelAllowedTarget{
				SubjectSetType: &SubjectSetType{ResourceType: "group"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := provider.SetActiveModel(ctx, &SetActiveModelRequest{
				Model: &AuthorizationModel{
					Id:      "model-1",
					Version: "v1",
					ResourceTypes: []*AuthorizationModelResourceType{
						{
							Name: "group",
							Relations: []*AuthorizationModelRelation{
								{
									Name:           "member",
									AllowedTargets: []*AuthorizationModelAllowedTarget{tt.allowedTarget},
								},
							},
						},
					},
				},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("SetActiveModel() error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestProviderSetActiveModelRejectsInvalidDefaultAccessPolicy(t *testing.T) {
	ctx := context.Background()
	provider := New()
	fakeDB := &fakeIndexedDB{}
	provider.configureDatabase(fakeDB)
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	_, err := provider.SetActiveModel(ctx, &SetActiveModelRequest{
		Model: &AuthorizationModel{
			Id:      "model-1",
			Version: "v1",
			ResourceTypes: []*AuthorizationModelResourceType{
				{
					Name:                "telemetry",
					DefaultAccessPolicy: DefaultAccessPolicy(2),
				},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SetActiveModel() error = %v, want InvalidArgument", err)
	}
}

func TestProviderSetAuthorizationStateAndListRelationships(t *testing.T) {
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
			{Name: "group"},
			{Name: "repository"},
		},
	}
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

	setResp, err := provider.SetAuthorizationState(ctx, &SetAuthorizationStateRequest{
		Model:         model,
		Relationships: relationships,
	})
	if err != nil {
		t.Fatalf("SetAuthorizationState() error = %v", err)
	}
	if setResp.ActiveModel.Id != "model-1" {
		t.Fatalf("SetAuthorizationState().ActiveModel.Id = %q, want model-1", setResp.ActiveModel.Id)
	}
	if setResp.ActiveModel.Version != "v1" {
		t.Fatalf("SetAuthorizationState().ActiveModel.Version = %q, want v1", setResp.ActiveModel.Version)
	}
	if setResp.ActiveModel.CreatedAt.IsZero() {
		t.Fatalf("SetAuthorizationState().ActiveModel.CreatedAt is zero")
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

	addedRelationship := &Relationship{
		Tuple: &RelationshipTuple{
			Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:carol"}},
			Relation: "reader",
			Resource: &Resource{Type: "repository", Id: "valon-tools"},
		},
		SourceLayer: SourceLayerRuntime,
	}
	addResp, err := provider.AddRelationship(ctx, &AddRelationshipRequest{Relationship: addedRelationship})
	if err != nil {
		t.Fatalf("AddRelationship() error = %v", err)
	}
	if !reflect.DeepEqual(addResp.Relationship, addedRelationship) {
		t.Fatalf("AddRelationship().Relationship = %#v, want %#v", addResp.Relationship, addedRelationship)
	}

	withAdded := append(append([]*Relationship{}, relationships...), addedRelationship)
	listResp, err = provider.ListRelationships(ctx, &ListRelationshipsRequest{})
	if err != nil {
		t.Fatalf("ListRelationships(after add) error = %v", err)
	}
	if !sameRelationshipSet(listResp.Relationships, withAdded) {
		t.Fatalf("ListRelationships(after add).Relationships = %#v, want %#v", listResp.Relationships, withAdded)
	}

	_, err = provider.DeleteRelationship(ctx, &DeleteRelationshipRequest{RelationshipTuple: addedRelationship.Tuple})
	if err != nil {
		t.Fatalf("DeleteRelationship() error = %v", err)
	}
	listResp, err = provider.ListRelationships(ctx, &ListRelationshipsRequest{})
	if err != nil {
		t.Fatalf("ListRelationships(after delete) error = %v", err)
	}
	if !sameRelationshipSet(listResp.Relationships, relationships) {
		t.Fatalf("ListRelationships(after delete).Relationships = %#v, want %#v", listResp.Relationships, relationships)
	}
}

func TestProviderCheckAccess(t *testing.T) {
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
			{
				Name: "group",
				Relations: []*AuthorizationModelRelation{
					{Name: "member"},
				},
			},
			{
				Name: "repository",
				Relations: []*AuthorizationModelRelation{
					{Name: "reader"},
					{Name: "maintainer"},
				},
				Actions: []*AuthorizationModelAction{
					{Name: "read", Relations: []string{"reader", "maintainer"}},
					{Name: "administer", Relations: []string{"maintainer"}},
				},
			},
			{
				Name:                "telemetry",
				DefaultAccessPolicy: DefaultAccessPolicyAllow,
				Actions: []*AuthorizationModelAction{
					{Name: "readMetrics", Relations: []string{"reader"}},
				},
			},
		},
	}

	relationships := []*Relationship{
		{
			Tuple: &RelationshipTuple{
				Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
				Relation: "member",
				Resource: &Resource{Type: "group", Id: "engineering"},
			},
			SourceLayer: SourceLayerRuntime,
		},
		{
			Tuple: &RelationshipTuple{
				Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:carol"}},
				Relation: "member",
				Resource: &Resource{Type: "group", Id: "platform"},
			},
			SourceLayer: SourceLayerRuntime,
		},
		{
			Tuple: &RelationshipTuple{
				Target: &RelationshipTarget{
					SubjectSet: &SubjectSet{
						Resource: &Resource{Type: "group", Id: "platform"},
						Relation: "member",
					},
				},
				Relation: "member",
				Resource: &Resource{Type: "group", Id: "engineering"},
			},
			SourceLayer: SourceLayerRuntime,
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
			SourceLayer: SourceLayerRuntime,
		},
	}
	if _, err := provider.SetAuthorizationState(ctx, &SetAuthorizationStateRequest{
		Model:         model,
		Relationships: relationships,
	}); err != nil {
		t.Fatalf("SetAuthorizationState() error = %v", err)
	}

	tests := []struct {
		name    string
		request *CheckAccessRequest
		allowed bool
	}{
		{
			name: "allows subject set reader",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:alice"},
				Action:   &Action{Name: "read"},
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			allowed: true,
		},
		{
			name: "allows direct maintainer",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:bob"},
				Action:   &Action{Name: "administer"},
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			allowed: true,
		},
		{
			name: "allows chained subject set reader",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:carol"},
				Action:   &Action{Name: "read"},
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			allowed: true,
		},
		{
			name: "denies missing relationship",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:dana"},
				Action:   &Action{Name: "read"},
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			allowed: false,
		},
		{
			name: "allows default allow resource type without relationship",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:dana"},
				Action:   &Action{Name: "readMetrics"},
				Resource: &Resource{Type: "telemetry", Id: "metrics"},
			},
			allowed: true,
		},
		{
			name: "denies unknown action",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:alice"},
				Action:   &Action{Name: "delete"},
				Resource: &Resource{Type: "repository", Id: "valon-tools"},
			},
			allowed: false,
		},
		{
			name: "denies unknown resource type",
			request: &CheckAccessRequest{
				Subject:  &Subject{Type: "subject", Id: "user:alice"},
				Action:   &Action{Name: "read"},
				Resource: &Resource{Type: "issue", Id: "123"},
			},
			allowed: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := provider.CheckAccess(ctx, tt.request)
			if err != nil {
				t.Fatalf("CheckAccess() error = %v", err)
			}
			if resp.Allowed != tt.allowed {
				t.Fatalf("CheckAccess().Allowed = %v, want %v", resp.Allowed, tt.allowed)
			}
			if resp.ModelId != "model-1" {
				t.Fatalf("CheckAccess().ModelId = %q, want model-1", resp.ModelId)
			}
		})
	}

	manyResp, err := provider.CheckAccessMany(ctx, &CheckAccessManyRequest{
		Requests: []*CheckAccessRequest{
			tests[0].request,
			tests[3].request,
			tests[4].request,
			tests[1].request,
		},
	})
	if err != nil {
		t.Fatalf("CheckAccessMany() error = %v", err)
	}
	wantAllowed := []bool{true, false, true, true}
	if len(manyResp.Decisions) != len(wantAllowed) {
		t.Fatalf("CheckAccessMany() decisions = %d, want %d", len(manyResp.Decisions), len(wantAllowed))
	}
	for i, decision := range manyResp.Decisions {
		if decision.Allowed != wantAllowed[i] {
			t.Fatalf("CheckAccessMany().Decisions[%d].Allowed = %v, want %v", i, decision.Allowed, wantAllowed[i])
		}
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
