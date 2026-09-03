package indexeddb

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteRelationshipsAtomicMixedBatch(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	r1 := testRelationship("repo-1", SourceLayerRuntime)
	r2 := testRelationship("repo-2", SourceLayerRuntime)
	r3 := testRelationship("repo-3", SourceLayerRuntime)
	addRelationship(t, provider, r1)
	addRelationship(t, provider, r2)

	touched := cloneRelationship(r1)
	touched.Properties = map[string]any{"reason": "refresh"}
	_, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{
		{Operation: RelationshipUpdateOperationTouch, Relationship: touched},
		{Operation: RelationshipUpdateOperationDelete, Relationship: &Relationship{Tuple: r2.Tuple}},
		{Operation: RelationshipUpdateOperationCreate, Relationship: r3},
	}})
	if err != nil {
		t.Fatalf("WriteRelationships() error = %v", err)
	}
	listed, err := provider.ListRelationships(ctx, nil)
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	byResource := make(map[string]*Relationship, len(listed.Relationships))
	for _, relationship := range listed.Relationships {
		byResource[relationship.Tuple.Resource.Id] = relationship
	}
	if len(listed.Relationships) != 2 || byResource["repo-1"] == nil || byResource["repo-3"] == nil {
		t.Fatalf("relationships after atomic batch = %#v, want repo-1 and repo-3", listed.Relationships)
	}
	if byResource["repo-1"].Properties["reason"] != "refresh" {
		t.Fatalf("touched properties = %#v, want refresh", byResource["repo-1"].Properties)
	}
}

func TestWriteRelationshipsCommitFailureRollsBackBatch(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	existing := testRelationship("repo-1", SourceLayerRuntime)
	addRelationship(t, provider, existing)
	db.commitErr = errors.New("injected commit failure")

	updated := cloneRelationship(existing)
	updated.Properties = map[string]any{"state": "updated"}
	_, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{
		{Operation: RelationshipUpdateOperationTouch, Relationship: updated},
		{Operation: RelationshipUpdateOperationTouch, Relationship: testRelationship("repo-2", SourceLayerRuntime)},
	}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("WriteRelationships() code = %v, want Internal", status.Code(err))
	}
	db.commitErr = status.Error(codes.ResourceExhausted, "capacity exhausted")
	if _, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{
		Operation: RelationshipUpdateOperationTouch, Relationship: updated,
	}}}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("capacity failure code = %v, want ResourceExhausted", status.Code(err))
	}
	db.commitErr = nil
	listed, err := provider.ListRelationships(ctx, nil)
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	if len(listed.Relationships) != 1 || listed.Relationships[0].Tuple.Resource.Id != "repo-1" || listed.Relationships[0].Properties["state"] != nil {
		t.Fatalf("relationships after failed commit = %#v, want unchanged repo-1", listed.Relationships)
	}
}

func TestWriteRelationshipsCreateConflictRollsBackEarlierUpdates(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	existing := testRelationship("repo-1", SourceLayerRuntime)
	newRelationship := testRelationship("repo-2", SourceLayerRuntime)
	addRelationship(t, provider, existing)

	_, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{
		{Operation: RelationshipUpdateOperationTouch, Relationship: newRelationship},
		{Operation: RelationshipUpdateOperationCreate, Relationship: existing},
	}})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteRelationships() code = %v, want AlreadyExists", status.Code(err))
	}
	listed, err := provider.ListRelationships(ctx, nil)
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	if len(listed.Relationships) != 1 || listed.Relationships[0].Tuple.Resource.Id != existing.Tuple.Resource.Id {
		t.Fatalf("relationships after CREATE conflict = %#v, want only existing relationship", listed.Relationships)
	}
}

func TestWriteRelationshipsPreconditionsUseInitialSnapshotAndRollback(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name          string
		preconditions []*Precondition
		wantCode      codes.Code
	}{
		{
			name: "both match initial snapshot",
			preconditions: []*Precondition{
				{Operation: PreconditionOperationMustMatch, Filter: relationshipFilter("repo-1")},
				{Operation: PreconditionOperationMustNotMatch, Filter: relationshipFilter("repo-2")},
			},
		},
		{
			name:          "must match fails",
			preconditions: []*Precondition{{Operation: PreconditionOperationMustMatch, Filter: relationshipFilter("repo-2")}},
			wantCode:      codes.FailedPrecondition,
		},
		{
			name:          "must not match fails",
			preconditions: []*Precondition{{Operation: PreconditionOperationMustNotMatch, Filter: relationshipFilter("repo-1")}},
			wantCode:      codes.FailedPrecondition,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := relationshipTestDB(t)
			provider := New()
			provider.configureDatabase(db)
			existing := testRelationship("repo-1", SourceLayerRuntime)
			newRelationship := testRelationship("repo-2", SourceLayerRuntime)
			addRelationship(t, provider, existing)

			_, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{
				Updates: []*RelationshipUpdate{
					{Operation: RelationshipUpdateOperationTouch, Relationship: newRelationship},
					{Operation: RelationshipUpdateOperationDelete, Relationship: &Relationship{Tuple: existing.Tuple}},
				},
				OptionalPreconditions: test.preconditions,
			})
			if test.wantCode == codes.OK {
				if err != nil {
					t.Fatalf("WriteRelationships() error = %v", err)
				}
				listed, err := provider.ListRelationships(ctx, nil)
				if err != nil {
					t.Fatalf("ListRelationships() error = %v", err)
				}
				if len(listed.Relationships) != 1 || listed.Relationships[0].Tuple.Resource.Id != newRelationship.Tuple.Resource.Id {
					t.Fatalf("relationships after successful preconditioned write = %#v, want only new relationship", listed.Relationships)
				}
				return
			}
			if status.Code(err) != test.wantCode {
				t.Fatalf("WriteRelationships() code = %v, want %v", status.Code(err), test.wantCode)
			}
			listed, err := provider.ListRelationships(ctx, nil)
			if err != nil {
				t.Fatalf("ListRelationships() error = %v", err)
			}
			if len(listed.Relationships) != 1 || listed.Relationships[0].Tuple.Resource.Id != existing.Tuple.Resource.Id {
				t.Fatalf("relationships after failed precondition = %#v, want only existing relationship", listed.Relationships)
			}
		})
	}
}

func TestWriteRelationshipsSourceGuardAndIdempotentDelete(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	static := testRelationship("repo-1", SourceLayerStaticConfig)
	addRelationship(t, provider, static)

	_, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{
		Operation:    RelationshipUpdateOperationDelete,
		Relationship: &Relationship{Tuple: static.Tuple, SourceLayer: SourceLayerRuntime},
	}}})
	if err != nil {
		t.Fatalf("source-guarded delete error = %v", err)
	}
	listed, err := provider.ListRelationships(ctx, nil)
	if err != nil {
		t.Fatalf("ListRelationships() after source-guarded delete error = %v", err)
	}
	if len(listed.Relationships) != 1 || listed.Relationships[0].SourceLayer != SourceLayerStaticConfig {
		t.Fatalf("relationships after source-guarded delete = %#v, want static relationship", listed.Relationships)
	}
	for range 2 {
		if _, err := provider.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{
			Operation:    RelationshipUpdateOperationDelete,
			Relationship: &Relationship{Tuple: static.Tuple},
		}}}); err != nil {
			t.Fatalf("idempotent unspecified delete error = %v", err)
		}
	}
	listed, err = provider.ListRelationships(ctx, nil)
	if err != nil {
		t.Fatalf("ListRelationships() after idempotent delete error = %v", err)
	}
	if len(listed.Relationships) != 0 {
		t.Fatalf("relationships after idempotent delete = %#v, want empty", listed.Relationships)
	}
}

func TestWriteRelationshipsValidation(t *testing.T) {
	ctx := context.Background()
	baseRelationship := testRelationship("repo-1", SourceLayerRuntime)
	unknownSourceRelationship := cloneRelationship(baseRelationship)
	unknownSourceRelationship.SourceLayer = SourceLayer(99)
	tests := []struct {
		name string
		req  *WriteRelationshipsRequest
	}{
		{name: "nil request"},
		{name: "empty updates", req: &WriteRelationshipsRequest{}},
		{name: "nil relationship", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch}}}},
		{name: "unspecified update operation", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{Relationship: baseRelationship}}}},
		{name: "unknown update operation", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{Operation: RelationshipUpdateOperation(99), Relationship: baseRelationship}}}},
		{name: "unknown relationship source layer", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: unknownSourceRelationship}}}},
		{name: "invalid update after valid prefix", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{
			{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship},
			{Operation: RelationshipUpdateOperationUnspecified, Relationship: testRelationship("repo-2", SourceLayerRuntime)},
		}}},
		{name: "duplicate relationship identity", req: &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{
			{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship},
			{Operation: RelationshipUpdateOperationDelete, Relationship: baseRelationship},
		}}},
		{name: "nil precondition filter", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Operation: PreconditionOperationMustMatch}},
		}},
		{name: "empty precondition filter", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Operation: PreconditionOperationMustMatch, Filter: &RelationshipFilter{}}},
		}},
		{name: "unspecified precondition operation", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Filter: relationshipFilter("repo-1")}},
		}},
		{name: "unknown precondition operation", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Operation: PreconditionOperation(99), Filter: relationshipFilter("repo-1")}},
		}},
		{name: "unknown precondition source layer", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Operation: PreconditionOperationMustMatch, Filter: &RelationshipFilter{SourceLayer: SourceLayer(99)}}},
		}},
		{name: "unknown precondition target type", req: &WriteRelationshipsRequest{
			Updates:               []*RelationshipUpdate{{Operation: RelationshipUpdateOperationTouch, Relationship: baseRelationship}},
			OptionalPreconditions: []*Precondition{{Operation: PreconditionOperationMustMatch, Filter: &RelationshipFilter{TargetType: RelationshipTargetType(99)}}},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := relationshipTestDB(t)
			provider := New()
			provider.configureDatabase(db)

			_, err := provider.WriteRelationships(ctx, test.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteRelationships() code = %v, want InvalidArgument", status.Code(err))
			}
			listed, err := provider.ListRelationships(ctx, nil)
			if err != nil {
				t.Fatalf("ListRelationships() error = %v", err)
			}
			if len(listed.Relationships) != 0 {
				t.Fatalf("relationships after invalid request = %#v, want empty", listed.Relationships)
			}
		})
	}
}

func TestLegacyAddDeleteMapToTouchAndDelete(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	relationship := testRelationship("repo-1", SourceLayerRuntime)
	if _, err := provider.AddRelationship(ctx, &AddRelationshipRequest{Relationship: relationship}); err != nil {
		t.Fatalf("first AddRelationship() error = %v", err)
	}
	listed, err := provider.ListRelationships(ctx, nil)
	if err != nil || len(listed.Relationships) != 1 {
		t.Fatalf("ListRelationships() after first add = %#v, error %v", listed, err)
	}
	updated := cloneRelationship(relationship)
	updated.Properties = map[string]any{"source": "legacy"}
	if _, err := provider.AddRelationship(ctx, &AddRelationshipRequest{Relationship: updated}); err != nil {
		t.Fatalf("second AddRelationship() error = %v", err)
	}
	listed, err = provider.ListRelationships(ctx, nil)
	if err != nil || len(listed.Relationships) != 1 || listed.Relationships[0].Properties["source"] != "legacy" {
		t.Fatalf("ListRelationships() after second add = %#v, error %v", listed, err)
	}
	if _, err := provider.DeleteRelationship(ctx, &DeleteRelationshipRequest{RelationshipTuple: relationship.Tuple}); err != nil {
		t.Fatalf("first DeleteRelationship() error = %v", err)
	}
	listed, err = provider.ListRelationships(ctx, nil)
	if err != nil || len(listed.Relationships) != 0 {
		t.Fatalf("ListRelationships() after first delete = %#v, error %v", listed, err)
	}
	if _, err := provider.DeleteRelationship(ctx, &DeleteRelationshipRequest{RelationshipTuple: relationship.Tuple}); err != nil {
		t.Fatalf("second DeleteRelationship() error = %v", err)
	}
	listed, err = provider.ListRelationships(ctx, nil)
	if err != nil || len(listed.Relationships) != 0 {
		t.Fatalf("ListRelationships() after second delete = %#v, error %v", listed, err)
	}
}

func relationshipFilter(resourceID string) *RelationshipFilter {
	return &RelationshipFilter{
		Resource:    &Resource{Type: "repository", Id: resourceID},
		Relation:    "reader",
		SourceLayer: SourceLayerRuntime,
	}
}

func addRelationship(t *testing.T, provider *Provider, relationship *Relationship) {
	t.Helper()
	if _, err := provider.AddRelationship(context.Background(), &AddRelationshipRequest{Relationship: relationship}); err != nil {
		t.Fatalf("AddRelationship() error = %v", err)
	}
}
