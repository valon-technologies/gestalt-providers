package indexeddb

import (
	"context"
	"reflect"
	"testing"

	sdkindexeddb "github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

func TestRelationshipIndexesCoverSupportedListPlans(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		filter *RelationshipFilter
	}{
		{
			name: "resource relation",
			filter: &RelationshipFilter{
				Resource:    &Resource{Type: "repository", Id: "repo-1"},
				Relation:    "reader",
				SourceLayer: SourceLayerRuntime,
			},
		},
		{
			name: "subject",
			filter: &RelationshipFilter{
				Target:      &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
				SourceLayer: SourceLayerRuntime,
			},
		},
		{
			name: "subject set",
			filter: &RelationshipFilter{
				Target: &RelationshipTarget{SubjectSet: &SubjectSet{
					Resource: &Resource{Type: "group", Id: "engineering"},
					Relation: "member",
				}},
				SourceLayer: SourceLayerRuntime,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := relationshipTestDB(t)
			provider := New()
			provider.configureDatabase(db)
			relationship := testRelationship("repo-1", SourceLayerRuntime)
			if test.filter.Target != nil {
				relationship.Tuple.Target = test.filter.Target
			}
			seedRelationship(t, db, relationship)

			response, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: test.filter})
			if err != nil {
				t.Fatalf("ListRelationships() error = %v", err)
			}
			if len(response.Relationships) != 1 {
				t.Fatalf("ListRelationships() count = %d, want 1", len(response.Relationships))
			}
			if db.nilGetAllCalls != 0 {
				t.Fatalf("indexed list used object-store GetAll(nil) %d times", db.nilGetAllCalls)
			}
		})
	}
}

func TestRelationshipIndexLegacySourceLayerAndPaginationParity(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)

	relationships := []*Relationship{
		testRelationship("repo-1", SourceLayerRuntime),
		testRelationship("repo-2", SourceLayerRuntime),
		testRelationship("repo-3", SourceLayerRuntime),
	}
	for i, relationship := range relationships {
		record := relationshipRecord(t, relationship)
		if i == 1 {
			value := record["value"].(map[string]any)
			value["source_layer"] = float64(SourceLayerRuntime)
		}
		db.objectStore(getStoreNames().relationships).(*fakeObjectStore).records[fakeRecordID(record)] = record
	}
	filter := &RelationshipFilter{
		Target:      &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
		SourceLayer: SourceLayerRuntime,
	}
	all, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: filter})
	if err != nil {
		t.Fatalf("unpaged ListRelationships() error = %v", err)
	}
	if len(all.Relationships) != len(relationships) {
		t.Fatalf("unpaged relationship count = %d, want %d", len(all.Relationships), len(relationships))
	}

	first, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: filter, PageSize: 2})
	if err != nil {
		t.Fatalf("first page error = %v", err)
	}
	second, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: filter, PageSize: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatalf("second page error = %v", err)
	}
	paged := append(first.Relationships, second.Relationships...)
	if !reflect.DeepEqual(paged, all.Relationships) {
		t.Fatalf("paged relationships = %#v, want %#v", paged, all.Relationships)
	}
	if db.nilGetAllCalls != 0 {
		t.Fatalf("indexed list used object-store GetAll(nil) %d times", db.nilGetAllCalls)
	}
	seen := make(map[string]struct{}, len(paged))
	for _, relationship := range paged {
		key := relationshipID(relationship.Tuple)
		if _, exists := seen[key]; exists {
			t.Fatalf("relationship %q returned more than once", key)
		}
		seen[key] = struct{}{}
	}
}

func TestRelationshipIndexIgnoresTupleProperties(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	for _, property := range []string{"one", "two"} {
		relationship := testRelationship("repo-1", SourceLayerRuntime)
		relationship.Tuple.Resource.Properties = map[string]any{"tenant": property}
		seedRelationship(t, db, relationship)
	}

	response, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: &RelationshipFilter{
		Resource:    &Resource{Type: "repository", Id: "repo-1"},
		Relation:    "reader",
		SourceLayer: SourceLayerRuntime,
	}})
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	if len(response.Relationships) != 2 {
		t.Fatalf("ListRelationships() count = %d, want 2", len(response.Relationships))
	}
}

func TestRelationshipListUnsupportedFilterScansStore(t *testing.T) {
	ctx := context.Background()
	db := relationshipTestDB(t)
	provider := New()
	provider.configureDatabase(db)
	seedRelationship(t, db, testRelationship("repo-1", SourceLayerRuntime))

	response, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: &RelationshipFilter{
		ResourceType: "repository",
		SourceLayer:  SourceLayerRuntime,
	}})
	if err != nil {
		t.Fatalf("ListRelationships() error = %v", err)
	}
	if len(response.Relationships) != 1 {
		t.Fatalf("ListRelationships() count = %d, want 1", len(response.Relationships))
	}
	if db.nilGetAllCalls != 1 {
		t.Fatalf("unsupported filter GetAll(nil) calls = %d, want 1", db.nilGetAllCalls)
	}
}

func TestRelationshipIndexMigrationDefinitions(t *testing.T) {
	ctx := context.Background()
	db := &fakeIndexedDB{}
	provider := New()
	options, _, err := provider.MigrationOptions(ctx, "test", nil)
	if err != nil {
		t.Fatalf("MigrationOptions() error = %v", err)
	}
	if _, err := migrations.Run(ctx, db, options); err != nil {
		t.Fatalf("migrations.Run() error = %v", err)
	}
	got := make(map[string]sdkindexeddb.IndexDefinition, len(db.createdIndexes))
	for _, definition := range db.createdIndexes {
		got[definition.Name] = definition
	}
	want := map[string]sdkindexeddb.IndexDefinition{
		"by_resource_relation_source": {Name: "by_resource_relation_source", KeyPath: []string{"value.tuple.resource.type", "value.tuple.resource.id", "value.tuple.relation", "value.source_layer"}},
		"by_subject_source":           {Name: "by_subject_source", KeyPath: []string{"value.tuple.target.subject.type", "value.tuple.target.subject.id", "value.source_layer"}},
		"by_subject_set_source":       {Name: "by_subject_set_source", KeyPath: []string{"value.tuple.target.subject_set.resource.type", "value.tuple.target.subject_set.resource.id", "value.tuple.target.subject_set.relation", "value.source_layer"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("index definitions = %#v, want %#v", got, want)
	}
}

func TestRelationshipIndexMigrationRetriesAfterPartialCreation(t *testing.T) {
	ctx := context.Background()
	db := &fakeIndexedDB{}
	provider := New()
	options, _, err := provider.MigrationOptions(ctx, "test", nil)
	if err != nil {
		t.Fatalf("MigrationOptions() error = %v", err)
	}
	partial := sdkindexeddb.IndexDefinition{Name: "by_resource_relation_source", KeyPath: []string{"value.tuple.resource.type", "value.tuple.resource.id", "value.tuple.relation", "value.source_layer"}}
	if err := db.CreateIndex(ctx, getStoreNames().relationships, partial); err != nil {
		t.Fatalf("pre-create index error = %v", err)
	}
	if _, err := migrations.Run(ctx, db, options); err != nil {
		t.Fatalf("migrations.Run() error = %v", err)
	}
	store := db.objectStore(getStoreNames().relationships).(*fakeObjectStore)
	if len(store.indexes) != 3 {
		t.Fatalf("indexes after retry = %d, want 3", len(store.indexes))
	}
}

func relationshipTestDB(t *testing.T) *fakeIndexedDB {
	t.Helper()
	db := &fakeIndexedDB{}
	for _, definition := range []sdkindexeddb.IndexDefinition{
		{Name: "by_resource_relation_source", KeyPath: []string{"value.tuple.resource.type", "value.tuple.resource.id", "value.tuple.relation", "value.source_layer"}},
		{Name: "by_subject_source", KeyPath: []string{"value.tuple.target.subject.type", "value.tuple.target.subject.id", "value.source_layer"}},
		{Name: "by_subject_set_source", KeyPath: []string{"value.tuple.target.subject_set.resource.type", "value.tuple.target.subject_set.resource.id", "value.tuple.target.subject_set.relation", "value.source_layer"}},
	} {
		if err := db.CreateIndex(context.Background(), getStoreNames().relationships, definition); err != nil {
			t.Fatalf("CreateIndex(%q) error = %v", definition.Name, err)
		}
	}
	return db
}

func seedRelationship(t *testing.T, db *fakeIndexedDB, relationship *Relationship) {
	t.Helper()
	record := relationshipRecord(t, relationship)
	db.objectStore(getStoreNames().relationships).(*fakeObjectStore).records[fakeRecordID(record)] = record
}

func relationshipRecord(t *testing.T, relationship *Relationship) sdkindexeddb.Record {
	t.Helper()
	record, err := relationshipToRecord(relationship)
	if err != nil {
		t.Fatalf("relationshipToRecord() error = %v", err)
	}
	return record
}

func testRelationship(resourceID string, sourceLayer SourceLayer) *Relationship {
	return &Relationship{
		Tuple: &RelationshipTuple{
			Target:   &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
			Relation: "reader",
			Resource: &Resource{Type: "repository", Id: resourceID},
		},
		SourceLayer: sourceLayer,
	}
}
