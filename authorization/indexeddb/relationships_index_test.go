package indexeddb

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func TestRelationshipListSupportsResourceTypeFilter(t *testing.T) {
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
}

func TestRelationshipListRequiresMigration(t *testing.T) {
	ctx := context.Background()
	provider := New()
	provider.configureDatabase(&fakeIndexedDB{})
	_, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: &RelationshipFilter{
		Target: &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
	}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ListRelationships without migration error = %v, want Internal", err)
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
	provider.configureDatabase(db)
	seedRelationship(t, db, testRelationship("repo-1", SourceLayerRuntime))
	response, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: &RelationshipFilter{
		Target: &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
	}})
	if err != nil || len(response.Relationships) != 1 {
		t.Fatalf("ListRelationships after migration retry = %v, error = %v", response, err)
	}
}

func relationshipTestDB(t *testing.T) *fakeIndexedDB {
	t.Helper()
	db := &fakeIndexedDB{}
	options, _, err := New().MigrationOptions(context.Background(), "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.Run(context.Background(), db, options); err != nil {
		t.Fatal(err)
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

func TestRelationshipPagesContinueAfterEarlierDeletion(t *testing.T) {
	for _, plan := range []string{"primary", "resource", "subject", "subject set"} {
		t.Run(plan, func(t *testing.T) {
			ctx := context.Background()
			provider := New()
			provider.configureDatabase(relationshipTestDB(t))
			filter := &RelationshipFilter{SourceLayer: SourceLayerRuntime}
			target := &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}}
			switch plan {
			case "resource":
				filter.Resource = &Resource{Type: "repository", Id: "repo-1"}
			case "subject":
				filter.Target = target
			case "subject set":
				target = &RelationshipTarget{SubjectSet: &SubjectSet{Resource: &Resource{Type: "group", Id: "engineering"}, Relation: "member"}}
				filter.Target = target
			}
			var expected []*Relationship
			for i := range 12 {
				relationship := testRelationship("repo-1", SourceLayerRuntime)
				relationship.Tuple.Target = target
				relationship.Tuple.Resource.Properties = map[string]any{"position": float64(i)}
				if i%2 != 0 {
					relationship.SourceLayer = SourceLayerStaticConfig
				} else {
					expected = append(expected, relationship)
				}
				if _, err := provider.AddRelationship(ctx, &AddRelationshipRequest{Relationship: relationship}); err != nil {
					t.Fatal(err)
				}
			}
			first, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: filter, PageSize: 2})
			if err != nil || len(first.Relationships) != 2 || first.NextPageToken == "" {
				t.Fatalf("first page = %v, error = %v", first, err)
			}
			if _, err := provider.DeleteRelationship(ctx, &DeleteRelationshipRequest{RelationshipTuple: first.Relationships[0].Tuple}); err != nil {
				t.Fatal(err)
			}
			got := first.Relationships
			token := first.NextPageToken
			for pages := 0; token != ""; pages++ {
				if pages >= len(expected) {
					t.Fatal("pagination did not terminate")
				}
				next, err := provider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: filter, PageSize: 2, PageToken: token})
				if err != nil {
					t.Fatal(err)
				}
				if len(next.Relationships) != 2 {
					t.Fatalf("next page count = %d, want 2", len(next.Relationships))
				}
				got = append(got, next.Relationships...)
				token = next.NextPageToken
			}
			if !sameRelationshipSet(got, expected) {
				t.Fatalf("pagination skipped or duplicated relationships: got %v, want %v", got, expected)
			}
		})
	}
}

func TestRelationshipListRejectsInvalidPagination(t *testing.T) {
	provider := New()
	provider.configureDatabase(relationshipTestDB(t))
	for _, req := range []*ListRelationshipsRequest{{PageSize: -1}, {PageToken: "2"}, {PageToken: "bogus"}} {
		if _, err := provider.ListRelationships(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("ListRelationships(%v) error = %v, want InvalidArgument", req, err)
		}
	}
}
