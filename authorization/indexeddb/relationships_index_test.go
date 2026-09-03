package indexeddb

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	sdkindexeddb "github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

func TestRelationshipIndexesPreserveListContract(t *testing.T) {
	ctx := context.Background()
	indexedDB := relationshipTestDB(t)
	indexedProvider := New()
	indexedProvider.configureDatabase(indexedDB)
	scanDB := &fakeIndexedDB{}
	scanProvider := New()
	scanProvider.configureDatabase(scanDB)

	relationships := []*Relationship{
		testRelationship("repo-1", SourceLayerRuntime),
		testRelationship("repo-2", SourceLayerRuntime),
		testRelationship("repo-static", SourceLayerStaticConfig),
	}
	bob := testRelationship("repo-1", SourceLayerRuntime)
	bob.Tuple.Target.Subject.Id = "user:bob"
	relationships = append(relationships, bob)
	engineering := testRelationship("repo-1", SourceLayerRuntime)
	engineering.Tuple.Target = &RelationshipTarget{SubjectSet: &SubjectSet{Resource: &Resource{Type: "group", Id: "engineering"}, Relation: "member"}}
	relationships = append(relationships, engineering)
	for _, relationship := range relationships {
		seedRelationship(t, indexedProvider, relationship)
		seedRelationship(t, scanProvider, relationship)
	}

	tests := []struct {
		name      string
		filter    *RelationshipFilter
		wantCount int
		indexed   bool
	}{
		{
			name: "resource relation and source",
			filter: &RelationshipFilter{
				Resource:    &Resource{Type: "repository", Id: "repo-1"},
				Relation:    "reader",
				SourceLayer: SourceLayerRuntime,
			},
			wantCount: 3,
			indexed:   true,
		},
		{
			name: "direct subject and source",
			filter: &RelationshipFilter{
				Target:      &RelationshipTarget{Subject: &Subject{Type: "subject", Id: "user:alice"}},
				SourceLayer: SourceLayerRuntime,
			},
			wantCount: 2,
			indexed:   true,
		},
		{
			name: "subject set and source",
			filter: &RelationshipFilter{
				Target: &RelationshipTarget{SubjectSet: &SubjectSet{
					Resource: &Resource{Type: "group", Id: "engineering"},
					Relation: "member",
				}},
				SourceLayer: SourceLayerRuntime,
			},
			wantCount: 1,
			indexed:   true,
		},
		{name: "broader resource type", filter: &RelationshipFilter{ResourceType: "repository", SourceLayer: SourceLayerRuntime}, wantCount: 4},
		{name: "unfiltered", wantCount: 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeScans := indexedDB.nilGetAllCalls
			indexed, err := indexedProvider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: test.filter})
			if err != nil {
				t.Fatalf("indexed ListRelationships() error = %v", err)
			}
			reference, err := scanProvider.ListRelationships(ctx, &ListRelationshipsRequest{Filter: test.filter})
			if err != nil {
				t.Fatalf("scan ListRelationships() error = %v", err)
			}
			if !reflect.DeepEqual(indexed, reference) {
				t.Fatalf("indexed response = %#v, scan response = %#v", indexed, reference)
			}
			if len(indexed.Relationships) != test.wantCount {
				t.Fatalf("ListRelationships() count = %d, want %d", len(indexed.Relationships), test.wantCount)
			}
			wantScans := 1
			if test.indexed {
				wantScans = 0
			}
			if scans := indexedDB.nilGetAllCalls - beforeScans; scans != wantScans {
				t.Fatalf("object-store scans = %d, want %d", scans, wantScans)
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
		if i != 1 {
			seedRelationship(t, provider, relationship)
			continue
		}
		record := relationshipRecord(t, relationship)
		value := record["value"].(map[string]any)
		value["source_layer"] = float64(SourceLayerRuntime)
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
		seedRelationship(t, provider, relationship)
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

func BenchmarkListRelationships100K(b *testing.B) {
	indexedDB := relationshipTestDB(b)
	indexedProvider := New()
	indexedProvider.configureDatabase(indexedDB)
	scanDB := &fakeIndexedDB{}
	scanProvider := New()
	scanProvider.configureDatabase(scanDB)
	for i := range 100_000 {
		relationship := testRelationship(fmt.Sprintf("repo-%06d", i), SourceLayerRuntime)
		seedRelationship(b, indexedProvider, relationship)
		seedRelationship(b, scanProvider, relationship)
	}
	request := &ListRelationshipsRequest{Filter: &RelationshipFilter{
		Resource:    &Resource{Type: "repository", Id: "repo-050000"},
		Relation:    "reader",
		SourceLayer: SourceLayerRuntime,
	}}
	for name, fixture := range map[string]struct {
		provider *Provider
		db       *fakeIndexedDB
	}{
		"indexed": {provider: indexedProvider, db: indexedDB},
		"scan":    {provider: scanProvider, db: scanDB},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			beforeScans := fixture.db.nilGetAllCalls
			for range b.N {
				response, err := fixture.provider.ListRelationships(context.Background(), request)
				if err != nil || len(response.Relationships) != 1 {
					b.Fatalf("ListRelationships() = %#v, %v", response, err)
				}
			}
			b.ReportMetric(float64(fixture.db.nilGetAllCalls-beforeScans)/float64(b.N), "object-store-scans/op")
		})
	}
}

func relationshipTestDB(t testing.TB) *fakeIndexedDB {
	t.Helper()
	db := &fakeIndexedDB{}
	options, _, err := New().MigrationOptions(context.Background(), "test", nil)
	if err != nil {
		t.Fatalf("MigrationOptions() error = %v", err)
	}
	if _, err := migrations.Run(context.Background(), db, options); err != nil {
		t.Fatalf("migrations.Run() error = %v", err)
	}
	return db
}

func seedRelationship(t testing.TB, provider *Provider, relationship *Relationship) {
	t.Helper()
	if _, err := provider.AddRelationship(context.Background(), &AddRelationshipRequest{Relationship: relationship}); err != nil {
		t.Fatalf("AddRelationship() error = %v", err)
	}
}

func relationshipRecord(t testing.TB, relationship *Relationship) sdkindexeddb.Record {
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
