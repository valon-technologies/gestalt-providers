package indexeddb

import (
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "authorization/indexeddb/0001_init"
const migrationRelationshipIndexesRevisionID = "authorization/indexeddb/0002_relationship_indexes"

type storeNames struct {
	state         string
	models        string
	relationships string
}

func getStoreNames() storeNames {
	return storeNames{
		state:         "authz_state",
		models:        "authz_models",
		relationships: "authz_relationships",
	}
}

func (stores storeNames) all() []string {
	return []string{
		stores.state,
		stores.models,
		stores.relationships,
	}
}

func authorizationMigrations() []migrations.Revision {
	stores := getStoreNames()
	return []migrations.Revision{{
		ID: migrationInitRevisionID,
		Schema: &migrations.SchemaDeclaration{
			Stores: []migrations.StoreDeclaration{
				{Name: stores.state},
				{Name: stores.models},
				{Name: stores.relationships},
			},
		},
	}, {
		ID: migrationRelationshipIndexesRevisionID,
		Schema: &migrations.SchemaDeclaration{AddIndexes: []migrations.AddIndexDeclaration{
			{Store: stores.relationships, Index: indexeddb.IndexSchema{Name: "by_resource_relation_source", KeyPath: []string{"value.tuple.resource.type", "value.tuple.resource.id", "value.tuple.relation", "value.source_layer"}}},
			{Store: stores.relationships, Index: indexeddb.IndexSchema{Name: "by_subject_source", KeyPath: []string{"value.tuple.target.subject.type", "value.tuple.target.subject.id", "value.source_layer"}}},
			{Store: stores.relationships, Index: indexeddb.IndexSchema{Name: "by_subject_set_source", KeyPath: []string{"value.tuple.target.subject_set.resource.type", "value.tuple.target.subject_set.resource.id", "value.tuple.target.subject_set.relation", "value.source_layer"}}},
		}},
	}}
}
