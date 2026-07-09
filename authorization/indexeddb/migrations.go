package indexeddb

import (
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "authorization/indexeddb/0001_init"

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
	}}
}
