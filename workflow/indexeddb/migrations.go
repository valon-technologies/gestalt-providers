package indexeddb

import (
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "workflow/indexeddb/0001_init"

func workflowIndexedDBMigrations() []migrations.Revision {
	return []migrations.Revision{{
		ID: migrationInitRevisionID,
		Schema: &migrations.SchemaDeclaration{
			Stores: []migrations.StoreDeclaration{
				{Name: storeSchedules},
				{Name: storeDefinitions},
				{Name: storeIdempotency},
				{Name: storeWorkflowKeys},
				{Name: storeRuns},
				migrationStore(storeRunClaims, workflowRunClaimSchema()),
				migrationStore(storeSignals, workflowSignalSchema()),
			},
		},
	}}
}

func migrationStore(name string, schema gestalt.ObjectStoreOptions) migrations.StoreDeclaration {
	return migrations.StoreDeclaration{
		Name:    name,
		Columns: schema.Columns,
		Indexes: schema.Indexes,
	}
}
