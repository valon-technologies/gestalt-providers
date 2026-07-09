package temporal

import (
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "workflow/temporal/0001_init"

func workflowTemporalMigrations() []migrations.Revision {
	return []migrations.Revision{{
		ID: migrationInitRevisionID,
		Schema: &migrations.SchemaDeclaration{
			Stores: []migrations.StoreDeclaration{
				migrationStore(storeTemporalDefinitions, temporalDefinitionSchema()),
				migrationStore(storeTemporalRunIdempotency, temporalRunIdempotencySchema()),
				migrationStore(storeTemporalSignalIdempotency, temporalSignalIdempotencySchema()),
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
