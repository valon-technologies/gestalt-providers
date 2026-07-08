package externalcredentials

import (
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "externalcredentials/default/0001_init"

func externalCredentialMigrations() []migrations.Revision {
	schema := externalCredentialSchema()
	stores := []migrations.StoreDeclaration{{
		Name:    storeName,
		Columns: schema.Columns,
		Indexes: schema.Indexes,
	}}
	return []migrations.Revision{{
		ID: migrationInitRevisionID,
		Schema: &migrations.SchemaDeclaration{
			Stores: stores,
		},
	}}
}
