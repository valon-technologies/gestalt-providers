package oidc

import (
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
)

const migrationInitRevisionID = "auth/oidc/0001_init"
const migrationGrantOwnersRevisionID = "auth/oidc/0002_grant_owners"

func oidcMigrations() []migrations.Revision {
	return []migrations.Revision{
		{
			ID: migrationInitRevisionID,
			Schema: &migrations.SchemaDeclaration{
				Stores: []migrations.StoreDeclaration{
					migrationStore(grantStoreName, grantStoreSchema()),
					migrationStore(tokenHashStoreName, tokenHashStoreSchema()),
					migrationStore(pendingOAuthStoreName, pendingOAuthStoreSchema()),
					migrationStore(subjectClaimsStoreName, subjectClaimsStoreSchema()),
				},
			},
		},
		{
			ID: migrationGrantOwnersRevisionID,
			Schema: &migrations.SchemaDeclaration{
				Stores: []migrations.StoreDeclaration{
					migrationStore(grantOwnerStoreName, grantOwnerStoreSchema()),
				},
			},
		},
	}
}

func migrationStore(name string, schema gestalt.ObjectStoreOptions) migrations.StoreDeclaration {
	return migrations.StoreDeclaration{
		Name:    name,
		Columns: schema.Columns,
		Indexes: schema.Indexes,
	}
}
