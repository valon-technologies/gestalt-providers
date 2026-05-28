package indexeddb

import (
	"context"
	"errors"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type storeNames struct {
	runtimeResourceTypes string
	runtimeRelationships string
	state                string
	models               string
	relationships        string
}

func getStoreNames() storeNames {
	return storeNames{
		runtimeResourceTypes: "authz_runtime_resource_types",
		runtimeRelationships: "authz_runtime_relationships",
		state:                "authz_state",
		models:               "authz_models",
		relationships:        "authz_relationships",
	}
}

func (stores storeNames) all() []string {
	return []string{
		stores.runtimeResourceTypes,
		stores.runtimeRelationships,
		stores.state,
		stores.models,
		stores.relationships,
	}
}

func ensureAuthorizationStores(ctx context.Context, db indexeddb.Database, stores storeNames) error {
	if db == nil {
		return fmt.Errorf("indexeddb database is required")
	}
	for _, name := range stores.all() {
		if _, err := db.CreateObjectStore(ctx, name, indexeddb.ObjectStoreOptions{}); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			return fmt.Errorf("create %s store: %w", name, err)
		}
	}
	return nil
}
