package externalcredentials

import (
	"context"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

var connectIndexedDB = func(binding string) (indexeddb.Database, error) {
	if binding == "" {
		return gestalt.IndexedDB(context.Background())
	}
	return gestalt.IndexedDB(context.Background(), binding)
}
