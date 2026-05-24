package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

var connectIndexedDB = func(binding string) (indexeddb.Database, error) {
	if binding == "" {
		return gestalt.IndexedDB(context.Background())
	}
	return gestalt.IndexedDB(context.Background(), binding)
}
