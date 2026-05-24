package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	idb "github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type workflowDB = idb.Database
type workflowObjectStore = idb.ObjectStore
type workflowIndex = idb.Index
type workflowCursor = idb.Cursor
type workflowTx = idb.Transaction
type workflowTxObjectStore = idb.TransactionObjectStore
type workflowTxIndex = idb.TransactionIndex

var connectIndexedDB = func() (workflowDB, error) {
	return gestalt.IndexedDB(context.Background())
}
