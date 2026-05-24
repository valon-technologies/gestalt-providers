package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type workflowDB = indexeddb.Database
type workflowObjectStore = indexeddb.ObjectStore
type workflowIndex = indexeddb.Index
type workflowCursor = indexeddb.Cursor
type workflowTx = indexeddb.Transaction
type workflowTxObjectStore = indexeddb.TransactionObjectStore
type workflowTxIndex = indexeddb.TransactionIndex

var connectIndexedDB = func() (workflowDB, error) {
	return gestalt.IndexedDB(context.Background())
}
