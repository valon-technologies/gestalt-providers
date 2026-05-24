package temporal

import (
	"context"

	idbfake "github.com/valon-technologies/gestalt-providers/workflow/temporal/internal/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func wrapProviderWorkflowDB(provider gestalt.IndexedDBProvider) workflowDB {
	return workflowDBAdapter{ProviderDB: idbfake.NewProviderDB(provider)}
}

type workflowDBAdapter struct {
	idbfake.ProviderDB
}

func (db workflowDBAdapter) ObjectStore(name string) workflowObjectStore {
	return workflowObjectStoreAdapter{ProviderStore: db.ProviderDB.ObjectStore(name)}
}

func (db workflowDBAdapter) Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (workflowTx, error) {
	tx, err := db.ProviderDB.Transaction(ctx, stores, mode, opts)
	if err != nil {
		return nil, err
	}
	return workflowTxAdapter{ProviderTx: tx}, nil
}

type workflowObjectStoreAdapter struct {
	idbfake.ProviderStore
}

func (s workflowObjectStoreAdapter) Index(name string) workflowIndex {
	return workflowIndexAdapter{ProviderIndex: s.ProviderStore.Index(name)}
}

func (s workflowObjectStoreAdapter) OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (workflowCursor, error) {
	cur, err := s.ProviderStore.OpenCursor(ctx, r, dir)
	if err != nil {
		return nil, err
	}
	return workflowCursorAdapter{ProviderCursor: cur}, nil
}

type workflowIndexAdapter struct {
	idbfake.ProviderIndex
}

type workflowCursorAdapter struct {
	*idbfake.ProviderCursor
}

type workflowTxAdapter struct {
	idbfake.ProviderTx
}

func (tx workflowTxAdapter) ObjectStore(name string) workflowTxObjectStore {
	return workflowTxObjectStoreAdapter{ProviderTxStore: tx.ProviderTx.ObjectStore(name)}
}

type workflowTxObjectStoreAdapter struct {
	idbfake.ProviderTxStore
}

func (s workflowTxObjectStoreAdapter) Index(name string) workflowTxIndex {
	return workflowTxIndexAdapter{ProviderTxIndex: s.ProviderTxStore.Index(name)}
}

type workflowTxIndexAdapter struct {
	idbfake.ProviderTxIndex
}
