package relationaldb

import (
	"context"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

// CreateIndex adds a secondary index to an existing object store, implementing
// gestalt.IndexedDBIndexProvider.
func (p *Provider) CreateIndex(ctx context.Context, req gestalt.IndexedDBCreateIndexRequest) error {
	if p.Store == nil {
		return fmt.Errorf("relationaldb: store is not configured")
	}
	return p.Store.createIndexStrict(ctx, req.Store, req.Name, req.KeyPath, IndexParameters{Unique: req.Unique})
}

// DeleteIndex removes a secondary index from an existing object store.
func (p *Provider) DeleteIndex(ctx context.Context, req gestalt.IndexedDBDeleteIndexRequest) error {
	if p.Store == nil {
		return fmt.Errorf("relationaldb: store is not configured")
	}
	return p.Store.deleteIndexStrict(ctx, req.Store, req.Name)
}

var _ gestalt.IndexedDBIndexProvider = (*Provider)(nil)
