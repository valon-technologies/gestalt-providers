package mongodb

import (
	"context"
	"encoding/json"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const defaultDatabase = "gestalt"

type config struct {
	URI      string `json:"uri"`
	Database string `json:"database"`
}

type Provider struct {
	proto.UnimplementedIndexedDBServer
	store *Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	var cfg config
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("mongodb datastore: marshal config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("mongodb datastore: decode config: %w", err)
	}
	if cfg.URI == "" {
		return fmt.Errorf("mongodb datastore: uri is required")
	}
	if cfg.Database == "" {
		cfg.Database = defaultDatabase
	}

	clientOpts := options.Client().ApplyURI(cfg.URI)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return fmt.Errorf("mongodb datastore: connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongodb datastore: ping: %w", err)
	}

	store := NewStore(client, client.Database(cfg.Database))
	if err := store.loadSchemas(ctx); err != nil {
		_ = store.Close()
		return fmt.Errorf("mongodb datastore: load schemas: %w", err)
	}

	if p.store != nil {
		_ = p.store.Close()
	}
	p.store = store
	return nil
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	if p.store == nil {
		return fmt.Errorf("mongodb datastore: not configured")
	}
	return p.store.client.Ping(ctx, nil)
}

func (p *Provider) Close() error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) configured() (*Store, error) {
	if p.store == nil {
		return nil, fmt.Errorf("mongodb datastore: not configured")
	}
	return p.store, nil
}

// Ensure Provider satisfies the gestalt interfaces at compile time.
var _ gestalt.IndexedDBProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)

// indexedDBForward embeds into Provider so the IndexedDBServer methods can
// delegate to the store without repeating the configured() boilerplate 18
// times. It is wired up via the embed in provider_indexeddb.go.

func idFilter(id any) bson.D {
	return bson.D{{Key: "_id", Value: id}}
}
