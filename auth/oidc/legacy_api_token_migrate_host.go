package oidc

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	proto "github.com/valon-technologies/gestalt/server/rpc/protov1/v1"
)

const envHostServiceSocket = "GESTALT_HOST_SERVICE_SOCKET"

type LegacyMigrationHost struct {
	DB      indexeddb.Database
	Store   *grantStore
	Cleanup func()
}

func OpenLegacyMigrationHost(ctx context.Context, dsn, schema string) (*LegacyMigrationHost, error) {
	if strings.TrimSpace(schema) == "" {
		schema = "gestaltd"
	}
	provider := relationaldb.New()
	if err := provider.Configure(ctx, "", map[string]any{
		"dsn":    dsn,
		"schema": schema,
	}); err != nil {
		return nil, fmt.Errorf("configure relationaldb: %w", err)
	}

	socketPath, err := os.CreateTemp("", "migrate-legacy-api-tokens-*.sock")
	if err != nil {
		_ = provider.Close()
		return nil, fmt.Errorf("create host socket: %w", err)
	}
	socket := socketPath.Name()
	_ = socketPath.Close()
	_ = os.Remove(socket)

	if err := os.Setenv(proto.EnvProviderSocket, socket); err != nil {
		_ = provider.Close()
		_ = os.Remove(socket)
		return nil, fmt.Errorf("set %s: %w", proto.EnvProviderSocket, err)
	}
	if err := os.Setenv(envHostServiceSocket, "unix://"+socket); err != nil {
		_ = provider.Close()
		_ = os.Remove(socket)
		return nil, fmt.Errorf("set %s: %w", envHostServiceSocket, err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- gestalt.ServeIndexedDBProvider(serveCtx, provider)
	}()

	if !waitForMigrationHostSocket(socket, 5*time.Second) {
		cancel()
		select {
		case err := <-serveErr:
			_ = provider.Close()
			_ = os.Remove(socket)
			return nil, fmt.Errorf("host service did not start: %v", err)
		case <-time.After(2 * time.Second):
			_ = provider.Close()
			_ = os.Remove(socket)
			return nil, fmt.Errorf("host service did not start")
		}
	}

	db, err := gestalt.IndexedDB(ctx)
	if err != nil {
		cancel()
		_ = provider.Close()
		_ = os.Remove(socket)
		return nil, fmt.Errorf("connect indexeddb client: %w", err)
	}

	store, err := openGrantStore(ctx, db, time.Now)
	if err != nil {
		_ = db.Close()
		cancel()
		_ = provider.Close()
		_ = os.Remove(socket)
		return nil, err
	}

	cleanup := func() {
		_ = db.Close()
		cancel()
		select {
		case err := <-serveErr:
			if err != nil && err != context.Canceled {
				fmt.Fprintf(os.Stderr, "indexeddb host stopped: %v\n", err)
			}
		case <-time.After(2 * time.Second):
		}
		_ = provider.Close()
		_ = os.Remove(socket)
	}
	return &LegacyMigrationHost{
		DB:      db,
		Store:   store,
		Cleanup: cleanup,
	}, nil
}

func OpenGrantStoreFromRelationalDB(ctx context.Context, dsn, schema string) (*grantStore, func(), error) {
	host, err := OpenLegacyMigrationHost(ctx, dsn, schema)
	if err != nil {
		return nil, nil, err
	}
	return host.Store, host.Cleanup, nil
}

func waitForMigrationHostSocket(socketPath string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}
