package valkey

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const testDB = 15

func TestProviderRequiresAddresses(t *testing.T) {
	err := New().Configure(context.Background(), "session", map[string]any{})
	if err == nil {
		t.Fatal("Configure without addresses succeeded, want error")
	}
	if err.Error() != "valkey cache: addresses is required" {
		t.Fatalf("Configure without addresses error = %q, want %q", err.Error(), "valkey cache: addresses is required")
	}
}

func TestProviderRequiresConfiguration(t *testing.T) {
	provider := New()

	_, _, err := provider.Get(context.Background(), "alpha")
	if err == nil {
		t.Fatal("Get without Configure succeeded, want error")
	}
	if err.Error() != "valkey cache: not configured" {
		t.Fatalf("Get without Configure error = %q, want not configured", err.Error())
	}
}

func TestProviderLifecycle(t *testing.T) {
	addr := os.Getenv("GESTALT_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("GESTALT_TEST_VALKEY_ADDR is not set")
	}

	ctx := context.Background()
	raw := newTestClient(t, addr)
	flushTestDB(t, ctx, raw)

	provider := New()
	if err := provider.Configure(ctx, "session", map[string]any{
		"addresses": []string{addr},
		"db":        testDB,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Close()
		flushTestDB(t, ctx, raw)
		raw.Close()
	})

	if err := provider.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if err := provider.Set(ctx, "bad-ttl", []byte("x"), gestalt.CacheSetOptions{TTL: -time.Second}); err == nil {
		t.Fatal("Set with negative ttl succeeded, want error")
	}
	if err := provider.Set(ctx, "subms-set", []byte("short"), gestalt.CacheSetOptions{TTL: 500 * time.Microsecond}); err != nil {
		t.Fatalf("Set(subms-set): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	_, submsSet, err := provider.Get(ctx, "subms-set")
	if err != nil {
		t.Fatalf("Get(subms-set after expiry): %v", err)
	}
	if submsSet {
		t.Fatal("Get(subms-set after expiry) found = true, want false")
	}

	if err := provider.Set(ctx, "alpha", []byte("one"), gestalt.CacheSetOptions{}); err != nil {
		t.Fatalf("Set(alpha): %v", err)
	}

	alpha, found, err := provider.Get(ctx, "alpha")
	if err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	if !found || string(alpha) != "one" {
		t.Fatalf("Get(alpha) = (%v, %q), want (true, %q)", found, alpha, "one")
	}

	if err := provider.Set(ctx, "ephemeral", []byte("temp"), gestalt.CacheSetOptions{TTL: 150 * time.Millisecond}); err != nil {
		t.Fatalf("Set(ephemeral): %v", err)
	}
	if _, err := provider.Touch(ctx, "ephemeral", 500*time.Millisecond); err != nil {
		t.Fatalf("Touch(ephemeral): %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	ephemeral, found, err := provider.Get(ctx, "ephemeral")
	if err != nil {
		t.Fatalf("Get(ephemeral after touch): %v", err)
	}
	if !found || string(ephemeral) != "temp" {
		t.Fatalf("Get(ephemeral after touch) = (%v, %q), want (true, %q)", found, ephemeral, "temp")
	}

	time.Sleep(400 * time.Millisecond)
	_, expired, err := provider.Get(ctx, "ephemeral")
	if err != nil {
		t.Fatalf("Get(ephemeral after expiry): %v", err)
	}
	if expired {
		t.Fatal("Get(ephemeral after expiry) found = true, want false")
	}

	if err := provider.SetMany(ctx, []gestalt.CacheEntry{
		{Key: "beta", Value: []byte("two")},
		{Key: "gamma", Value: []byte("three")},
	}, gestalt.CacheSetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if err := provider.SetMany(ctx, []gestalt.CacheEntry{
		{Key: "subms-batch-a", Value: []byte("a")},
		{Key: "subms-batch-b", Value: []byte("b")},
	}, gestalt.CacheSetOptions{TTL: 500 * time.Microsecond}); err != nil {
		t.Fatalf("SetMany(subms): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	_, submsBatch, err := provider.Get(ctx, "subms-batch-a")
	if err != nil {
		t.Fatalf("Get(subms-batch-a after expiry): %v", err)
	}
	if submsBatch {
		t.Fatal("Get(subms-batch-a after expiry) found = true, want false")
	}

	many, err := provider.GetMany(ctx, []string{"beta", "gamma", "missing"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got, ok := many["beta"]; !ok || string(got) != "two" {
		t.Fatalf("GetMany beta = (%v, %q), want (true, %q)", ok, got, "two")
	}
	if got, ok := many["gamma"]; !ok || string(got) != "three" {
		t.Fatalf("GetMany gamma = (%v, %q), want (true, %q)", ok, got, "three")
	}
	if _, ok := many["missing"]; ok {
		t.Fatal("GetMany missing found = true, want false")
	}

	deleted, err := provider.Delete(ctx, "beta")
	if err != nil {
		t.Fatalf("Delete(beta): %v", err)
	}
	if !deleted {
		t.Fatal("Delete(beta) deleted = false, want true")
	}

	deletedMany, err := provider.DeleteMany(ctx, []string{"gamma", "missing", "gamma"})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if deletedMany != 1 {
		t.Fatalf("DeleteMany deleted = %d, want 1", deletedMany)
	}

	if err := provider.Set(ctx, "persistent", []byte("forever"), gestalt.CacheSetOptions{TTL: time.Second}); err != nil {
		t.Fatalf("Set(persistent): %v", err)
	}
	touched, err := provider.Touch(ctx, "persistent", 0)
	if err != nil {
		t.Fatalf("Touch(persistent clear ttl): %v", err)
	}
	if !touched {
		t.Fatal("Touch(persistent clear ttl) touched = false, want true")
	}
	if err := provider.Set(ctx, "subms-touch", []byte("touch"), gestalt.CacheSetOptions{}); err != nil {
		t.Fatalf("Set(subms-touch): %v", err)
	}
	if _, err := provider.Touch(ctx, "subms-touch", 500*time.Microsecond); err != nil {
		t.Fatalf("Touch(subms-touch): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	_, submsTouch, err := provider.Get(ctx, "subms-touch")
	if err != nil {
		t.Fatalf("Get(subms-touch after expiry): %v", err)
	}
	if submsTouch {
		t.Fatal("Get(subms-touch after expiry) found = true, want false")
	}
	time.Sleep(1100 * time.Millisecond)
	persistent, found, err := provider.Get(ctx, "persistent")
	if err != nil {
		t.Fatalf("Get(persistent): %v", err)
	}
	if !found || string(persistent) != "forever" {
		t.Fatalf("Get(persistent) = (%v, %q), want (true, %q)", found, persistent, "forever")
	}
}

func newTestClient(t *testing.T, addr string) valkey.Client {
	t.Helper()

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		SelectDB:          testDB,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		t.Fatalf("Ping: %v", err)
	}
	return client
}

func flushTestDB(t *testing.T, ctx context.Context, client valkey.Client) {
	t.Helper()
	if err := client.Do(ctx, client.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("Flushdb: %v", err)
	}
}
