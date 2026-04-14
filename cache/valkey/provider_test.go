package valkey

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

	_, err := provider.Get(context.Background(), &proto.CacheGetRequest{Key: "alpha"})
	if err == nil {
		t.Fatal("Get without Configure succeeded, want error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Get without Configure code = %s, want %s", status.Code(err), codes.FailedPrecondition)
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
	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "bad-ttl",
		Value: []byte("x"),
		Ttl:   durationProto(-time.Second),
	}); err == nil {
		t.Fatal("Set with negative ttl succeeded, want error")
	} else if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Set with negative ttl code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "subms-set",
		Value: []byte("short"),
		Ttl:   durationProto(500 * time.Microsecond),
	}); err != nil {
		t.Fatalf("Set(subms-set): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	submsSet, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "subms-set"})
	if err != nil {
		t.Fatalf("Get(subms-set after expiry): %v", err)
	}
	if submsSet.GetFound() {
		t.Fatal("Get(subms-set after expiry) found = true, want false")
	}

	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "alpha",
		Value: []byte("one"),
	}); err != nil {
		t.Fatalf("Set(alpha): %v", err)
	}

	alpha, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "alpha"})
	if err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	if !alpha.GetFound() || string(alpha.GetValue()) != "one" {
		t.Fatalf("Get(alpha) = (%v, %q), want (true, %q)", alpha.GetFound(), alpha.GetValue(), "one")
	}

	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "ephemeral",
		Value: []byte("temp"),
		Ttl:   durationProto(150 * time.Millisecond),
	}); err != nil {
		t.Fatalf("Set(ephemeral): %v", err)
	}
	if _, err := provider.Touch(ctx, &proto.CacheTouchRequest{
		Key: "ephemeral",
		Ttl: durationProto(500 * time.Millisecond),
	}); err != nil {
		t.Fatalf("Touch(ephemeral): %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	ephemeral, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "ephemeral"})
	if err != nil {
		t.Fatalf("Get(ephemeral after touch): %v", err)
	}
	if !ephemeral.GetFound() || string(ephemeral.GetValue()) != "temp" {
		t.Fatalf("Get(ephemeral after touch) = (%v, %q), want (true, %q)", ephemeral.GetFound(), ephemeral.GetValue(), "temp")
	}

	time.Sleep(400 * time.Millisecond)
	expired, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "ephemeral"})
	if err != nil {
		t.Fatalf("Get(ephemeral after expiry): %v", err)
	}
	if expired.GetFound() {
		t.Fatal("Get(ephemeral after expiry) found = true, want false")
	}

	if _, err := provider.SetMany(ctx, &proto.CacheSetManyRequest{
		Entries: []*proto.CacheSetEntry{
			{Key: "beta", Value: []byte("two")},
			{Key: "gamma", Value: []byte("three")},
		},
		Ttl: durationProto(time.Second),
	}); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	if _, err := provider.SetMany(ctx, &proto.CacheSetManyRequest{
		Entries: []*proto.CacheSetEntry{
			{Key: "subms-batch-a", Value: []byte("a")},
			{Key: "subms-batch-b", Value: []byte("b")},
		},
		Ttl: durationProto(500 * time.Microsecond),
	}); err != nil {
		t.Fatalf("SetMany(subms): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	submsBatch, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "subms-batch-a"})
	if err != nil {
		t.Fatalf("Get(subms-batch-a after expiry): %v", err)
	}
	if submsBatch.GetFound() {
		t.Fatal("Get(subms-batch-a after expiry) found = true, want false")
	}

	many, err := provider.GetMany(ctx, &proto.CacheGetManyRequest{Keys: []string{"beta", "gamma", "missing"}})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got := many.GetEntries()[0]; !got.GetFound() || string(got.GetValue()) != "two" {
		t.Fatalf("GetMany beta = (%v, %q), want (true, %q)", got.GetFound(), got.GetValue(), "two")
	}
	if got := many.GetEntries()[1]; !got.GetFound() || string(got.GetValue()) != "three" {
		t.Fatalf("GetMany gamma = (%v, %q), want (true, %q)", got.GetFound(), got.GetValue(), "three")
	}
	if got := many.GetEntries()[2]; got.GetFound() {
		t.Fatal("GetMany missing found = true, want false")
	}

	deleted, err := provider.Delete(ctx, &proto.CacheDeleteRequest{Key: "beta"})
	if err != nil {
		t.Fatalf("Delete(beta): %v", err)
	}
	if !deleted.GetDeleted() {
		t.Fatal("Delete(beta) deleted = false, want true")
	}

	deletedMany, err := provider.DeleteMany(ctx, &proto.CacheDeleteManyRequest{Keys: []string{"gamma", "missing", "gamma"}})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if deletedMany.GetDeleted() != 1 {
		t.Fatalf("DeleteMany deleted = %d, want 1", deletedMany.GetDeleted())
	}

	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "persistent",
		Value: []byte("forever"),
		Ttl:   durationProto(time.Second),
	}); err != nil {
		t.Fatalf("Set(persistent): %v", err)
	}
	touched, err := provider.Touch(ctx, &proto.CacheTouchRequest{Key: "persistent"})
	if err != nil {
		t.Fatalf("Touch(persistent clear ttl): %v", err)
	}
	if !touched.GetTouched() {
		t.Fatal("Touch(persistent clear ttl) touched = false, want true")
	}
	if _, err := provider.Set(ctx, &proto.CacheSetRequest{
		Key:   "subms-touch",
		Value: []byte("touch"),
	}); err != nil {
		t.Fatalf("Set(subms-touch): %v", err)
	}
	if _, err := provider.Touch(ctx, &proto.CacheTouchRequest{
		Key: "subms-touch",
		Ttl: durationProto(500 * time.Microsecond),
	}); err != nil {
		t.Fatalf("Touch(subms-touch): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	submsTouch, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "subms-touch"})
	if err != nil {
		t.Fatalf("Get(subms-touch after expiry): %v", err)
	}
	if submsTouch.GetFound() {
		t.Fatal("Get(subms-touch after expiry) found = true, want false")
	}
	time.Sleep(1100 * time.Millisecond)
	persistent, err := provider.Get(ctx, &proto.CacheGetRequest{Key: "persistent"})
	if err != nil {
		t.Fatalf("Get(persistent): %v", err)
	}
	if !persistent.GetFound() || string(persistent.GetValue()) != "forever" {
		t.Fatalf("Get(persistent) = (%v, %q), want (true, %q)", persistent.GetFound(), persistent.GetValue(), "forever")
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

func durationProto(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}
