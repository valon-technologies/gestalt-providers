package relationaldb

import (
	"context"
	"testing"
	"time"
)

func TestAcquireLockFreshAndContended(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	lease, err := store.acquireLock(ctx, "migrate", "inst-a", time.Minute)
	if err != nil {
		t.Fatalf("acquire inst-a: %v", err)
	}
	if !lease.Acquired || lease.Holder != "inst-a" {
		t.Fatalf("expected inst-a to acquire, got %+v", lease)
	}
	if lease.FencingToken != 1 {
		t.Fatalf("expected fencing token 1, got %d", lease.FencingToken)
	}
	if !lease.ExpiresAt.After(time.Now()) {
		t.Fatalf("expected future expiry, got %v", lease.ExpiresAt)
	}

	contended, err := store.acquireLock(ctx, "migrate", "inst-b", time.Minute)
	if err != nil {
		t.Fatalf("acquire inst-b: %v", err)
	}
	if contended.Acquired {
		t.Fatalf("expected inst-b to be blocked by the live lease")
	}
	if contended.Holder != "inst-a" {
		t.Fatalf("expected holder inst-a, got %q", contended.Holder)
	}
}

func TestReleaseAllowsReacquire(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	if _, err := store.acquireLock(ctx, "k", "a", time.Minute); err != nil {
		t.Fatalf("acquire a: %v", err)
	}

	// A release by a non-holder is a no-op.
	if err := store.releaseLock(ctx, "k", "b"); err != nil {
		t.Fatalf("release by non-holder: %v", err)
	}
	if l, err := store.acquireLock(ctx, "k", "b", time.Minute); err != nil || l.Acquired {
		t.Fatalf("expected lease still held by a, got %+v (err %v)", l, err)
	}

	if err := store.releaseLock(ctx, "k", "a"); err != nil {
		t.Fatalf("release by holder: %v", err)
	}
	l, err := store.acquireLock(ctx, "k", "b", time.Minute)
	if err != nil {
		t.Fatalf("reacquire b: %v", err)
	}
	if !l.Acquired || l.Holder != "b" {
		t.Fatalf("expected b to acquire after release, got %+v", l)
	}
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// A negative TTL makes the lease immediately expired.
	if _, err := store.acquireLock(ctx, "k", "a", -time.Minute); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	l, err := store.acquireLock(ctx, "k", "b", time.Minute)
	if err != nil {
		t.Fatalf("reclaim by b: %v", err)
	}
	if !l.Acquired || l.Holder != "b" {
		t.Fatalf("expected b to reclaim expired lease, got %+v", l)
	}
	if l.FencingToken <= 1 {
		t.Fatalf("expected fencing token to increment on reclaim, got %d", l.FencingToken)
	}
}

func TestSameHolderRenews(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	first, err := store.acquireLock(ctx, "k", "a", time.Minute)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	renewed, err := store.acquireLock(ctx, "k", "a", time.Minute)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if !renewed.Acquired || renewed.Holder != "a" {
		t.Fatalf("expected a to renew, got %+v", renewed)
	}
	if renewed.FencingToken <= first.FencingToken {
		t.Fatalf("expected fencing token to advance on renew, got %d then %d", first.FencingToken, renewed.FencingToken)
	}
}
