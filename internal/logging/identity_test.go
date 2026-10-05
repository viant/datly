package logging

import (
	"context"
	"sync"
	"testing"
)

func TestIdentityOptInAndConflict(t *testing.T) {
	identity := Identity{UserID: 7, Username: "alice", Email: "alice@example.test", Scope: "read"}
	plain := context.Background()
	ObserveIdentity(plain, identity)
	if _, verified, conflict := IdentitySnapshot(plain); verified || conflict {
		t.Fatal("unrequested observation")
	}
	ctx := WithIdentityObservation(plain)
	if WithIdentityObservation(ctx) != ctx {
		t.Fatal("nested setup replaced evidence")
	}
	ObserveIdentity(ctx, identity)
	ObserveIdentity(ctx, identity)
	got, verified, conflict := IdentitySnapshot(ctx)
	if got != identity || !verified || conflict {
		t.Fatal("identical evidence was not idempotent")
	}
	got.Username = "changed"
	if got, _, _ := IdentitySnapshot(ctx); got != identity {
		t.Fatal("snapshot mutated state")
	}
	ObserveIdentity(ctx, Identity{UserID: 8})
	ObserveIdentity(ctx, identity)
	if got, verified, conflict := IdentitySnapshot(ctx); got != (Identity{}) || verified || !conflict {
		t.Fatal("conflict failed to suppress attribution")
	}
}

func TestIdentityConcurrentObservations(t *testing.T) {
	for _, conflicting := range []bool{false, true} {
		ctx := WithIdentityObservation(context.Background())
		var workers sync.WaitGroup
		for i := 0; i < 100; i++ {
			workers.Add(1)
			go func(i int) {
				defer workers.Done()
				id := Identity{UserID: 7}
				if conflicting && i%2 == 0 {
					id.UserID = 8
				}
				ObserveIdentity(ctx, id)
				IdentitySnapshot(ctx)
			}(i)
		}
		workers.Wait()
		got, verified, conflict := IdentitySnapshot(ctx)
		if conflicting {
			if got != (Identity{}) || verified || !conflict {
				t.Fatal("concurrent conflict retained attribution")
			}
		} else if got.UserID != 7 || !verified || conflict {
			t.Fatal("concurrent identical observations lost attribution")
		}
	}
}
