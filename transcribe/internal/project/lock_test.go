package project

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProjectMetadataStoreLockContentionAndRelease(t *testing.T) {
	root := t.TempDir()
	first, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}

	releaseFirst, err := first.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			releaseFirst()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if release, err := second.Lock(ctx); release != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended lock: release=%v err=%v, want deadline exceeded", release != nil, err)
	}

	releaseFirst()
	released = true
	releaseSecond, err := second.Lock(context.Background())
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	releaseSecond()
}
