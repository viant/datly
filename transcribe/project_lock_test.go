package transcribe

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProjectMetadataStoreLockContentionAndRelease(t *testing.T) {
	root := t.TempDir()
	first, err := newProjectMetadataStore(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newProjectMetadataStore(root)
	if err != nil {
		t.Fatal(err)
	}

	releaseFirst, err := first.lock(context.Background())
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
	if release, err := second.lock(ctx); release != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended lock: release=%v err=%v, want deadline exceeded", release != nil, err)
	}

	releaseFirst()
	released = true
	releaseSecond, err := second.lock(context.Background())
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	releaseSecond()
}
