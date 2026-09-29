package managed

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileGenerationAtomicPublication(t *testing.T) {
	root := t.TempDir()
	first, second := NewFileStore(root), NewFileStore(root)
	_, err := first.Rotate(context.Background(), All)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errors := make(chan error, 200)
	for _, scope := range []Scope{All, Lazy, Warmup} {
		wg.Add(1)
		go func(scope Scope) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := first.Rotate(context.Background(), scope); err != nil {
					errors <- err
				}
				if _, err := second.Read(context.Background()); err != nil {
					errors <- err
				}
			}
		}(scope)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}
