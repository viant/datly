package txread

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGateTransactionIdentityCancellationAndCleanup(t *testing.T) {
	ctx := context.Background()
	first, other := new(sql.Tx), new(sql.Tx)
	release, err := Acquire(ctx, first)
	require.NoError(t, err)
	t.Cleanup(release)
	independent, err := Acquire(ctx, other)
	require.NoError(t, err)
	independent()
	noTransaction, err := Acquire(ctx, nil)
	require.NoError(t, err)
	noTransaction()
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		unlock, err := Acquire(waitCtx, first)
		if unlock != nil {
			unlock()
		}
		finished <- err
	}()
	require.Eventually(t, func() bool {
		active.Lock()
		defer active.Unlock()
		return active.gates[first].users == 2
	}, time.Second, time.Millisecond)
	cancel()
	require.True(t, errors.Is(<-finished, context.Canceled))
	active.Lock()
	users := active.gates[first].users
	active.Unlock()
	require.Equal(t, 1, users)
	release()
	release() // Cleanup and explicit release may both run.
	active.Lock()
	_, found := active.gates[first]
	active.Unlock()
	require.False(t, found)
	unlock, err := Acquire(ctx, first)
	require.NoError(t, err)
	unlock()
}
