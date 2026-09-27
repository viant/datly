package reader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
)

func TestRelationSchedulerContainsPanicAndJoins(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		scheduler := newRelationScheduler(ctx, concurrency)
		entered := make(chan struct{})
		var stopped, skipped atomic.Int32
		work := []relationWork{{run: func(context.Context) error {
			if concurrency > 1 {
				<-entered
			}
			panic("private child-driver details")
		}}, {run: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			stopped.Add(1)
			return ctx.Err()
		}, skip: func() { skipped.Add(1) }}, {skip: func() { skipped.Add(1) }}}
		err := scheduler.Run(work)
		scheduler.Close()
		cancel()
		var failure *exec.PanicError
		require.ErrorAs(t, err, &failure)
		require.NotContains(t, err.Error(), "private child-driver details")
		require.NotEmpty(t, failure.Stack())
		if concurrency == 1 {
			require.EqualValues(t, 2, skipped.Load())
		} else {
			require.EqualValues(t, 1, stopped.Load())
			require.EqualValues(t, 1, skipped.Load())
		}
	}
}

func TestFetchSetContainsPanicAndJoins(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		entered := make(chan struct{})
		var stopped atomic.Int32
		err := (fetchSet{count: 2, concurrency: concurrency}).run(ctx, func(ctx context.Context, index int) error {
			if index == 0 {
				if concurrency > 1 {
					<-entered
				}
				panic("batch driver panic")
			}
			close(entered)
			<-ctx.Done()
			stopped.Add(1)
			return ctx.Err()
		})
		cancel()
		var failure *exec.PanicError
		require.ErrorAs(t, err, &failure)
		if concurrency > 1 {
			require.EqualValues(t, 1, stopped.Load())
		}
	}
}
