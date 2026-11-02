package reader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProjectionBindingContextKeepsAbsenceAndCallerCancellation(t *testing.T) {
	type key struct{}
	deadline := time.Now().Add(time.Minute)
	active, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "foreign-authority"), deadline)
	defer cancel()
	for _, source := range []context.Context{nil, context.Background()} {
		captured := projectionBindingContext{Context: active, source: source}
		require.Nil(t, captured.Value(key{}))
		got, ok := captured.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, got)
		require.Equal(t, active.Done(), captured.Done())
	}
	cancel()
	require.ErrorIs(t, (projectionBindingContext{Context: active, source: context.Background()}).Err(), context.Canceled)
}
