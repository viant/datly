package observability

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSourceBeginStartsImmediatelyAndCompletesOnce(t *testing.T) {
	p, key, view := policyFixture(t)
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	require.NoError(t, err)
	start := time.Now()
	done := r.Begin(resolved.Operation, start)
	require.EqualValues(t, 1, r.Cumulative(resolved.Operation, "count"))
	require.Zero(t, r.Values(resolved.Operation)["Success"])
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); done(start.Add(time.Millisecond), "Success") }()
	}
	wg.Wait()
	require.EqualValues(t, 1, r.Cumulative(resolved.Operation, "count"))
	require.EqualValues(t, 1, r.Values(resolved.Operation)["Success"])
	require.Zero(t, r.Values(resolved.Operation)["Error"])
	// Default and diagnostic-only native14 captures retain the exact existing
	// deferred total Count; Source11 is the explicit compatibility opt-in.
	native := NewRecorder(nil)
	nativeDone := native.Begin("native", start)
	require.Zero(t, native.Cumulative("native", "count"))
	nativeDone(start.Add(time.Millisecond), "Success")
	require.EqualValues(t, 1, native.Cumulative("native", "count"))
	p.Views[0].Operation = nil
	diagnostic := NewRecorder(nil, WithPolicy(p))
	label, err := diagnostic.Resolve(key, view)
	require.NoError(t, err)
	diagnosticDone := diagnostic.Begin(label.Operation, start)
	require.Zero(t, diagnostic.Cumulative(label.Operation, "count"))
	diagnosticDone(start.Add(time.Millisecond), "Success")
	require.EqualValues(t, 1, diagnostic.Cumulative(label.Operation, "count"))
	require.Len(t, diagnostic.Values(label.Operation), 14)
}
