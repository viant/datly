package observability

import (
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/gmetric"
	"github.com/viant/gmetric/provider"
	"github.com/viant/xdatly/response"
)

func TestBasicProviderMatchesOriginalLibraryReporting(t *testing.T) {
	p, key, view := policyFixture(t)
	p.Views[0].Operation.Provider = Basic2
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	require.NoError(t, err)
	start := time.Now().Truncate(time.Minute).Add(-time.Millisecond)
	end := start.Add(2 * time.Millisecond)
	baseline := gmetric.New()
	op := baseline.MultiOperationCounter("platform/delivery/advertiser", resolved.Operation, "datafees performance", time.Millisecond, time.Minute, 2, provider.NewBasic())
	r.Pending(resolved.Operation, 1)
	op.IncrementValueBy("pending", 1)
	require.EqualValues(t, 1, r.Values(resolved.Operation)["pending"])
	done := r.Begin(resolved.Operation, start)
	control := op.Begin(start)
	require.EqualValues(t, 1, r.Cumulative(resolved.Operation, "count"))
	done(end, "Success")
	control(end, "Success")
	r.Pending(resolved.Operation, -1)
	op.IncrementValueBy("pending", -1)
	cause := errors.New("query failed")
	r.Begin(resolved.Operation, start)(end, cause)
	op.Begin(start)(end, cause)
	r.Cache(resolved.Operation, &response.CacheStats{FoundLazy: true})
	r.Created(resolved.Operation, "lazy", 3)
	require.Equal(t, map[string]int64{"error": 1, "pending": 0}, r.Values(resolved.Operation))
	require.Equal(t, []string{"error", "pending"}, r.operations[resolved.Operation].Provider.Keys())
	for _, suffix := range []string{"operations", "operation/" + resolved.Operation, "operation/" + resolved.Operation + "/cumulative/error", "operation/" + resolved.Operation + "/cumulative/error.pct", "operation/" + resolved.Operation + "/recent/error", "operation/" + resolved.Operation + "/recent", "counters"} {
		a, b := httptest.NewRecorder(), httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/metric/"+suffix, nil)
		r.ServeMetrics("/metric/", a, req)
		gmetric.NewHandler("/metric/", baseline).ServeHTTP(b, req)
		require.Equal(t, b.Code, a.Code, suffix)
		require.JSONEq(t, b.Body.String(), a.Body.String(), suffix)
	}
}

func TestBasicProviderConcurrentCompletionCountsOnce(t *testing.T) {
	p, key, view := policyFixture(t)
	p.Views[0].Operation.Provider = Basic2
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	require.NoError(t, err)
	start := time.Now()
	done := r.Begin(resolved.Operation, start)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); done(start.Add(time.Millisecond), errors.New("failure")) }()
	}
	wg.Wait()
	require.EqualValues(t, 1, r.Cumulative(resolved.Operation, "count"))
	require.Equal(t, map[string]int64{"error": 1, "pending": 0}, r.Values(resolved.Operation))
}
