package observability

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/gmetric"
)

type slowMetricWriter struct {
	header           http.Header
	started, release chan struct{}
	once             sync.Once
}

func (w *slowMetricWriter) Header() http.Header { return w.header }
func (w *slowMetricWriter) WriteHeader(int)     {}
func (w *slowMetricWriter) Write(value []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(value), nil
}

func TestMetricReportingUnlocksBeforeSlowClientWrite(t *testing.T) {
	r := NewRecorder(nil)
	r.Pending("native", 1)
	w := &slowMetricWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.ServeMetrics("/metric/", w, httptest.NewRequest("GET", "/metric/operations", nil))
	}()
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("report did not reach client")
	}
	capture := make(chan struct{})
	go func() { r.Begin("lazy", time.Now())(time.Now(), "Success"); r.Pending("native", -1); close(capture) }()
	select {
	case <-capture:
	case <-time.After(time.Second):
		close(w.release)
		<-done
		t.Fatal("slow client held capture lock")
	}
	close(w.release)
	<-done
	require.Zero(t, r.Values("native")["Pending"])
	require.EqualValues(t, 1, r.Values("lazy")["Success"])
}

func TestMetricReportingConcurrentCaptureLazyRegistration(t *testing.T) {
	r := NewRecorder(nil)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				name := fmt.Sprintf("worker%d/op%d", index, i%4)
				r.Pending(name, 1)
				r.Begin(name, time.Now())(time.Now(), "Success")
				r.Pending(name, -1)
				res := httptest.NewRecorder()
				r.ServeMetrics("/metric/", res, httptest.NewRequest("GET", "/metric/operations", nil))
				if res.Code != 200 {
					t.Errorf("report status %d", res.Code)
				}
			}
		}(worker)
	}
	wg.Wait()
	for worker := 0; worker < 8; worker++ {
		for op := 0; op < 4; op++ {
			values := r.Values(fmt.Sprintf("worker%d/op%d", worker, op))
			require.Zero(t, values["Pending"])
			want := int64(12)
			if op < 2 {
				want = 13
			}
			require.Equal(t, want, values["Success"])
		}
	}
}

func TestMetricReportingExactLibraryWindowsAndSchema(t *testing.T) {
	p, key, view := policyFixture(t)
	r := NewRecorder(nil, WithPolicy(p))
	resolved, err := r.Resolve(key, view)
	require.NoError(t, err)
	// Existing gmetric semantics index by begun timestamp, including crossing a
	// bucket boundary. Use deterministic timestamps without waiting a minute.
	start := time.Now().Truncate(time.Minute).Add(-time.Millisecond)
	end := start.Add(2 * time.Millisecond)
	r.Begin(resolved.Operation, start)(end, "Success")
	baseline := gmetric.New()
	baseline.MultiOperationCounter("platform/delivery/advertiser", resolved.Operation, "datafees performance", time.Millisecond, time.Minute, 2, viewMetricProvider{source: true}).Begin(start)(end, "Success")
	for _, suffix := range []string{"operations", "operation/" + resolved.Operation, "operation/" + resolved.Operation + "/cumulative/Success", "operation/" + resolved.Operation + "/cumulative/Success.pct", "operation/" + resolved.Operation + "/recent/Success", "operation/" + resolved.Operation + "/recent", "operation/unknown", "operation/unknown/cumulative/Success", "operation/unknown/recent/Success", "counters", "counter/unknown"} {
		a, b := httptest.NewRecorder(), httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/metric/"+suffix, nil)
		r.ServeMetrics("/metric/", a, req)
		gmetric.NewHandler("/metric/", baseline).ServeHTTP(b, req)
		require.Equal(t, b.Code, a.Code, suffix)
		require.JSONEq(t, b.Body.String(), a.Body.String(), suffix)
	}
	res := httptest.NewRecorder()
	r.ServeMetrics("/metric/", res, httptest.NewRequest("GET", "/metric/operation/"+resolved.Operation, nil))
	var catalog struct {
		Counters                                []struct{ Value string }
		Recent                                  []struct{ Counters []struct{ Value string } }
		Unit, RecentUnit, Description, Location string
	}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &catalog))
	require.Len(t, catalog.Counters, 11)
	require.Len(t, catalog.Recent, 2)
	require.Equal(t, "1ms", catalog.Unit)
	require.Equal(t, "1m0s", catalog.RecentUnit)
	require.Equal(t, "datafees performance", catalog.Description)
	require.Equal(t, "platform/delivery/advertiser", catalog.Location)
	for i, key := range viewMetricKeys[:11] {
		require.Equal(t, key, catalog.Counters[i].Value)
		for _, bucket := range catalog.Recent {
			require.Len(t, bucket.Counters, 11)
			require.Equal(t, key, bucket.Counters[i].Value)
		}
	}
	r.Begin("native", start)(end, "Success")
	res = httptest.NewRecorder()
	r.ServeMetrics("/metric/", res, httptest.NewRequest("GET", "/metric/operation/native", nil))
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &catalog))
	require.Len(t, catalog.Counters, 14)
}
