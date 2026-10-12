package http

import (
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	druntime "github.com/viant/datly/runtime"
	rroute "github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
)

func TestMetricReportingOptionalHTTPPolicySQLite(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings = nil
	identity, err := f.component.RootView.Identity()
	require.NoError(t, err)
	policy := &observability.Policy{Views: []observability.ViewObservation{{Component: f.component.Key, ViewIdentity: identity, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.records", Location: "platform", Description: "records performance", Provider: observability.Source11}}}}
	rt := f.runtime(t, druntime.WithObservability(druntime.ObservabilityConfig{Policy: policy}))
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	send := func(h *Handler, method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	disabled, err := (Config{}).NewHandler(rt, nil, "")
	require.NoError(t, err)
	require.Equal(t, 404, send(disabled, "GET", "/v1/api/meta/metric/operations").Code)
	active, err := (Config{Meta: Meta{MetricURI: "/v1/api/meta/metric"}}).NewHandler(rt, nil, "")
	require.NoError(t, err)
	read := send(active, "GET", "/api/records?tenant=1")
	require.Equal(t, 200, read.Code, read.Body.String())
	require.Empty(t, read.Header().Get("Datly-Metrics"), "reporting cannot enable diagnostics")
	for name := range read.Header() {
		require.False(t, strings.HasPrefix(name, "Datly-Metrics-"), "default per-view diagnostic headers remain disabled")
	}
	explicit, err := (Config{Meta: Meta{MetricURI: "/v1/api/meta/metric"}, Metrics: &MetricsConfig{}}).NewHandler(rt, nil, "")
	require.NoError(t, err)
	request := httptest.NewRequest("GET", "/api/records?tenant=1", nil)
	request.Header.Set("Datly-Show-Metrics", "true")
	diagnostics := httptest.NewRecorder()
	explicit.ServeHTTP(diagnostics, request)
	require.Equal(t, 200, diagnostics.Code, diagnostics.Body.String())
	var diagnosticCount int
	for name, values := range diagnostics.Header() {
		if !strings.HasPrefix(name, "Datly-Metrics-") {
			continue
		}
		diagnosticCount++
		var metric response.Metric
		require.NoError(t, json.Unmarshal([]byte(values[0]), &metric))
		require.Equal(t, "records#", metric.View)
		for _, execution := range metric.Executions {
			require.Empty(t, execution.SQL)
			require.Empty(t, execution.Args)
		}
	}
	require.Equal(t, 1, diagnosticCount)
	require.EqualValues(t, 2, rt.Observability().Recorder.Values("platform.records")["Success"], "explicit diagnostic headers retain the same actual source owner")
	for _, suffix := range []string{"operations", "operation/platform.records", "operation/platform.records/cumulative/Success", "operation/platform.records/recent/Success", "operation/platform.records/recent", "counters", "counter/unknown"} {
		w := send(active, "GET", "/v1/api/meta/metric/"+suffix)
		require.Equal(t, 200, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), "SELECT")
		require.NotContains(t, w.Body.String(), "tenant")
		require.Empty(t, w.Header().Get("Datly-Metrics"))
	}
	require.Equal(t, 405, send(active, "POST", "/v1/api/meta/metric/operations").Code)
	require.Equal(t, 404, send(active, "GET", "/v1/api/meta/metric/unknown").Code)
	blocked, err := (Config{Meta: Meta{MetricURI: "/v1/api/meta/metric", AllowedSubnet: []string{"127.0.0.1"}}}).NewHandler(rt, nil, "")
	require.NoError(t, err)
	require.Equal(t, 403, send(blocked, "GET", "/v1/api/meta/metric/operations").Code)
	for _, prefix := range []string{"/api/records", "/v1/api/cache/warmup", "/v1/api/meta/openapi", "bad", "/metric?bad=1"} {
		_, err := (Config{Meta: Meta{MetricURI: prefix}}).NewHandler(rt, nil, "")
		require.Error(t, err, prefix)
	}
}

func TestMetricReportingDeclaredLocationSQLite(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings = nil
	identity, err := f.component.RootView.Identity()
	require.NoError(t, err)
	policy := &observability.Policy{Views: []observability.ViewObservation{{Component: f.component.Key, ViewIdentity: identity, Operation: &observability.OperationDescriptor{Name: "mdp.deal.deal", Location: "mdp/deal", Description: "deal performance", Provider: observability.Basic2}}}}
	rt := f.runtime(t, druntime.WithObservability(druntime.ObservabilityConfig{Policy: policy}))
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	h, err := (Config{Meta: Meta{MetricURI: "/v1/api/meta/metric"}}).NewHandler(rt, nil, "")
	require.NoError(t, err)
	send := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	prefix := "/v1/api/meta/metric/mdp/deal/"
	before := send("GET", prefix+"operations")
	require.Equal(t, 200, before.Code, before.Body.String())
	require.JSONEq(t, "null", before.Body.String(), "staging reporting paths must not register counters")
	require.Equal(t, 200, send("GET", "/api/records?tenant=1").Code)
	rt.Observability().Recorder.Begin("native/unrelated", time.Now())(time.Now(), "Success")
	for _, suffix := range []string{"operations", "operation/mdp.deal.deal", "operation/mdp.deal.deal/cumulative/error", "operation/mdp.deal.deal/recent/pending", "operation/mdp.deal.deal/recent", "counters", "counter/unknown"} {
		w := send("GET", prefix+suffix)
		require.Equal(t, 200, w.Code, w.Body.String())
	}
	var filtered []struct {
		Name     string
		Count    int64
		Counters []struct {
			Value string
			Count int64
		}
	}
	require.NoError(t, json.Unmarshal(send("GET", prefix+"operations").Body.Bytes(), &filtered))
	require.Len(t, filtered, 1)
	require.Equal(t, "mdp.deal.deal", filtered[0].Name)
	require.EqualValues(t, 1, filtered[0].Count)
	require.Len(t, filtered[0].Counters, 2)
	require.Equal(t, "error", filtered[0].Counters[0].Value)
	require.Equal(t, "pending", filtered[0].Counters[1].Value)
	require.Zero(t, filtered[0].Counters[0].Count)
	require.Zero(t, filtered[0].Counters[1].Count)
	global := send("GET", "/v1/api/meta/metric/operations")
	require.Contains(t, global.Body.String(), "native/unrelated")
	require.Equal(t, 405, send("POST", prefix+"operations").Code)
	require.Equal(t, 404, send("GET", "/v1/api/meta/metric/not-declared/operations").Code)
}

func TestMetricReportingDeclaredLocationRejectsTemplateCollisionSQLite(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings = nil
	f.component.Routes[0].Path = "/{root}/{module}/{resource}/operations"
	identity, err := f.component.RootView.Identity()
	require.NoError(t, err)
	policy := &observability.Policy{Views: []observability.ViewObservation{{Component: f.component.Key, ViewIdentity: identity, Operation: &observability.OperationDescriptor{Name: "records", Location: "mdp/deal", Description: "records performance", Provider: observability.Basic2}}}}
	rt := f.runtime(t, druntime.WithObservability(druntime.ObservabilityConfig{Policy: policy}))
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	_, err = (Config{Meta: Meta{MetricURI: "/metric"}}).NewHandler(rt, nil, "")
	require.ErrorContains(t, err, "metric reporting route collides")
	require.Empty(t, rt.Observability().Recorder.Values("records"))
}

func TestMetricReportingRejectsNativeTemplateIntersectionSQLite(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings = nil
	f.component.Routes[0].Path = "/{tenant}/operation/special"
	rt := f.runtime(t)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	var authorizationCalls int
	c := Config{Meta: Meta{MetricURI: "/metric"}, Authorize: func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error {
		authorizationCalls++
		return errors.New("protected component")
	}}
	h, err := c.NewHandler(rt, nil, "")
	if err == nil {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/metric/operation/special?tenant=1", nil))
		t.Errorf("intersecting reporting path reached status%d with component authorization calls%d; staging must reject", w.Code, authorizationCalls)
	}
	require.ErrorContains(t, err, "metric reporting")
	require.Empty(t, rt.Observability().Recorder.Values(f.component.Key.String()+"/records"), "failed staging cannot create a counter")
}

func TestMetricReportingRejectsStaticRoot(t *testing.T) {
	rt, err := druntime.NewRuntime(nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	_, err = (Config{Meta: Meta{MetricURI: "/metric"}, StaticContent: []*spec.StaticContent{{Path: "/", Namespace: "site"}}}).metricPrefix(HandlerInput{Runtime: rt})
	require.ErrorContains(t, err, "metric reporting namespace collides with static route /")
}

func TestMetricNativeTemplateIntersectionExhaustive(t *testing.T) {
	for _, suffix := range []string{"operations", "counters", "operation/{name}", "counter/{name}", "operation/{name}/recent", "operation/{name}/recent/{metric}", "operation/{name}/cumulative/{metric}"} {
		t.Run(suffix, func(t *testing.T) {
			reporting, err := rroute.CompilePathTemplate("/metric/" + suffix)
			require.NoError(t, err)
			// Arbitrary names and metrics expose the former Success-only probe gap.
			concrete := strings.ReplaceAll(strings.ReplaceAll(suffix, "{name}", "special"), "{metric}", "OtherMetric")
			for _, path := range []string{"/{tenant}/" + concrete, "/{tenant}/" + concrete + "/", "/{tenant}/" + suffix} {
				component, err := rroute.CompilePathTemplate(path)
				require.NoError(t, err)
				intersects, err := metricTemplatesIntersect(component, reporting)
				require.NoError(t, err)
				require.True(t, intersects, path)
			}
			component, err := rroute.CompilePathTemplate("/private/" + concrete)
			require.NoError(t, err)
			intersects, err := metricTemplatesIntersect(component, reporting)
			require.NoError(t, err)
			require.False(t, intersects)
		})
	}
	for _, pair := range [][2]string{{"/{tenant}/operation/special value", "/metric/operation/{name}"}, {"/{tenant}/operation/special%value", "/metric/operation/{name}"}} {
		first, err := rroute.CompilePathTemplate(pair[0])
		require.NoError(t, err)
		second, err := rroute.CompilePathTemplate(pair[1])
		require.NoError(t, err)
		matched, err := metricTemplatesIntersect(first, second)
		require.NoError(t, err)
		require.True(t, matched)
	}
}
