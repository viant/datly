package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xexec "github.com/viant/xdatly/exec"
)

type tracedWarmupInput struct {
	Tenant int
	Auth   *configOutput `parameter:"Auth,kind=component,in=GET:/auth"`
}

type tracedWarmupAuthInput struct {
	User *configOutput `parameter:"User,kind=component,in=GET:/user"`
}

// Exercise real binding-time child readers, multiple cache cases, and the
// application recorder rather than only checking an execution constructor.
func TestHTTPWarmupTraceSummaries(t *testing.T) {
	f := newConfigFixture(t)
	var logs testharness.Output
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	var registered []*druntime.RegisteredComponent
	add := func(component *spec.Component, input reflect.Type) {
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: input, OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows"})
		require.NoError(t, err)
		reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
		require.NoError(t, err)
		registered = append(registered, &druntime.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[configOutput](), Reader: reader})
	}
	for _, name := range []string{"user", "auth"} {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: name}, Routes: []*spec.Route{{Method: "GET", Path: "/" + name}}, RootView: &spec.View{Name: name, Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=1"}}}
		input := reflect.TypeFor[struct{}]()
		if name == "auth" {
			input = reflect.TypeFor[tracedWarmupAuthInput]()
			component.Settings = &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: name, Location: t.TempDir(), TTL: "1m"}}
		}
		add(component, input)
	}
	add(f.component, reflect.TypeFor[tracedWarmupInput]())
	rt, err := druntime.NewRuntime(registered, druntime.WithObservability(druntime.ObservabilityConfig{Logger: logger}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	config := warmupConfig()
	var admitted *xexec.Context
	var completed WarmupResult
	var completionError error
	config.Warmup.Completed = func(result WarmupResult, err error) {
		completed, completionError = result, err
	}
	authorize := config.Warmup.Authorize
	config.Warmup.Authorize = func(ctx context.Context, req *stdhttp.Request, target dexec.ComponentTarget) error {
		admitted = xexec.GetContext(ctx)
		if admitted != nil {
			require.NotContains(t, logs.String(), admitted.TraceID, "start must follow administrator authorization")
		}
		return authorize(ctx, req, target)
	}
	h, err := config.NewHandler(rt, logger, "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Shutdown(context.Background())) })
	var previous string
	for attempt := 0; attempt < 2; attempt++ {
		before := len(logs.String())
		req := httptest.NewRequest("POST", "/admin/warm/records", nil)
		req.Header.Set("X-Admin", "admin")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		require.Equal(t, 200, res.Code, res.Body.String())
		require.NotNil(t, admitted)
		require.NotEmpty(t, admitted.TraceID)
		require.Equal(t, admitted.TraceID, completed.TraceID)
		require.NoError(t, completionError)
		require.Positive(t, completed.Elapsed)
		require.NotContains(t, res.Body.String(), "TraceID")
		require.NotContains(t, res.Body.String(), "elapsed")
		require.NotContains(t, res.Body.String(), admitted.TraceID)
		_, err := uuid.Parse(admitted.TraceID)
		require.NoError(t, err)
		require.NotEqual(t, previous, admitted.TraceID)
		previous = admitted.TraceID
		views := map[string]int{}
		cacheReads := 0
		starts := 0
		for _, line := range strings.Split(strings.TrimSpace(logs.String()[before:]), "\n") {
			var record map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &record))
			switch record["msg"] {
			case "datly cache warmup started":
				require.Equal(t, admitted.TraceID, record["reqTraceId"])
				require.Equal(t, completed.Target, record["target"])
				require.Empty(t, views, "start must precede preparation's child reads")
				starts++
			case "datly view read":
				require.Equal(t, admitted.TraceID, record["reqTraceId"])
				views[record["view"].(string)]++
				require.Contains(t, record, "rows")
				require.NotEmpty(t, record["elapsed"])
				require.Equal(t, "ok", record["status"])
			case "datly cache read":
				require.Equal(t, admitted.TraceID, record["reqTraceId"])
				cacheReads++
			}
		}
		require.Equal(t, 2, views["records"], "both warmup cases must share the operation trace")
		require.GreaterOrEqual(t, views["auth"], 4, "preparation and fill both bind Auth")
		require.GreaterOrEqual(t, views["user"], 4, "nested UserContext reads retain correlation")
		require.Positive(t, cacheReads)
		require.Equal(t, 1, starts)
	}
	// Binding the UserContext dependency fails during preparation, before any
	// root warmup write. Its error and the completion callback still correlate.
	require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE records"))
	before := len(logs.String())
	req := httptest.NewRequest("POST", "/admin/warm/records", nil)
	req.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	require.Equal(t, 500, res.Code)
	require.Error(t, completionError)
	require.Equal(t, "rejected", completed.Status)
	require.Zero(t, completed.Groups)
	require.NotEmpty(t, completed.TraceID)
	require.NotEqual(t, previous, completed.TraceID)
	require.Positive(t, completed.Elapsed)
	failures := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()[before:]), "\n") {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		switch record["msg"] {
		case "datly cache warmup started", "datly view read", "datly SQL read failed", "HTTP cache warmup failed":
			require.Equal(t, completed.TraceID, record["reqTraceId"])
			failures[record["msg"].(string)] = true
		}
	}
	require.True(t, failures["datly view read"])
	require.True(t, failures["datly cache warmup started"], "preparation failures must have a start event")
	require.True(t, failures["HTTP cache warmup failed"])
}

func TestHTTPWarmupTraceIsolation(t *testing.T) {
	type callerKey struct{}
	f := newConfigFixture(t)
	rt := f.runtime(t)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	config := warmupConfig()
	parent := xexec.New(xexec.WithMethod("parent"), xexec.WithTraceResource("parent", "test"))
	parent.SetValue("private", "caller state")
	parent.Complete(time.Now(), nil)
	parentBefore := parent.SnapshotForLogging()
	// Even an execution context on the server lifetime must not be reused.
	config.Warmup.Lifetime = NewWarmupLifetime(xexec.WithContext(context.Background(), parent))
	arrived := make(chan context.Context, 2)
	release := make(chan struct{})
	defer close(release)
	denied := errors.New("test admission denied")
	config.Warmup.Authorize = func(ctx context.Context, req *stdhttp.Request, _ dexec.ComponentTarget) error {
		if xexec.GetContext(req.Context()) != xexec.GetContext(ctx) {
			return errors.New("snapshot lost execution context")
		}
		arrived <- ctx
		select {
		case <-release:
		case <-ctx.Done():
		}
		return denied
	}
	completed := make(chan WarmupResult, 2)
	config.Warmup.Completed = func(result WarmupResult, _ error) { completed <- result }
	h, err := config.NewHandler(rt, nil, "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Shutdown(context.Background())) })
	caller, cancel := context.WithCancel(context.WithValue(xexec.WithContext(context.Background(), parent), callerKey{}, "private"))
	defer cancel()
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			req := httptest.NewRequest("POST", "/admin/warm/records?secret=not-retained", nil).WithContext(caller)
			req.Header.Set("X-Request-ID", "untrusted-same-id")
			h.ServeHTTP(httptest.NewRecorder(), req)
		}()
	}
	executions := map[*xexec.Context]bool{}
	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case ctx := <-arrived:
			ec := xexec.GetContext(ctx)
			require.NotNil(t, ec)
			require.NotSame(t, parent, ec)
			require.NotSame(t, parent.Trace, ec.Trace)
			require.NotEmpty(t, ec.TraceID)
			require.Equal(t, ec.TraceID, ec.Trace.TraceID)
			require.NotEqual(t, parent.TraceID, ec.TraceID)
			require.NotEqual(t, "untrusted-same-id", ec.TraceID)
			require.Equal(t, "POST", ec.Method)
			require.Equal(t, "/admin/warm/records", ec.URI)
			require.Empty(t, ec.Header)
			_, found := ec.Value("private")
			require.False(t, found)
			require.Nil(t, ctx.Value(callerKey{}))
			ids[ec.TraceID], executions[ec] = true, true
		case <-time.After(5 * time.Second):
			t.Fatal("warmup did not reach authorization")
		}
	}
	require.Len(t, ids, 2)
	require.Len(t, executions, 2)
	cancel()
	require.NoError(t, h.Shutdown(context.Background()))
	for i := 0; i < 2; i++ {
		<-done
		result := <-completed
		require.True(t, ids[result.TraceID])
		delete(ids, result.TraceID)
	}
	require.Empty(t, ids)
	require.Equal(t, parentBefore, parent.SnapshotForLogging())
}
