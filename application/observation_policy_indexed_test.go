package application_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
)

type observationIndexGateKey struct{}
type observationIndexGate struct {
	started chan struct{}
	release chan struct{}
}
type observationIndexRow struct {
	ID int `sqlx:"id" json:"id"`
}

func (*observationIndexRow) OnFetch(ctx context.Context) error {
	gate, _ := ctx.Value(observationIndexGateKey{}).(*observationIndexGate)
	if gate == nil {
		return nil
	}
	gate.started <- struct{}{}
	select {
	case <-gate.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type observationIndexOutput struct {
	Rows []*observationIndexRow `json:"rows"`
}

func TestObservationDiscoveryTwoModulesLazyReloadSQLite(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/policyroot"}).Write(t, root)
	var dirs []string
	for _, name := range []string{"one", "two"} {
		dir := filepath.Join(root, name)
		dirs = append(dirs, dir)
		require.NoError(t, os.MkdirAll(dir, 0755))
		(testharness.GeneratedModule{Path: "example.com/policy" + name}).Write(t, dir)
		writeFixture(t, dir, "records/holder.go", `package records
import "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct { Read xdatly.Component[Input,Output] `+"`"+`component:"Records,path=/`+name+`,method=GET"`+"`"+` }
`)
	}
	buildIndex := func() *bootstrapindex.Snapshot {
		snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, ModuleDirs: dirs, Include: []string{"example.com/policyone/...", "example.com/policytwo/..."}}}).Build(ctx)
		require.NoError(t, err)
		return snapshot
	}
	snapshot := buildIndex()
	require.Len(t, snapshot.Entries(), 2)
	before, err := json.Marshal(snapshot.Entries())
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(11)"))
	view := &spec.View{Name: "records"}
	identity, err := view.Identity()
	require.NoError(t, err)
	policy := &observability.Policy{}
	for _, entry := range snapshot.Entries() {
		policy.Views = append(policy.Views, observability.ViewObservation{Component: entry.Key(), ViewIdentity: identity, Diagnostic: entry.Key().Scope + "/records#", Operation: &observability.OperationDescriptor{Name: "platform.shared.records", Location: "platform", Description: "records performance", Provider: observability.Source11}})
	}
	manager, err := application.New(nil, application.WithObservability(runtime.ObservabilityConfig{Policy: policy}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Shutdown(ctx)) })
	policy.Views[0].Diagnostic = "caller mutation"
	policy.Views[0].Operation.Name = "caller mutation"
	var mu sync.Mutex
	loads := map[string]int{}
	materializer := bootstrapindex.MaterializeFunc(func(_ context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
		mu.Lock()
		loads[entry.Key().String()]++
		mu.Unlock()
		component := entry.Component.Clone()
		component.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
		component.Parameters = []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
		a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[observationIndexOutput](), DirectViewField: "Rows"})
		if err != nil {
			return nil, err
		}
		e, err := a.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		return &bootstrapindex.Loaded{Registration: &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[observationIndexOutput](), Reader: e}}, err
	})
	compile := func(index *bootstrapindex.Snapshot, badHTTP string) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
			c := gateway.Config{Meta: gateway.Meta{MetricURI: "/metric"}}
			if badHTTP == "literal" {
				c.Meta.MetricURI = "/one"
			}
			if badHTTP == "static-root" {
				c.StaticContent = []*spec.StaticContent{{Path: "/", Namespace: "site"}}
			}
			return &application.Build{Index: index, Materializer: materializer, HTTP: c}, nil
		}
	}
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: compile(snapshot, "")}))
	require.Empty(t, loads, "unrelated native plans must stay lazy")
	metric := func() string {
		w := httptest.NewRecorder()
		manager.ServeHTTP(w, httptest.NewRequest("GET", "/metric/operations", nil))
		require.Equal(t, 200, w.Code, w.Body.String())
		return w.Body.String()
	}
	require.JSONEq(t, "null", metric(), "staging and reporting cannot create ghost operations")
	pinned, _, err := manager.Pin(ctx)
	require.NoError(t, err)
	defer mcpserver.Release(pinned)
	request := func(context context.Context, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		manager.ServeHTTP(w, httptest.NewRequest("GET", path, nil).WithContext(context))
		return w
	}
	w := request(pinned, "/one")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.JSONEq(t, `{"rows":[{"id":11}]}`, w.Body.String())
	require.NotContains(t, metric(), "caller mutation")
	mu.Lock()
	require.Len(t, loads, 1)
	mu.Unlock()
	after, err := json.Marshal(buildIndex().Entries())
	require.NoError(t, err)
	require.Equal(t, before, after, "discovery and public metadata must remain invariant")
	gate := &observationIndexGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	oldDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { oldDone <- request(context.WithValue(pinned, observationIndexGateKey{}, gate), "/one") }()
	select {
	case <-gate.started:
	case <-time.After(3 * time.Second):
		t.Fatal("old generation SQL did not block")
	}
	var live []struct {
		Name     string
		Count    int64
		Counters []struct {
			Value string
			Count int64
		}
	}
	require.NoError(t, json.Unmarshal([]byte(metric()), &live))
	require.Len(t, live, 1)
	require.EqualValues(t, 2, live[0].Count)
	require.EqualValues(t, 1, live[0].Counters[0].Count)
	require.EqualValues(t, 1, live[0].Counters[2].Count)
	require.NoError(t, manager.Reload(ctx, application.Request{Revision: 2, Compile: compile(buildIndex(), "")}))
	require.NoError(t, json.Unmarshal([]byte(metric()), &live))
	require.EqualValues(t, 2, live[0].Count)
	require.EqualValues(t, 1, live[0].Counters[2].Count)
	require.Error(t, manager.Reload(ctx, application.Request{Revision: 3, Compile: compile(buildIndex(), "literal")}))
	require.EqualValues(t, 2, manager.Revision())
	// Rejected generations must retain the actual old pending SQL operation,
	// reporting catalog and lazy materialization state without ghost counters.
	catalogBeforeRejected := metric()
	mu.Lock()
	loadsBeforeRejected := make(map[string]int, len(loads))
	for name, count := range loads {
		loadsBeforeRejected[name] = count
	}
	mu.Unlock()
	components := make([]*spec.Component, 0, len(snapshot.Entries()))
	for i, entry := range snapshot.Entries() {
		component := entry.Component.Clone()
		if i == 0 {
			component.Routes[0].Path = "/{tenant}/operation/special"
		}
		components = append(components, component)
	}
	intersectingIndex, err := bootstrapindex.BuildLinked([]string{"example.com/policyone", "example.com/policytwo"}, components)
	require.NoError(t, err)
	require.ErrorContains(t, manager.Reload(ctx, application.Request{Revision: 4, Compile: compile(intersectingIndex, "")}), "metric reporting")
	require.ErrorContains(t, manager.Reload(ctx, application.Request{Revision: 5, Compile: compile(buildIndex(), "static-root")}), "metric reporting")
	require.EqualValues(t, 2, manager.Revision())
	// Report elapsed windows vary with time, so compare ownership/lifecycle
	// values rather than a serialized time-dependent reporting response.
	var rejectedLive []struct {
		Name     string
		Count    int64
		Counters []struct {
			Value string
			Count int64
		}
	}
	require.NoError(t, json.Unmarshal([]byte(catalogBeforeRejected), &rejectedLive))
	var retainedLive []struct {
		Name     string
		Count    int64
		Counters []struct {
			Value string
			Count int64
		}
	}
	require.NoError(t, json.Unmarshal([]byte(metric()), &retainedLive))
	require.Equal(t, rejectedLive, retainedLive)
	mu.Lock()
	require.Equal(t, loadsBeforeRejected, loads)
	mu.Unlock()
	var wg sync.WaitGroup
	for _, pair := range []struct {
		ctx  context.Context
		path string
	}{{pinned, "/two"}, {ctx, "/one"}, {ctx, "/two"}} {
		wg.Add(1)
		go func(c context.Context, path string) {
			defer wg.Done()
			w := request(c, path)
			require.Equal(t, 200, w.Code, w.Body.String())
		}(pair.ctx, pair.path)
	}
	wg.Wait()
	close(gate.release)
	prior := <-oldDone
	require.Equal(t, 200, prior.Code, prior.Body.String())
	var catalog []struct {
		Name     string
		Count    int64
		Counters []struct {
			Value string
			Count int64
		}
	}
	require.NoError(t, json.Unmarshal([]byte(metric()), &catalog))
	require.Len(t, catalog, 1)
	require.Equal(t, "platform.shared.records", catalog[0].Name)
	require.Len(t, catalog[0].Counters, 11)
	require.Equal(t, "Success", catalog[0].Counters[0].Value)
	require.EqualValues(t, 5, catalog[0].Counters[0].Count)
	require.Zero(t, catalog[0].Counters[2].Count)
}
