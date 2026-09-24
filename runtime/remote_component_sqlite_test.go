package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/handler/custom"
	cacheprovider "github.com/viant/datly/runtime/handler/provider/cache"
	"github.com/viant/datly/runtime/handler/remote"
	"github.com/viant/datly/runtime/registry"
	remotecore "github.com/viant/datly/runtime/remote"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xcache "github.com/viant/xdatly/cache"
	xpredicate "github.com/viant/xdatly/predicate"
)

// The shipped contract's typed input uses the same native binder as custom
// application contracts; this test exercises it through a component route.
type remoteAuthInput = remote.Input

// remoteAuthContext is an ordinary user-declared output; the runtime never
// interprets its field names.
type remoteAuthContext struct {
	UserID          int
	Roles           []string
	AllowedEntities map[string][]int
}

type remoteAuthOutput struct {
	Context *remoteAuthContext
}

type remoteGatedInput struct {
	Auth    *remoteAuthOutput  `parameter:"Auth,kind=component,in=GET:/remote-auth,required"`
	Context *remoteAuthContext `parameter:"Context,kind=param,in=Auth.Context,required" predicate:"handler,example.RemoteEntityPredicate"`
}

type remoteEntityPredicate struct{}

func (p *remoteEntityPredicate) Compute(_ context.Context, value any) (*xpredicate.Criteria, error) {
	auth, ok := value.(*remoteAuthContext)
	if !ok || auth == nil {
		return nil, fmt.Errorf("authorization context missing")
	}
	ids := auth.AllowedEntities["project"]
	if len(ids) == 0 {
		return &xpredicate.Criteria{Expression: "1 = 0"}, nil
	}
	placeholders := make([]string, len(ids))
	values := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i], values[i] = "?", id
	}
	return &xpredicate.Criteria{Expression: "id IN (" + strings.Join(placeholders, ",") + ")", Placeholders: values}, nil
}

func remoteAuthConfig(url string) string {
	return `{"client":{"transport":"http","http":{"url":"` + url + `/whoami","method":"GET"}},` +
		`"request":[{"input":"Token","header":"Authorization"}],` +
		`"response":{"mappings":[{"path":"/user/id","output":"Context.UserID"},{"path":"/user/roles","output":"Context.Roles"},{"path":"/user/allowedEntities","output":"Context.AllowedEntities"}]},` +
		`"cache":{"name":"auth","ttl":"5m","partition":["Token"]}}`
}

func TestRemoteHandlerFeedsNativeComponentDependencyAndPredicateSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE projects (id INTEGER)", "INSERT INTO projects VALUES (101), (102), (103), (104)"); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	authServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		switch request.Header.Get("Authorization") {
		case "Bearer alice":
			_, _ = writer.Write([]byte(`{"user":{"id":1,"roles":["admin"],"allowedEntities":{"project":[101,102],"organization":[7]}}}`))
		case "Bearer bob":
			_, _ = writer.Write([]byte(`{"user":{"id":2,"roles":["viewer"],"allowedEntities":{"project":[103]}}}`))
		default:
			writer.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(authServer.Close)

	configValue := remoteAuthConfig(authServer.URL)
	authComponent := componentSpec("RemoteAuth", "GET", "/remote-auth", []*spec.Parameter{
		{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: boolValue(true)},
		{Name: "Remote", Source: spec.BindSource{Kind: "const", Name: "Remote"}, Value: &configValue},
	})
	authArtifact := componentArtifact(t, authComponent, reflect.TypeOf(remoteAuthInput{}), reflect.TypeOf(remoteAuthOutput{}))

	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	consumer := componentSpec("RemoteProjects", "GET", "/remote-projects", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	consumer.RootView = &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM projects ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(remoteEntityPredicate{}), PkgPath: "example", Name: "RemoteEntityPredicate"}); err != nil {
		t.Fatal(err)
	}
	consumerArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: consumer, InputType: reflect.TypeOf(remoteGatedInput{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data", Types: types})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: consumerArtifact.Component, InputType: reflect.TypeOf(remoteGatedInput{}), OutputType: reflect.TypeOf(output{}), Plan: consumerArtifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	mapper := remotecore.NewMapper(remotecore.WithClock(func() time.Time { return now }))
	authContract := &remote.Handler[remoteAuthOutput]{}
	authHandler := custom.New(authContract)
	backend, err := cacheprovider.NewMemory(0)
	if err != nil {
		t.Fatal(err)
	}
	cacheRegistry, err := cacheprovider.New(map[string]xcache.Cache{"auth": backend})
	if err != nil {
		t.Fatal(err)
	}
	cacheConfig, err := remotecore.DecodeConfig([]byte(configValue))
	if err != nil {
		t.Fatal(err)
	}
	// No per-component providers: the runtime's default client composition
	// supplies the HTTP provider and owns its lifecycle.
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: authArtifact.Component, Input: authArtifact.Input, OutputType: reflect.TypeOf(remoteAuthOutput{}), Handler: authHandler, Providers: cacheprovider.Providers(cacheRegistry)},
		{Component: consumerArtifact.Component, Input: consumerArtifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader},
	}, WithRemoteMapper(mapper))
	if err != nil {
		t.Fatal(err)
	}
	if authContract.Mapper != mapper || authContract.HTTP == nil || authContract.Cache == nil {
		t.Fatalf("remote handler static dependencies were not bound at registration: %#v", authContract)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	projects := func(t *testing.T, header string, query url.Values, wantCalls int64) ([]row, error) {
		t.Helper()
		request := testharness.NewRequest(http.MethodGet, "/remote-projects").WithQuery(query)
		if header != "" {
			request = request.WithHeaders(http.Header{"Authorization": {header}})
		}
		actual, err := executeTestRoute(t, runtime, ctx, request)
		if calls.Load() != wantCalls {
			t.Fatalf("remote calls = %d, want %d", calls.Load(), wantCalls)
		}
		if err != nil {
			return nil, err
		}
		return actual.(*output).Data, nil
	}
	rows, err := projects(t, "Bearer alice", nil, 1)
	if err != nil || !reflect.DeepEqual(rows, []row{{101}, {102}}) {
		t.Fatalf("alice rows = %#v err = %v", rows, err)
	}
	if rows, err = projects(t, "Bearer alice", nil, 1); err != nil || !reflect.DeepEqual(rows, []row{{101}, {102}}) {
		t.Fatalf("cached alice rows = %#v err = %v", rows, err)
	}
	if rows, err = projects(t, "Bearer bob", nil, 2); err != nil || !reflect.DeepEqual(rows, []row{{103}}) {
		t.Fatalf("bob rows = %#v err = %v", rows, err)
	}
	if _, err = projects(t, "", nil, 2); err == nil {
		t.Fatal("missing credential was accepted without invoking the remote")
	}
	if _, err = projects(t, "Bearer mallory", nil, 3); err == nil {
		t.Fatal("rejected credential produced rows")
	}
	// Caller-supplied fake authorization facts never replace the component-bound input.
	fake := url.Values{"Auth": {`{"Context":{"UserID":9,"AllowedEntities":{"project":[104]}}}`}, "Context": {`{"UserID":9,"AllowedEntities":{"project":[104]}}`}, "AllowedEntities": {`{"project":[104]}`}}
	if rows, err = projects(t, "Bearer alice", fake, 3); err != nil || !reflect.DeepEqual(rows, []row{{101}, {102}}) {
		t.Fatalf("fake auth rows = %#v err = %v", rows, err)
	}
	// Caller attempts to override the remote declaration are ignored by
	// canonical const authority: the served entry proves identical configuration.
	override := url.Values{"Remote": {`{"client":{"transport":"http","http":{"url":"http://127.0.0.1:9/evil","method":"GET"}},"response":{"mappings":[{"path":"/user/id","output":"UserID"}]}}`}}
	if rows, err = projects(t, "Bearer alice", override, 3); err != nil || !reflect.DeepEqual(rows, []row{{101}, {102}}) {
		t.Fatalf("override rows = %#v err = %v", rows, err)
	}
	direct := testharness.NewRequest(http.MethodGet, "/remote-auth").WithQuery(override).WithHeaders(http.Header{"Authorization": {"Bearer bob"}, "Remote": {override.Get("Remote")}})
	actual, err := executeTestRoute(t, runtime, ctx, direct)
	if err != nil {
		t.Fatal(err)
	}
	authContext := actual.(*remoteAuthOutput).Context
	if authContext == nil || authContext.UserID != 2 || !reflect.DeepEqual(authContext.AllowedEntities, map[string][]int{"project": {103}}) || calls.Load() != 3 {
		t.Fatalf("direct auth output = %#v calls = %d", authContext, calls.Load())
	}
	if removed, err := mapper.Invalidate(ctx, cacheConfig, cacheRegistry, "Bearer bob"); err != nil || removed != 1 {
		t.Fatalf("invalidated %d entries: %v", removed, err)
	}
	now = now.Add(5 * time.Minute)
	if rows, err = projects(t, "Bearer alice", nil, 4); err != nil || !reflect.DeepEqual(rows, []row{{101}, {102}}) {
		t.Fatalf("expired alice rows = %#v err = %v", rows, err)
	}
	if rows, err = projects(t, "Bearer bob", nil, 5); err != nil || !reflect.DeepEqual(rows, []row{{103}}) {
		t.Fatalf("bob rows after invalidation = %#v err = %v", rows, err)
	}
}

func TestRemoteHandlerRejectsMalformedConstBeforeNetwork(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"user":{"id":1}}`))
	}))
	defer server.Close()
	valid := remoteAuthConfig(server.URL)
	broken := strings.Replace(valid, `"output":"Context.Roles"`, `"output":"Context.Missing"`, 1)
	component := componentSpec("RemoteAuth", "GET", "/remote-auth", []*spec.Parameter{
		{Name: "Remote", Source: spec.BindSource{Kind: "const", Name: "Remote"}, Value: &broken},
	})
	artifact := componentArtifact(t, component, reflect.TypeFor[remoteAuthInput](), reflect.TypeFor[remoteAuthOutput]())
	handler := custom.New(&remote.Handler[remoteAuthOutput]{})
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[remoteAuthOutput](), Handler: handler}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	_, err = executeTestRoute(t, rt, context.Background(), testharness.NewRequest(http.MethodGet, "/remote-auth").WithHeaders(http.Header{"Authorization": {"Bearer alice"}}))
	if err == nil || !strings.Contains(err.Error(), `Context.Missing`) {
		t.Fatalf("malformed constant result: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("malformed mapping invoked remote %d times", calls.Load())
	}
}

func TestRemoteHandlerBindsOnlySelectedClientProvider(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer alice" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"user":{"id":7}}`))
	}))
	defer server.Close()
	config := `{"client":{"transport":"http","http":{"url":"` + server.URL + `","method":"GET"}},` +
		`"request":[{"input":"Token","header":"Authorization"}],` +
		`"response":{"mappings":[{"path":"/user/id","output":"Context.UserID"}]}}`
	component := componentSpec("RemoteHTTPOnly", "GET", "/remote-http-only", []*spec.Parameter{
		{Name: "Remote", Source: spec.BindSource{Kind: "const", Name: "Remote"}, Value: &config},
	})
	artifact := componentArtifact(t, component, reflect.TypeFor[remoteAuthInput](), reflect.TypeFor[remoteAuthOutput]())
	httpProvider := &countingHTTPProvider{client: server.Client()}
	handler := &remote.Handler[remoteAuthOutput]{}
	rt, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[remoteAuthOutput](),
		Handler: custom.New(handler),
	}}, WithClientProviders(httpProvider, nil))
	if err != nil {
		t.Fatal(err)
	}
	if handler.Mapper == nil || handler.HTTP == nil || handler.MCP != nil {
		t.Fatalf("selected static client binding = mapper:%v http:%v mcp:%v", handler.Mapper != nil, handler.HTTP != nil, handler.MCP != nil)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	actual, err := executeTestRoute(t, rt, context.Background(), testharness.NewRequest(http.MethodGet, "/remote-http-only").WithHeaders(http.Header{"Authorization": {"Bearer alice"}}))
	if err != nil {
		t.Fatal(err)
	}
	output := actual.(*remoteAuthOutput)
	if output.Context == nil || output.Context.UserID != 7 || httpProvider.calls.Load() != 1 {
		t.Fatalf("output=%#v provider calls=%d", output, httpProvider.calls.Load())
	}
}

func boolValue(value bool) *bool { return &value }
