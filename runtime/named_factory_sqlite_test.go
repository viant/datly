package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/report"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/mutation"
	"github.com/viant/datly/runtime/registry"
	fixture "github.com/viant/datly/runtime/testdata/namedfactory"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/x"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type namedFactoryMCPContext struct{}

func (namedFactoryMCPContext) Client() xmcp.Client { return nil }

func TestLinkedNamedFactoriesUseProductionProjectAssemblySQLite(t *testing.T) {
	for _, mcpContext := range []bool{false, true} {
		t.Run(fmt.Sprintf("MCP=%v", mcpContext), func(t *testing.T) {
			ctx := context.Background()
			if mcpContext {
				ctx = xmcp.WithContext(ctx, namedFactoryMCPContext{})
			}
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			exports := x.NewRegistry()
			pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
			calls := map[string]int{}
			for _, factory := range []struct {
				name   string
				create func() (rhandler.TypedHandler, error)
			}{{"NewRegular", custom.Factory(fixture.NewRegular)}, {"NewMutation", mutation.Factory(fixture.NewMutation)}} {
				factory := factory
				function, err := x.NewFunction(pkg, factory.name, func() (rhandler.TypedHandler, error) { calls[factory.name]++; return factory.create() })
				if err != nil {
					t.Fatal(err)
				}
				if err = exports.RegisterFunctions(function); err != nil {
					t.Fatal(err)
				}
			}
			var inputs []bootstrap.ArtifactInput
			holder := reflect.TypeOf(fixture.Component{})
			for index := 0; index < holder.NumField(); index++ {
				field := holder.Field(index)
				tag, _, err := dtag.ParseComponent(field.Tag)
				if err != nil {
					t.Fatal(err)
				}
				source := &bootstrap.RouteSource{PackagePath: pkg, PackageName: "namedfactory", HolderType: "Component", FieldName: field.Name, Tag: tag, InputType: "Input", OutputType: "Output"}
				component, err := source.Resolve(reflect.TypeOf(fixture.Input{}), reflect.TypeOf(fixture.Output{}))
				if err != nil {
					t.Fatal(err)
				}
				inputs = append(inputs, bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})})
			}
			compiled, err := report.NewProjectCompiler(report.ProjectConfig{Registry: exports}).CompileArtifacts(inputs)
			if err != nil {
				t.Fatal(err)
			}
			components, err := compiled.RuntimeComponents(ctx, report.RuntimeConfigureFunc(func(context.Context, *report.ComponentArtifact) (report.RuntimeCapabilities, error) {
				return report.RuntimeCapabilities{DataSource: dml.Source{DB: h.DB}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := druntime.NewRuntime(components)
			if err != nil {
				t.Fatal(err)
			}
			for index, route := range []string{"regular", "mutation"} {
				request := testharness.NewRequest("POST", "/"+route).WithBody([]byte(fmt.Sprintf(`{"data":{"id":%d,"name":"value"}}`, index+1)), "application/json")
				scope, err := request.Scope()
				if err != nil {
					t.Fatal(err)
				}
				actual, err := runtime.ExecuteRoute(ctx, "POST", "/"+route, scope)
				scope.Close()
				if err != nil {
					t.Fatal(err)
				}
				output := actual.(*fixture.Output)
				if output.Data.Name != "value:"+route {
					t.Fatalf("output=%+v", output)
				}
				joined := strings.Join(output.Events, ",")
				if mcpContext != strings.Contains(joined, "input MCP") {
					t.Fatalf("MCP initialization=%v", output.Events)
				}
				if route == "mutation" {
					if !strings.HasPrefix(joined, "capture,input init") || !strings.HasSuffix(joined, "finalize committed") || strings.Contains(joined, "regular finalize") {
						t.Fatalf("mutation lifecycle=%s", joined)
					}
				} else if !strings.HasSuffix(joined, "regular finalize") {
					t.Fatalf("regular lifecycle=%s", joined)
				}
			}
			if calls["NewRegular"] != 1 || calls["NewMutation"] != 1 {
				t.Fatalf("factory lifetime=%v", calls)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []fixture.Record{{ID: 1, Name: "value:regular"}, {ID: 2, Name: "value:mutation"}})
		})
	}
}

func TestNamedFactorySnapshotIsolation(t *testing.T) {
	pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
	exports := x.NewRegistry()
	first, _ := x.NewFunction(pkg, "NewRegular", custom.Factory(fixture.NewRegular))
	if err := exports.RegisterFunctions(first); err != nil {
		t.Fatal(err)
	}
	builder, err := bootstrap.NewArtifactBuilder(exports)
	if err != nil {
		t.Fatal(err)
	}
	input := bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Regular"}, Routes: []*spec.Route{{Method: "POST", Path: "/regular", Handler: "NewRegular"}}}, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})}
	artifact, err := builder.Build(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = artifact.Registration(registry.RegisteredComponent{Handler: custom.New(fixture.NewRegular())}); err == nil {
		t.Fatal("explicit/named conflict ignored")
	}
	second, _ := x.NewFunction(pkg, "NewMutation", mutation.Factory(fixture.NewMutation))
	if err = exports.RegisterFunctions(second); err != nil {
		t.Fatal(err)
	}
	input.Component = input.Component.Clone()
	input.Component.Routes[0].Handler = "NewMutation"
	if _, err = builder.Build(input); err == nil {
		t.Fatal("later registration leaked into stage")
	}
	fresh, err := bootstrap.NewArtifactBuilder(exports)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fresh.Build(input); err != nil {
		t.Fatal(err)
	}
	for _, reference := range []string{"Missing", "wrong.NewRegular", pkg + ".Missing"} {
		input.Component.Routes[0].Handler = reference
		if _, err = fresh.Build(input); err == nil {
			t.Fatalf("unknown factory %q accepted", reference)
		}
	}
}

type attachedFactoryReader struct{}

func (attachedFactoryReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	return nil, fmt.Errorf("attached reader must not be selected")
}

type nilFactoryMap map[string]int

func (nilFactoryMap) Execute(context.Context, rhandler.Invocation) (any, error) {
	panic("nil handler invoked")
}
func (nilFactoryMap) InputType() reflect.Type  { panic("nil contract invoked") }
func (nilFactoryMap) OutputType() reflect.Type { panic("nil contract invoked") }

type nilFactorySlice []int

func (nilFactorySlice) Execute(context.Context, rhandler.Invocation) (any, error) {
	panic("nil handler invoked")
}
func (nilFactorySlice) InputType() reflect.Type  { panic("nil contract invoked") }
func (nilFactorySlice) OutputType() reflect.Type { panic("nil contract invoked") }

type nilFactoryFunc func()

func (nilFactoryFunc) Execute(context.Context, rhandler.Invocation) (any, error) {
	panic("nil handler invoked")
}
func (nilFactoryFunc) InputType() reflect.Type  { panic("nil contract invoked") }
func (nilFactoryFunc) OutputType() reflect.Type { panic("nil contract invoked") }

type nilFactoryChan chan int

func (nilFactoryChan) Execute(context.Context, rhandler.Invocation) (any, error) {
	panic("nil handler invoked")
}
func (nilFactoryChan) InputType() reflect.Type  { panic("nil contract invoked") }
func (nilFactoryChan) OutputType() reflect.Type { panic("nil contract invoked") }

func TestLinkedFactoryValidationAndAuthority(t *testing.T) {
	pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
	for _, test := range []struct {
		name     string
		function any
		match    string
	}{
		{"arguments", func(int) rhandler.TypedHandler { return nil }, "must return"},
		{"wrong return", func() int { return 1 }, "must return"},
		{"nil", func() rhandler.TypedHandler { return nil }, "returned nil"},
		{"typed nil map", func() nilFactoryMap { return nil }, "returned nil"},
		{"typed nil slice", func() nilFactorySlice { return nil }, "returned nil"},
		{"typed nil func", func() nilFactoryFunc { return nil }, "returned nil"},
		{"typed nil chan", func() nilFactoryChan { return nil }, "returned nil"},
		{"wrong contract", func() rhandler.TypedHandler {
			return custom.NewFunc(func(context.Context, *struct{}) (*struct{}, error) { return nil, nil })
		}, "contract mismatch"},
		{"factory error", func() (rhandler.TypedHandler, error) { return nil, errors.New("construction rejected") }, "construction rejected"},
		{"typed nil custom", custom.Factory[fixture.Input, fixture.Output](func() xhandler.Contract[fixture.Input, fixture.Output] { return nil }), "returned nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := x.NewRegistry()
			function, err := x.NewFunction(pkg, "Factory", test.function)
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.RegisterFunctions(function); err != nil {
				t.Fatal(err)
			}
			builder, err := bootstrap.NewArtifactBuilder(registry)
			if err != nil {
				t.Fatal(err)
			}
			_, err = builder.Build(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Test"}, Routes: []*spec.Route{{Method: "POST", Path: "/test", Handler: "Factory"}}}, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})})
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestNamedFactoryUsesExplicitPackageAndAllowsAttachedReaderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
	exports := x.NewRegistry()
	function, _ := x.NewFunction(pkg, "Factory", custom.Factory(fixture.NewRegular))
	if err := exports.RegisterFunctions(function); err != nil {
		t.Fatal(err)
	}
	builder, err := bootstrap.NewArtifactBuilder(exports)
	if err != nil {
		t.Fatal(err)
	}
	for index, reference := range []string{"Factory", "linked.Factory", pkg + ".Factory"} {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Test"}, TypeContext: &spec.TypeContext{DefaultPackage: "example.com/different/input", Imports: []spec.ImportSpec{{Alias: "linked", Package: pkg}}}, Routes: []*spec.Route{{Method: "POST", Path: "/test", Handler: reference}, {Method: "POST", Path: "/extra"}}}
		artifact, err := builder.Build(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})})
		if err != nil {
			t.Fatal(err)
		}
		registered, err := artifact.Registration(registry.RegisteredComponent{Reader: attachedFactoryReader{}, DataSource: dml.Source{DB: h.DB}})
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
		if err != nil {
			t.Fatal(err)
		}
		request := testharness.NewRequest("POST", "/extra").WithBody([]byte(fmt.Sprintf(`{"data":{"id":%d,"name":"linked"}}`, index+1)), "application/json")
		scope, err := request.Scope()
		if err != nil {
			t.Fatal(err)
		}
		_, err = runtime.ExecuteRoute(ctx, "POST", "/extra", scope)
		scope.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []fixture.Record{{ID: 1, Name: "linked:regular"}, {ID: 2, Name: "linked:regular"}, {ID: 3, Name: "linked:regular"}})
}

func TestNamedFactoryApplicationGenerationKeepsInflightHandlerSQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	input := bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}, Routes: []*spec.Route{{Method: "POST", Path: "/records", Handler: "Factory"}}}, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})}
	stage := func(revision uint64, create any) error {
		exports := x.NewRegistry()
		function, err := x.NewFunction(pkg, "Factory", create)
		if err != nil {
			return err
		}
		if err = exports.RegisterFunctions(function); err != nil {
			return err
		}
		return manager.Reload(ctx, application.Request{Revision: revision, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			compiled, err := report.NewProjectCompiler(report.ProjectConfig{Types: types, Registry: exports}).CompileArtifacts([]bootstrap.ArtifactInput{input})
			if err != nil {
				return nil, err
			}
			components, err := compiled.RuntimeComponents(ctx, report.RuntimeConfigureFunc(func(context.Context, *report.ComponentArtifact) (report.RuntimeCapabilities, error) {
				return report.RuntimeCapabilities{DataSource: dml.Source{DB: h.DB}}, nil
			}))
			if err != nil {
				return nil, err
			}
			catalog, err := compiled.Types()
			if err != nil {
				return nil, err
			}
			return &application.Build{Components: components, Types: catalog}, nil
		}})
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseOld := func() { releaseOnce.Do(func() { close(release) }) }
	if err = stage(1, custom.Factory(func() xhandler.Contract[fixture.Input, fixture.Output] {
		return fixture.NewBlockingRegular(release, started)
	})); err != nil {
		t.Fatal(err)
	}
	old := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/records", strings.NewReader(`{"data":{"id":1,"name":"old"}}`))
	request = request.WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		releaseOld()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("old invocation did not exit")
		}
	})
	go func() { manager.ServeHTTP(old, request); close(done) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("old invocation did not start")
	}
	if err = stage(2, mutation.Factory(fixture.NewMutation)); err != nil {
		t.Fatal(err)
	}
	current := httptest.NewRecorder()
	request = httptest.NewRequest("POST", "/records", strings.NewReader(`{"data":{"id":2,"name":"new"}}`))
	request = request.WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	manager.ServeHTTP(current, request)
	releaseOld()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("old invocation did not complete")
	}
	if old.Code != 200 || current.Code != 200 || !strings.Contains(old.Body.String(), "old:regular") || !strings.Contains(current.Body.String(), "new:mutation") {
		t.Fatalf("old=%d %s new=%d %s", old.Code, old.Body.String(), current.Code, current.Body.String())
	}
	if err = stage(3, func() int { return 1 }); err == nil || manager.Revision() != 2 {
		t.Fatalf("invalid stage changed generation: revision=%d error=%v", manager.Revision(), err)
	}
	retained := httptest.NewRecorder()
	request = httptest.NewRequest("POST", "/records", strings.NewReader(`{"data":{"id":3,"name":"retained"}}`)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	manager.ServeHTTP(retained, request)
	if retained.Code != 200 || !strings.Contains(retained.Body.String(), "retained:mutation") {
		t.Fatalf("failed stage changed handler: %d %s", retained.Code, retained.Body.String())
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []fixture.Record{{ID: 1, Name: "old:regular"}, {ID: 2, Name: "new:mutation"}, {ID: 3, Name: "retained:mutation"}})
}

func TestNamedFactoryPrivateComponentStaysInternalSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	pkg := reflect.TypeOf(fixture.Component{}).PkgPath()
	exports := x.NewRegistry()
	function, _ := x.NewFunction(pkg, "Factory", custom.Factory(fixture.NewRegular))
	if err := exports.RegisterFunctions(function); err != nil {
		t.Fatal(err)
	}
	var inputs []bootstrap.ArtifactInput
	for _, scope := range []string{pkg, pkg + "/private"} {
		path := "/public"
		if scope != pkg {
			path = "/private"
		}
		inputs = append(inputs, bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: "Records"}, Routes: []*spec.Route{{Method: "POST", Path: path, Handler: pkg + ".Factory"}}}, InputType: reflect.TypeOf(fixture.Input{}), OutputType: reflect.TypeOf(fixture.Output{})})
	}
	compiled, err := report.NewProjectCompiler(report.ProjectConfig{Registry: exports}).CompileArtifacts(inputs)
	if err != nil {
		t.Fatal(err)
	}
	components, err := compiled.RuntimeComponents(ctx, report.RuntimeConfigureFunc(func(context.Context, *report.ComponentArtifact) (report.RuntimeCapabilities, error) {
		return report.RuntimeCapabilities{DataSource: dml.Source{DB: h.DB}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime(components, druntime.WithExposedPackages([]string{pkg}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.RouteByMethodPath("POST", "/private"); found {
		t.Fatal("private component became public")
	}
	_, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: pkg + "/private", Name: "Records"}, Route: spec.RouteRef{Method: "POST", Path: "/private"}}, Input: &fixture.Input{Record: &fixture.Record{ID: 1, Name: "internal"}}})
	if err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records"}, []fixture.Record{{ID: 1, Name: "internal:regular"}})
}
