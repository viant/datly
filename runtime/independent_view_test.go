package runtime

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	"github.com/viant/datly/transcribe"
)

type independentViewRow struct {
	ID   int
	Name string
}

type independentManyInput struct {
	Existing []*independentViewRow
	Tenant   int
}

type independentOneInput struct {
	Existing *independentViewRow
	Tenant   int
}

type independentViewOutput struct {
	IDs []int
}

type independentVeltyOutput struct {
	Existing []*independentViewRow
}

type packageViewInput struct {
	Existing []*independentViewRow `parameter:"Existing,kind=view,in=Existing" view:"Existing,table=users" sql:"SELECT id, name FROM users WHERE tenant = :Tenant ORDER BY id"`
	Tenant   int                   `parameter:"Tenant,kind=query,in=tenant"`
}

func TestCustomHandlerReceivesIndependentViewThroughUnifiedBinding(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two'), (3, 8, 'other')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	transcribed, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{
		Scope: "example.com/independent",
		Name:  "Many",
		Text: `#setting($_ = $route('/views', 'GET'))
#define($_ = $Tenant<int>(query/tenant))
#define($_ = $Existing<[]IndependentViewRow>(data_view/existing) /*
SELECT id, name FROM users WHERE tenant = :Tenant ORDER BY id
*/)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("transcribe independent view: %v", err)
	}
	component := transcribed.Component
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(independentManyInput{}), OutputType: reflect.TypeOf(independentViewOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies,
		Input:        artifact.Input,
		SQL:          &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("NewViewProvider() error = %v", err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(independentViewOutput{}),
		Providers: []locator.Provider{viewProvider},
		Handler: customhandler.NewFunc(func(_ context.Context, input *independentManyInput) (*independentViewOutput, error) {
			result := &independentViewOutput{}
			for _, row := range input.Existing {
				result.IDs = append(result.IDs, row.ID)
			}
			return result, nil
		}),
	}
	service := independentViewRuntime(t, component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"7"}}))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	output := actual.(*independentViewOutput)
	if !reflect.DeepEqual(output.IDs, []int{1, 2}) {
		t.Fatalf("IDs = %v, want [1 2]", output.IDs)
	}
}

func TestCustomHandlerUsesTranscribedIndependentViewConnectorAndLimit(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	transcribed, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{
		Scope: "example.com/independent", Name: "Limited",
		Text: `#setting($_ = $route('/limited-views', 'GET'))
#define($_ = $Tenant<int>(query/tenant))
#define($_ = $Existing<[]IndependentViewRow>(view/existing).WithURI('queries/existing.sql').Connector('main').WithLimit(1).WithCache('existing-cache') /* SELECT id, name FROM users WHERE tenant = :Tenant ORDER BY id */)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("transcribe independent view: %v", err)
	}
	component := transcribed.Component
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(independentManyInput{}), OutputType: reflect.TypeOf(independentViewOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if len(artifact.ViewDependencies) != 1 || artifact.ViewDependencies[0].Plan.Root.Connector != "main" ||
		artifact.ViewDependencies[0].Plan.Root.View.Cache == nil || artifact.ViewDependencies[0].Plan.Root.View.Cache.Name != "existing-cache" ||
		artifact.ViewDependencies[0].Plan.Root.View.Spec.Source == nil || artifact.ViewDependencies[0].Plan.Root.View.Spec.Source.URI != "queries/existing.sql" ||
		artifact.ViewDependencies[0].Plan.Root.View.Spec.Source.Controls == nil ||
		artifact.ViewDependencies[0].Plan.Root.View.Spec.Source.Controls.Limit == nil || *artifact.ViewDependencies[0].Plan.Root.View.Spec.Source.Controls.Limit != 1 {
		t.Fatalf("compiled view dependency = %+v", artifact.ViewDependencies)
	}
	sqlComponent := &dsql.SQLComponent{}
	if err := sqlComponent.RegisterConnector("main", h.DB); err != nil {
		t.Fatalf("register connector: %v", err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: sqlComponent,
	})
	if err != nil {
		t.Fatalf("NewViewProvider() error = %v", err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(independentViewOutput{}),
		Providers: []locator.Provider{viewProvider},
		Handler: customhandler.NewFunc(func(_ context.Context, input *independentManyInput) (*independentViewOutput, error) {
			result := &independentViewOutput{}
			for _, row := range input.Existing {
				result.IDs = append(result.IDs, row.ID)
			}
			return result, nil
		}),
	}
	service := independentViewRuntime(t, component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/limited-views").WithQuery(url.Values{"tenant": {"7"}}))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	if output := actual.(*independentViewOutput); !reflect.DeepEqual(output.IDs, []int{1}) {
		t.Fatalf("IDs = %v, want [1]", output.IDs)
	}
}

func TestIndependentViewDependencyHonorsOneCardinality(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two'), (3, 8, 'other')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	component := independentViewComponent("One", spec.CardinalityOne)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(independentOneInput{}), OutputType: reflect.TypeOf(independentViewOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies,
		Input:        artifact.Input,
		SQL:          &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("NewViewProvider() error = %v", err)
	}
	called := false
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(independentViewOutput{}),
		Providers: []locator.Provider{viewProvider},
		Handler: customhandler.NewFunc(func(_ context.Context, input *independentOneInput) (*independentViewOutput, error) {
			called = true
			if input.Existing == nil {
				return &independentViewOutput{}, nil
			}
			return &independentViewOutput{IDs: []int{input.Existing.ID}}, nil
		}),
	}
	service := independentViewRuntime(t, component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"8"}}))
	if err != nil {
		t.Fatalf("one-row ExecuteRoute() error = %v", err)
	}
	if output := actual.(*independentViewOutput); !reflect.DeepEqual(output.IDs, []int{3}) {
		t.Fatalf("IDs = %v, want [3]", output.IDs)
	}
	called = false
	actual, err = executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"9"}}))
	if err != nil {
		t.Fatalf("zero-row ExecuteRoute() error = %v", err)
	}
	if !called || actual.(*independentViewOutput).IDs != nil {
		t.Fatalf("zero-row output = %#v, handler called = %v", actual, called)
	}
	called = false
	if _, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"7"}})); err == nil {
		t.Fatal("expected multiple-row cardinality error")
	}
	if called {
		t.Fatal("handler was invoked after independent-view binding failed")
	}
}

func TestVeltyHandlerReceivesIndependentViewThroughUnifiedBinding(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	component := independentViewComponent("Velty", spec.CardinalityMany)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(independentManyInput{}), OutputType: reflect.TypeOf(independentVeltyOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies,
		Input:        artifact.Input,
		SQL:          &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("NewViewProvider() error = %v", err)
	}
	handler, err := veltyhandler.New[independentManyInput, independentVeltyOutput](veltyhandler.Config{
		Template: `#set($Output.Existing = $Existing)`,
	})
	if err != nil {
		t.Fatalf("create Velty handler: %v", err)
	}
	registered := &registry.RegisteredComponent{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(independentVeltyOutput{}),
		Providers: []locator.Provider{viewProvider}, Handler: handler,
	}
	service := independentViewRuntime(t, component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"7"}}))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	output := actual.(*independentVeltyOutput)
	if len(output.Existing) != 2 || output.Existing[0].ID != 1 || output.Existing[1].ID != 2 {
		t.Fatalf("Existing = %#v", output.Existing)
	}
}

func TestPackageTagsProduceIndependentViewForUnifiedCustomHandler(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two'), (3, 8, 'other')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	authored := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/package", Name: "PackageView"}, Name: "PackageView",
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/package/views"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: authored, InputType: reflect.TypeOf(packageViewInput{}), OutputType: reflect.TypeOf(independentViewOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if len(authored.Parameters) != 0 || len(authored.Views) != 0 {
		t.Fatal("package bootstrap mutated authored metadata")
	}
	viewProvider, err := viewprovider.New(viewprovider.Config{
		Dependencies: artifact.ViewDependencies,
		Input:        artifact.Input,
		SQL:          &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("view provider: %v", err)
	}
	registered := &registry.RegisteredComponent{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(independentViewOutput{}),
		Providers: []locator.Provider{viewProvider},
		Handler: customhandler.NewFunc(func(_ context.Context, input *packageViewInput) (*independentViewOutput, error) {
			result := &independentViewOutput{}
			for _, row := range input.Existing {
				result.IDs = append(result.IDs, row.ID)
			}
			return result, nil
		}),
	}
	service := independentViewRuntime(t, artifact.Component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/package/views").WithQuery(url.Values{"tenant": {"7"}}))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	if output := actual.(*independentViewOutput); !reflect.DeepEqual(output.IDs, []int{1, 2}) {
		t.Fatalf("IDs = %v, want [1 2]", output.IDs)
	}
}

func independentViewComponent(name string, cardinality spec.Cardinality) *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/independent", Name: name}, Name: name,
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/views"}},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}},
			{Name: "Existing", Source: spec.BindSource{Kind: "view", Name: "Existing"}, Cardinality: string(cardinality)},
		},
		Views: []*spec.View{{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/independent", Name: "Existing"}, Name: "Existing",
			Source: &spec.ViewSource{SQL: "SELECT id, name FROM users WHERE tenant = :Tenant ORDER BY id"},
		}},
	}
}

func independentViewRuntime(t *testing.T, component *spec.Component, registered *registry.RegisteredComponent) *Runtime {
	t.Helper()
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("NewBundle() error = %v", err)
	}
	return newTestService(t, bundle, map[string]*registry.RegisteredComponent{component.Key.String(): registered})
}
