package transcribe

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func TestComponentLoaderPackageOnlyReturnsIsolatedCanonicalGraph(t *testing.T) {
	packageComponent := packageComponentFixture()
	loaded, err := (&componentLoader{packageComponent: packageComponent}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == packageComponent || loaded.RootView == packageComponent.RootView || loaded.Parameters[0] == packageComponent.Parameters[0] {
		t.Fatal("package-only load did not clone canonical metadata")
	}
	loaded.Parameters[0].Source.Name = "changed"
	loaded.RootView.Source.Table = "changed"
	if packageComponent.Parameters[0].Source.Name != "tenantID" || packageComponent.RootView.Source.Table != "package_users" {
		t.Fatal("loaded package graph aliases its source")
	}
}

func TestCompilerLoadsDQLOverPackageAuthority(t *testing.T) {
	packageComponent := packageComponentFixture()
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/dql", Name: "Users", PackageComponent: packageComponent,
		Text: `#package('example.com/dqltypes')
#import('dqlmodel','example.com/dql/model')
#setting($_ = $route('/dql/users/{tenantID}', 'GET'))
#setting($_ = $connector('dql'))
#define($_ = $Tenant<string>(path/tenantID).Required())
#define($_ = $Search<string>(query/search).Optional())
SELECT id, name FROM dql_users WHERE tenant_id = :Tenant`,
	})
	if err != nil {
		t.Fatal(err)
	}
	component := result.Component
	if component.Key.Scope != "example.com/app/dql" || component.Name != "Users" || len(component.Routes) != 1 ||
		component.Routes[0].Path != "/dql/users/{tenantID}" || component.Settings == nil ||
		component.Settings.DefaultConnector != "dql" || component.Settings.InputType != "PackageInput" ||
		component.Settings.OutputType != "PackageOutput" || component.Settings.Generation == nil || component.Settings.Generation.InputFile != "package_input.go" {
		t.Fatalf("component identity/settings = %+v", component)
	}
	if len(component.Parameters) != 3 || component.Parameters[0].Name != "Tenant" || component.Parameters[0].Source.Kind != "path" ||
		component.Parameters[0].Source.Name != "tenantID" || component.Parameters[1].Name != "Search" ||
		component.Parameters[2].Name != "Locale" || component.Parameters[2].Source.Kind != "header" {
		t.Fatalf("params = %+v", component.Parameters)
	}
	if component.RootView == nil || component.RootView.Source == nil || component.RootView.Source.SQL == "" ||
		component.RootView.Source.Table == "package_users" || len(component.Views) != 1 || component.Views[0].Name != "Audit" {
		t.Fatalf("views = root:%+v independent:%+v", component.RootView, component.Views)
	}
	if component.TypeContext == nil || component.TypeContext.DefaultPackage != "example.com/dqltypes" ||
		len(component.TypeContext.Imports) != 2 || component.TypeContext.Imports[0].Alias != "dqlmodel" ||
		component.TypeContext.Imports[1].Alias != "pkgmodel" {
		t.Fatalf("type context = %+v", component.TypeContext)
	}
	if packageComponent.Parameters[0].Source.Name != "tenantID" || packageComponent.RootView.Source.Table != "package_users" ||
		packageComponent.Settings.DefaultConnector != "package" {
		t.Fatal("DQL+package load mutated package authority")
	}
}

func TestCompilerLoadsDQLWithoutFabricatingPackageMetadata(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/dql", Name: "Search",
		Text: `#setting($_ = $route('/search', 'GET'))
#define($_ = $Query<string>(query/q).Optional())
SELECT id FROM search_index`,
	})
	if err != nil {
		t.Fatal(err)
	}
	component := result.Component
	if len(component.Parameters) != 1 || component.Parameters[0].Name != "Query" || len(component.Views) != 0 ||
		component.Settings != nil || component.TypeContext != nil {
		t.Fatalf("DQL-only component = %+v", component)
	}
}

func TestComponentLoaderRejectsConflictingPackageRouteMarshallers(t *testing.T) {
	_, err := (&componentLoader{
		packageComponent: &spec.Component{Routes: []*spec.Route{
			{Method: "GET", Path: "/users", Marshaller: "json"},
			{Method: "POST", Path: "/users", Marshaller: "tabular"},
		}},
		authoredComponent: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/v2/users"}}},
	}).Load()
	if err == nil || !strings.Contains(err.Error(), "conflicting marshallers") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestComponentLoaderMergesRouteMCPByExactRouteAuthority(t *testing.T) {
	packageExposure := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "package.users", Description: "Package users"}
	tests := []struct {
		name      string
		authored  *spec.Route
		want      string
		wantCount int
	}{
		{
			name:     "exact route inherits package exposure",
			authored: &spec.Route{Method: "get", Path: "/users"},
			want:     "package.users", wantCount: 1,
		},
		{
			name:     "authored exposure wins",
			authored: &spec.Route{Method: "GET", Path: "/users", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "dql.users"}}},
			want:     "dql.users", wantCount: 1,
		},
		{
			name:      "changed route does not inherit",
			authored:  &spec.Route{Method: "GET", Path: "/v2/users"},
			wantCount: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packageComponent := &spec.Component{Routes: []*spec.Route{{
				Method: "GET", Path: "/users", MCP: []*spec.MCPExposure{packageExposure},
			}}}
			loaded, err := (&componentLoader{
				packageComponent:  packageComponent,
				authoredComponent: &spec.Component{Routes: []*spec.Route{test.authored}},
			}).Load()
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded.Routes) != 1 || len(loaded.Routes[0].MCP) != test.wantCount {
				t.Fatalf("routes = %+v", loaded.Routes)
			}
			if test.wantCount > 0 && loaded.Routes[0].MCP[0].Name != test.want {
				t.Fatalf("route exposure = %+v", loaded.Routes[0].MCP)
			}
			if len(loaded.Routes[0].MCP) > 0 {
				loaded.Routes[0].MCP[0].Description = "changed"
				if packageExposure.Description != "Package users" {
					t.Fatal("loaded route exposure aliases package authority")
				}
			}
		})
	}
}

func TestCompilerMergesDQLAndPackageRouteMCPAuthority(t *testing.T) {
	tests := []struct {
		name string
		mcp  string
		want string
	}{
		{name: "package exposure inherited", want: "package.users"},
		{name: "DQL exposure wins", mcp: "#settings($_ = $mcp('dql.users', 'DQL users'))\n", want: "dql.users"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packageComponent := packageComponentFixture()
			packageComponent.Routes = []*spec.Route{{
				Method: "GET", Path: "/users",
				MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "package.users", Description: "Package users"}},
			}}
			result, err := NewCompiler().Compile(context.Background(), &Source{
				Scope: "example.com/app/dql", Name: "Users", PackageComponent: packageComponent,
				Text: test.mcp + `#setting($_ = $route('/users', 'GET'))
SELECT id FROM users`,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Component.Routes) != 1 || len(result.Component.Routes[0].MCP) != 1 ||
				result.Component.Routes[0].MCP[0].Name != test.want {
				t.Fatalf("route exposure = %+v", result.Component.Routes)
			}
			result.Component.Routes[0].MCP[0].Description = "changed"
			if packageComponent.Routes[0].MCP[0].Description != "Package users" {
				t.Fatal("compiled route exposure aliases package authority")
			}
		})
	}
}

func TestCompilerDQLPackageOverlayCompilesAgainstLinkedInput(t *testing.T) {
	type linkedInput struct {
		Tenant string `bind:"Tenant,kind=path,in=tenantID"`
		Search string `bind:"Search,kind=query,in=search"`
		Locale string `bind:"Locale,kind=header,in=X-Locale"`
	}
	packageComponent, err := (bootstrap.ContractResolver{InputType: xshape.Linked(reflect.TypeOf(linkedInput{})).Descriptor()}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/users", Name: "Users", PackageComponent: packageComponent,
		Text: `#setting($_ = $route('/users/{tenantID}', 'GET'))
#define($_ = $Tenant<string>(path/tenantID).Required())
#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users WHERE tenant_id = :Tenant AND name = :Search`,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := handlercompiler.New(handlercompiler.Input{
		Component: result.Component,
		InputType: reflect.TypeOf(linkedInput{}),
	}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	bindings := map[string]string{}
	for _, binding := range compiled.Bindings {
		bindings[binding.Path] = binding.Location.Kind + "/" + binding.Location.In
	}
	if bindings["Tenant"] != "path/tenantID" || bindings["Search"] != "query/search" || bindings["Locale"] != "header/X-Locale" {
		t.Fatalf("bindings = %+v", bindings)
	}
}

func TestCompilerDQLDeclarationOverridesMatchingPackageView(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/package", Name: "Users", PackageComponent: packageComponentFixture(),
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Audit<[]AuditRow>(view/Audit) /* SELECT id FROM audit */)
SELECT id FROM users`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Component.Views) != 1 || result.Component.Views[0].Name != "Audit" ||
		result.Component.Views[0].Source == nil || result.Component.Views[0].Source.SQL != "SELECT id FROM audit" {
		t.Fatalf("views = %+v", result.Component.Views)
	}
}

func TestCompilerPreservesPackageViewWithDistinctDeclarationIdentity(t *testing.T) {
	const (
		scope = "example.com/app/dql"
		SQL   = "SELECT id FROM audit a"
	)
	tests := []struct {
		name       string
		packageKey spec.Key
		namespace  string
	}{
		{
			name:       "different scope",
			packageKey: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "Audit"},
		},
		{
			name:       "different namespace",
			packageKey: spec.Key{Kind: spec.KindView, Scope: scope, Name: "Audit"},
			namespace:  "archive",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packageView := &spec.View{
				Key: test.packageKey, Name: "Audit", Namespace: test.namespace,
				Cardinality: spec.CardinalityOne, Source: &spec.ViewSource{SQL: SQL},
			}
			result, err := NewCompiler().Compile(context.Background(), &Source{
				Scope: scope, Name: "Users",
				PackageComponent: &spec.Component{Views: []*spec.View{packageView}},
				Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Audit<[]AuditRow>(view/Audit) /* SELECT id FROM audit a */)
SELECT id FROM users`,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Component.Views) != 2 {
				t.Fatalf("views = %+v", result.Component.Views)
			}
			authored := result.Component.Views[0]
			preserved := result.Component.Views[1]
			if authored.Key.Scope != scope || authored.Namespace != "a" || authored.Source == nil || authored.Source.SQL != SQL {
				t.Fatalf("compiled authored view = %+v", authored)
			}
			if preserved.Key != test.packageKey || preserved.Namespace != test.namespace || preserved.Source == nil || preserved.Source.SQL != SQL {
				t.Fatalf("preserved package view = %+v", preserved)
			}
			params := spec.EffectiveParameters(result.Component.Parameters)
			if len(params) != 1 || params[0].Cardinality != string(spec.CardinalityMany) {
				t.Fatalf("authored parameter cardinality followed the wrong same-name view: %+v", params)
			}
		})
	}
}

func TestCompilerNormalizesPackageOnlyRequiredViewCardinality(t *testing.T) {
	required := true
	optional := false
	packageComponent := &spec.Component{
		Parameters: []*spec.Parameter{
			{Name: "RequiredRows", Source: spec.BindSource{Kind: "view", Name: "RequiredRows"}, Required: &required},
			{Name: "OptionalOne", Source: spec.BindSource{Kind: "view", Name: "OptionalOne"}, Required: &optional},
			{Name: "OptionalMany", Source: spec.BindSource{Kind: "view", Name: "OptionalMany"}, Required: &optional},
		},
		Views: []*spec.View{
			{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "RequiredRows"}, Name: "RequiredRows", Cardinality: spec.CardinalityMany, Source: &spec.ViewSource{SQL: "SELECT id FROM required_rows"}},
			{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "OptionalOne"}, Name: "OptionalOne", Cardinality: spec.CardinalityOne, Source: &spec.ViewSource{SQL: "SELECT id FROM optional_one"}},
			{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "OptionalMany"}, Name: "OptionalMany", Source: &spec.ViewSource{SQL: "SELECT id FROM optional_many"}},
		},
	}
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/package", Name: "Users",
		PackageComponent: packageComponent,
		Text: `#setting($_ = $route('/users', 'GET'))
SELECT id FROM users`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	params := spec.EffectiveParameters(result.Component.Parameters)
	if len(params) != 3 || params[0].Cardinality != string(spec.CardinalityOne) ||
		params[1].Cardinality != string(spec.CardinalityOne) || params[2].Cardinality != string(spec.CardinalityMany) {
		t.Fatalf("params = %+v", params)
	}
	for _, param := range packageComponent.Parameters {
		if param.Cardinality != "" {
			t.Fatalf("package parameter %q was mutated: %+v", param.Name, param)
		}
	}
}

func TestCompilerRejectsInvalidIndependentViewCardinality(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/package", Name: "Users",
		PackageComponent: &spec.Component{
			Parameters: []*spec.Parameter{{Name: "Authorization", Source: spec.BindSource{Kind: "view", Name: "Authorization"}}},
			Views: []*spec.View{{
				Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "Authorization"}, Name: "Authorization",
				Cardinality: spec.Cardinality("Some"), Source: &spec.ViewSource{SQL: "SELECT authorized FROM authorization"},
			}},
		},
		Text: `#setting($_ = $route('/users', 'GET'))
SELECT id FROM users`,
	})
	if err == nil || result != nil || !strings.Contains(err.Error(), `unsupported cardinality "Some"`) {
		t.Fatalf("Compile() result=%+v error=%v", result, err)
	}
}

func TestSettingsLoaderOverlaysNestedSettingsWithoutAliasing(t *testing.T) {
	base := &spec.Settings{
		Generation: &spec.GenerationSettings{Template: "reader", ViewFile: "package.go", InputFile: "package_input.go"},
		Report: &spec.ReportSettings{Enabled: true, LinkedInputType: "PackageInput", InputLayout: &spec.ReportInputLayout{
			Dimensions: "PackageDimensions", Measures: "PackageMeasures",
		}},
		Cache: &spec.CacheSettings{Enabled: true, Name: "package-cache", Warmup: &spec.CacheWarmupSettings{
			Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "tenant", Values: []string{"1"}}}}},
		}},
		Const: map[string]string{"shared": "package", "package": "only"},
	}
	authored := &spec.Settings{
		Generation: &spec.GenerationSettings{Template: "patch", ViewFile: "dql.go"},
		Report: &spec.ReportSettings{Enabled: true, InputLayout: &spec.ReportInputLayout{
			Dimensions: "DQLDimensions",
		}},
		Cache: &spec.CacheSettings{Enabled: false},
		Const: map[string]string{"shared": "dql"},
	}
	actual := (&settingsLoader{base: base, authored: authored}).Load()
	if actual.Report == nil || !actual.Report.Enabled || actual.Report.LinkedInputType != "" || actual.Report.InputLayout == nil ||
		actual.Report.InputLayout.Dimensions != "DQLDimensions" || actual.Report.InputLayout.Measures != "PackageMeasures" ||
		actual.Cache == nil || actual.Cache.Enabled || actual.Cache.Name != "" || actual.Cache.Warmup != nil ||
		actual.Generation == nil || actual.Generation.Template != "patch" || actual.Generation.ViewFile != "dql.go" || actual.Generation.InputFile != "package_input.go" ||
		actual.Const["shared"] != "dql" || actual.Const["package"] != "only" {
		t.Fatalf("settings = %+v", actual)
	}
	actual.Const["package"] = "changed"
	actual.Generation.InputFile = "changed.go"
	actual.Report.InputLayout.Measures = "changed"
	if base.Cache.Warmup.Cases[0].Set[0].Values[0] != "1" || base.Const["package"] != "only" {
		t.Fatal("loaded settings alias package settings")
	}
	if base.Report.InputLayout.Measures != "PackageMeasures" {
		t.Fatal("loaded report input layout aliases package settings")
	}
	if base.Generation.InputFile != "package_input.go" {
		t.Fatal("loaded generation settings alias package settings")
	}
}

func TestSettingsLoaderAuthoredEmptyReportInputSelectsGeneratedType(t *testing.T) {
	actual := (&settingsLoader{
		base:     &spec.Settings{Report: &spec.ReportSettings{Enabled: true, LinkedInputType: "PackageInput"}},
		authored: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
	}).Load()
	if actual.Report == nil || actual.Report.LinkedInputType != "" {
		t.Fatalf("report settings = %+v, want generated input selection", actual.Report)
	}
}

func TestSettingsLoaderOverlaysConstantsByCanonicalName(t *testing.T) {
	base := &spec.Settings{Const: map[string]string{"Vendor": "package", "PackageOnly": "kept"}}
	authored := &spec.Settings{Const: map[string]string{"vendor": "dql"}}
	actual := (&settingsLoader{base: base, authored: authored}).Load()
	if len(actual.Const) != 2 || actual.Const["vendor"] != "dql" || actual.Const["PackageOnly"] != "kept" {
		t.Fatalf("constants = %+v", actual.Const)
	}
	if _, ok := actual.Const["Vendor"]; ok {
		t.Fatalf("package spelling survived authored override: %+v", actual.Const)
	}
	if base.Const["Vendor"] != "package" {
		t.Fatalf("base constants mutated: %+v", base.Const)
	}
}

func TestComponentLoaderPreservesSameNameParametersWithDifferentSources(t *testing.T) {
	packageParam := &spec.Parameter{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}}
	authoredParam := &spec.Parameter{Name: "Token", Source: spec.BindSource{Kind: "query", Name: "token"}}
	loaded, err := (&componentLoader{
		packageComponent:  &spec.Component{Parameters: []*spec.Parameter{packageParam}},
		authoredComponent: &spec.Component{Parameters: []*spec.Parameter{authoredParam}},
	}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Parameters) != 2 || loaded.Parameters[0].Identity() != authoredParam.Identity() || loaded.Parameters[1].Identity() != packageParam.Identity() {
		t.Fatalf("params = %+v", loaded.Parameters)
	}
}

func TestComponentLoaderUsesCanonicalViewIdentity(t *testing.T) {
	packageView := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/package", Name: "Audit"}, Name: "Audit"}
	authoredView := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/dql", Name: "Audit"}, Name: "Audit"}
	loaded, err := (&componentLoader{
		packageComponent:  &spec.Component{Views: []*spec.View{packageView}},
		authoredComponent: &spec.Component{Views: []*spec.View{authoredView}},
	}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Views) != 2 || loaded.Views[0].Key.Scope != "example.com/dql" || loaded.Views[1].Key.Scope != "example.com/package" {
		t.Fatalf("views = %+v", loaded.Views)
	}

	authoredView.Key.Scope = packageView.Key.Scope
	loaded, err = (&componentLoader{
		packageComponent:  &spec.Component{Views: []*spec.View{packageView}},
		authoredComponent: &spec.Component{Views: []*spec.View{authoredView}},
	}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Views) != 1 || loaded.Views[0].Key.Scope != packageView.Key.Scope {
		t.Fatalf("matching canonical view was not replaced: %+v", loaded.Views)
	}
}

func TestComponentLoaderRejectsDuplicateCanonicalViews(t *testing.T) {
	view := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/dql", Name: "Audit"}, Name: "Audit"}
	_, err := (&componentLoader{authoredComponent: &spec.Component{Views: []*spec.View{view, view.Clone()}}}).Load()
	if err == nil {
		t.Fatal("expected duplicate canonical view error")
	}
}

func TestComponentLoaderRejectsDuplicateCanonicalParams(t *testing.T) {
	param := &spec.Parameter{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}}
	_, err := (&componentLoader{packageComponent: &spec.Component{Parameters: []*spec.Parameter{param, param.Clone()}}}).Load()
	if err == nil {
		t.Fatal("expected duplicate canonical parameter error")
	}
}

func packageComponentFixture() *spec.Component {
	return &spec.Component{
		Key:         spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/package", Name: "PackageUsers"},
		Name:        "PackageUsers",
		Description: "package description",
		Settings: &spec.Settings{
			DefaultConnector: "package", InputType: "PackageInput", OutputType: "PackageOutput",
			Generation: &spec.GenerationSettings{InputFile: "package_input.go"},
		},
		TypeContext: &spec.TypeContext{
			DefaultPackage: "example.com/app/package",
			Imports:        []spec.ImportSpec{{Alias: "pkgmodel", Package: "example.com/app/package/model"}},
		},
		Routes: []*spec.Route{{Method: "POST", Path: "/package/users"}},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", Source: spec.BindSource{Kind: "path", Name: "tenantID"}, TypeExpr: "string"},
			{Name: "Locale", Source: spec.BindSource{Kind: "header", Name: "X-Locale"}, TypeExpr: "string"},
		},
		RootView: &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "PackageUsers"}, Name: "PackageUsers", Source: &spec.ViewSource{Table: "package_users"}},
		Views: []*spec.View{{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/app/package", Name: "Audit"}, Name: "Audit", Namespace: "audit", Source: &spec.ViewSource{Table: "audit"},
		}},
	}
}
