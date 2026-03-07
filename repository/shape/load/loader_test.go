package load

import (
	"context"
	"embed"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/scan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/x"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler/response"
)

//go:embed testdata/*.sql
var testFS embed.FS

type embeddedFS struct{}

func (embeddedFS) EmbedFS() *embed.FS {
	return &testFS
}

type reportRow struct {
	ID   int
	Name string
}

type relationTreeRoot struct {
	ID int `sqlx:"name=ID"`
}

type relationTreeChild struct {
	ID     int `sqlx:"name=ID"`
	RootID int `sqlx:"name=RootID"`
}

type relationTreeGrandChild struct {
	ID      int `sqlx:"name=ID"`
	ChildID int `sqlx:"name=ChildID"`
}

type cityRow struct {
	ID         int `sqlx:"name=ID"`
	DistrictID int `sqlx:"name=DISTRICT_ID"`
}

type vendorProductRow struct {
	ID       int `sqlx:"name=ID"`
	VendorID int `sqlx:"name=VENDOR_ID"`
}

type fieldOnlyUserACLRow struct {
	UserID     int `sqlx:"name=UserID"`
	IsReadOnly int `sqlx:"name=IsReadOnly"`
	Feature1   int `sqlx:"name=Feature1"`
}

type placeholderDistrictRow struct {
	Col1 string `sqlx:"name=col_1"`
	Col2 string `sqlx:"name=col_2"`
}

type reportSource struct {
	embeddedFS
	Rows   []reportRow `view:"rows,table=REPORT,connector=dev,cache=c1" sql:"uri=testdata/report.sql"`
	ID     int         `parameter:"id,kind=query,in=id"`
	Status any         `parameter:"status,kind=output,in=status"`
	Job    any         `parameter:"job,kind=async,in=job"`
	Meta   any         `parameter:"meta,kind=meta,in=view.name"`
	Route  struct{}    `component:",path=/v1/api/dev/report,method=GET,connector=dev"`
}

type typedRouteInput struct {
	ID int
}

type typedRouteOutput struct {
	Data []reportRow
}

type typedTeamRouteInput struct {
	TeamID string `parameter:",kind=path,in=teamID"`
}

type typedTeamRouteOutput struct{}

type typedRouteSource struct {
	embeddedFS
	Rows  []reportRow                                         `view:"rows,table=REPORT" sql:"uri=testdata/report.sql"`
	Route xdatly.Component[typedRouteInput, typedRouteOutput] `component:",path=/v1/api/dev/report,method=GET"`
}

type dynamicRouteInput struct {
	Name string
}

type dynamicRouteOutput struct {
	Count int
}

type namedDynamicRouteInput struct {
	Name string `parameter:"name,kind=query,in=name"`
}

type namedDynamicRouteOutput struct {
	response.Status `parameter:",kind=output,in=status" json:",omitempty"`
	Data            []*reportRow `parameter:",kind=output,in=view" view:"rows,table=REPORT" sql:"uri=testdata/report.sql" anonymous:"true"`
}

type dynamicRouteSource struct {
	embeddedFS
	Rows  []reportRow                `view:"rows,table=REPORT" sql:"uri=testdata/report.sql"`
	Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET"`
}

type routerOnlyInput struct {
	ID int `parameter:"id,kind=query,in=id"`
}

func (*routerOnlyInput) EmbedFS() *embed.FS { return &testFS }

type routerOnlyOutput struct {
	response.Status `parameter:",kind=output,in=status" json:",omitempty"`
	Data            []*reportRow `parameter:",kind=output,in=view" view:"rows,table=REPORT" sql:"uri=testdata/report.sql" anonymous:"true"`
}

type routerOnlySource struct {
	Route xdatly.Component[routerOnlyInput, routerOnlyOutput] `component:",path=/v1/api/dev/router-only,method=GET"`
}

func TestLoader_LoadViews(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)
	require.Len(t, artifacts.Views, 1)

	aView := artifacts.Views[0]
	assert.Equal(t, "rows", aView.Name)
	assert.Equal(t, "REPORT", aView.Table)
	require.NotNil(t, aView.Schema)
	assert.Equal(t, "Many", string(aView.Schema.Cardinality))
	require.NotNil(t, aView.Template)
	assert.Equal(t, "testdata/report.sql", aView.Template.SourceURL)
	assert.Contains(t, aView.Template.Source, "SELECT ID, NAME FROM REPORT")
	require.NotNil(t, aView.Connector)
	assert.Equal(t, "dev", aView.Connector.Ref)
	require.NotNil(t, aView.Cache)
	assert.Equal(t, "c1", aView.Cache.Ref)
	require.NotNil(t, artifacts.Resource.EmbedFS())
}

// stubPlanSpec is a non-plan-Result implementation of shape.PlanSpec used to
// verify that LoadViews() returns an error when given an unexpected plan type.
type stubPlanSpec struct{}

func (s *stubPlanSpec) ShapeSpecKind() string { return "stub" }

func TestLoader_LoadViews_InvalidPlanType(t *testing.T) {
	loader := New()
	_, err := loader.LoadViews(context.Background(), &shape.PlanResult{Source: &shape.Source{Name: "x"}, Plan: &stubPlanSpec{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported plan kind")
}

func TestLoader_LoadViews_Metadata(t *testing.T) {
	noLimit := true
	allowNulls := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:              "items",
					Table:             "ITEMS",
					Module:            "platform/items",
					AllowNulls:        &allowNulls,
					SelectorNamespace: "it",
					SelectorNoLimit:   &noLimit,
					SchemaType:        "*ItemView",
					Cardinality:       "many",
					FieldType:         reflect.TypeOf([]map[string]interface{}{}),
					ElementType:       reflect.TypeOf(map[string]interface{}{}),
					SQL:               "SELECT * FROM ITEMS",
					Declaration: &plan.ViewDeclaration{
						ColumnsConfig: map[string]*plan.ViewColumnConfig{
							"AUTHORIZED": {
								DataType: "bool",
								Tag:      `internal:"true"`,
							},
						},
					},
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}
	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 1)
	actual := artifacts.Views[0]
	assert.Equal(t, "platform/items", actual.Module)
	require.NotNil(t, actual.AllowNulls)
	assert.True(t, *actual.AllowNulls)
	require.NotNil(t, actual.Selector)
	assert.Equal(t, "it", actual.Selector.Namespace)
	assert.True(t, actual.Selector.NoLimit)
	require.NotNil(t, actual.Schema)
	assert.Equal(t, "*ItemView", actual.Schema.DataType)
	require.NotNil(t, actual.ColumnsConfig)
	require.Contains(t, actual.ColumnsConfig, "AUTHORIZED")
	require.NotNil(t, actual.ColumnsConfig["AUTHORIZED"].DataType)
	assert.Equal(t, "bool", *actual.ColumnsConfig["AUTHORIZED"].DataType)
	require.NotNil(t, actual.ColumnsConfig["AUTHORIZED"].Tag)
	assert.Equal(t, `internal:"true"`, *actual.ColumnsConfig["AUTHORIZED"].Tag)
}

func TestLoader_LoadViews_InfersColumnsFromBestSchemaType(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "user_acl"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "user_acl",
					SchemaType:  "*UserAclView",
					FieldType:   reflect.TypeOf([]fieldOnlyUserACLRow{}),
					ElementType: nil,
					SQL:         "SELECT 1",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}
	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.Len(t, artifacts.Views, 1)
	require.Len(t, artifacts.Views[0].Columns, 3)
	assert.Equal(t, "UserID", artifacts.Views[0].Columns[0].Name)
	assert.Equal(t, "IsReadOnly", artifacts.Views[0].Columns[1].Name)
	assert.Equal(t, "Feature1", artifacts.Views[0].Columns[2].Name)
}

func TestBindTemplateParameters_SkipsSelfViewParameter(t *testing.T) {
	resource := &view.Resource{
		Parameters: state.Parameters{
			{Name: "Jwt", In: state.NewHeaderLocation("Authorization")},
			{Name: "VendorID", In: state.NewPathLocation("vendorID")},
			{Name: "Authorization", In: state.NewViewLocation("authorization")},
			{Name: "Auth", In: state.NewComponent("GET:/auth")},
		},
		Views: []*view.View{
			{
				Name: "authorization",
				Template: view.NewTemplate("SELECT Authorized", view.WithTemplateParameters(
					&state.Parameter{Name: "Jwt", In: state.NewHeaderLocation("Authorization")},
				)),
			},
		},
	}

	bindTemplateParameters(resource)

	require.Len(t, resource.Views, 1)
	require.NotNil(t, resource.Views[0].Template)
	var names []string
	for _, param := range resource.Views[0].Template.Parameters {
		names = append(names, param.Name)
	}
	assert.ElementsMatch(t, []string{"Jwt", "VendorID", "Auth"}, names)
}

func TestLoader_LoadComponent_NormalizesViewInputSchemaFromResourceView(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/teams"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "user_team",
					Table:       "TEAM",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "UPDATE TEAM SET ACTIVE = false",
				},
				{
					Name:        "TeamStats",
					Table:       "TEAM",
					Cardinality: "many",
					SchemaType:  "*TeamStatsView",
					FieldType: reflect.TypeOf([]struct {
						ID          int    `sqlx:"name=ID"`
						TeamMembers int    `sqlx:"name=TEAM_MEMBERS"`
						Name        string `sqlx:"name=NAME"`
					}{}),
					SQL: "SELECT ID, 0 AS TEAM_MEMBERS, NAME FROM TEAM",
				},
			},
			States: []*plan.State{
				{Parameter: state.Parameter{Name: "TeamIDs", In: state.NewQueryLocation("TeamIDs"), Schema: &state.Schema{DataType: "[]int"}}},
				{Parameter: state.Parameter{Name: "TeamStats", In: state.NewViewLocation("TeamStats")}},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	param := component.InputParameters().Lookup("TeamStats")
	require.NotNil(t, param)
	require.NotNil(t, param.Schema)
	assert.Equal(t, "TeamStatsView", param.Schema.Name)
	assert.Equal(t, "*TeamStatsView", param.Schema.DataType)
	assert.Equal(t, state.Many, param.Schema.Cardinality)
}

func TestLoader_LoadComponent(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Name: "/v1/api/report", Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	actualPlan, ok := plan.ResultFrom(planned)
	require.True(t, ok)
	actualPlan.ColumnsDiscovery = true
	actualPlan.TypeContext = &typectx.Context{
		DefaultPackage: "mdp/performance",
		Imports: []typectx.Import{
			{Alias: "perf", Package: "github.com/acme/mdp/performance"},
		},
	}
	actualPlan.Directives = &dqlshape.Directives{
		Meta:             "docs/report.md",
		DefaultConnector: "analytics",
		Dest:             "all.go",
		InputDest:        "input.go",
		OutputDest:       "output.go",
		RouterDest:       "router.go",
		InputType:        "CustomInput",
		OutputType:       "CustomOutput",
		Cache: &dqlshape.CacheDirective{
			Enabled:      true,
			TTL:          "5m",
			Name:         "aerospike",
			Provider:     "aerospike://127.0.0.1:3000/test",
			Location:     "${view.Name}",
			TimeToLiveMs: 3600000,
		},
		MCP: &dqlshape.MCPDirective{
			Name:            "report.list",
			Description:     "List report rows",
			DescriptionPath: "docs/mcp/report.md",
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)
	require.NotNil(t, artifact.Component)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	assert.Equal(t, "/v1/api/report", component.Name)
	assert.Equal(t, "/v1/api/dev/report", component.URI)
	assert.Equal(t, "GET", component.Method)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Equal(t, "Route", component.ComponentRoutes[0].FieldName)
	assert.Equal(t, "/v1/api/dev/report", component.ComponentRoutes[0].RoutePath)
	assert.Equal(t, "dev", component.ComponentRoutes[0].Connector)
	assert.Equal(t, "rows", component.RootView)
	assert.Equal(t, []string{"rows"}, component.Views)
	assert.Len(t, component.Input, 1)
	assert.Len(t, component.Output, 1)
	assert.Len(t, component.Async, 1)
	assert.Len(t, component.Meta, 1)
	require.NotNil(t, component.TypeContext)
	assert.Equal(t, "mdp/performance", component.TypeContext.DefaultPackage)
	require.Len(t, component.TypeContext.Imports, 1)
	assert.Equal(t, "perf", component.TypeContext.Imports[0].Alias)
	require.NotNil(t, component.Directives)
	assert.Equal(t, "docs/report.md", component.Directives.Meta)
	assert.Equal(t, "analytics", component.Directives.DefaultConnector)
	require.NotNil(t, component.Directives.Cache)
	assert.True(t, component.Directives.Cache.Enabled)
	assert.Equal(t, "5m", component.Directives.Cache.TTL)
	assert.Equal(t, "aerospike", component.Directives.Cache.Name)
	require.NotNil(t, component.Directives.MCP)
	assert.Equal(t, "report.list", component.Directives.MCP.Name)
	require.NotEmpty(t, artifact.Resource.CacheProviders)
	assert.Equal(t, "aerospike", artifact.Resource.CacheProviders[0].Name)
	assert.Equal(t, "aerospike://127.0.0.1:3000/test", artifact.Resource.CacheProviders[0].Provider)
	assert.Equal(t, "${view.Name}", artifact.Resource.CacheProviders[0].Location)
	assert.Equal(t, 3600000, artifact.Resource.CacheProviders[0].TimeToLiveMs)
	assert.True(t, component.ColumnsDiscovery)
	require.NotNil(t, component.TypeSpecs)
	require.NotNil(t, component.TypeSpecs["input"])
	assert.Equal(t, "CustomInput", component.TypeSpecs["input"].TypeName)
	assert.Equal(t, "input.go", component.TypeSpecs["input"].Dest)
	require.NotNil(t, component.TypeSpecs["output"])
	assert.Equal(t, "CustomOutput", component.TypeSpecs["output"].TypeName)
	assert.Equal(t, "output.go", component.TypeSpecs["output"].Dest)
	assert.Equal(t, "router.go", component.Directives.RouterDest)
}

func TestLoader_LoadComponent_UsesComponentRouteWhenSourceNameMissing(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Equal(t, "/v1/api/dev/report", component.URI)
	assert.Equal(t, "/v1/api/dev/report", component.Name)
	assert.Equal(t, "GET", component.Method)
	assert.Equal(t, "Route", component.ComponentRoutes[0].FieldName)
}

func TestLoader_LoadComponent_InheritsTypeContextPackageForNamedStateSchemas(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			TypeContext: &typectx.Context{
				DefaultPackage: "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one",
				PackagePath:    "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one",
			},
			Views: []*plan.View{
				{
					Name:        "foos",
					Holder:      "Foos",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Cardinality: string(state.Many),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Foos",
						In:   &state.Location{Kind: state.KindRequestBody, Name: ""},
						Schema: &state.Schema{
							Name:     "Foos",
							DataType: "Foos",
						},
					},
				},
				{
					Parameter: state.Parameter{
						Name: "Foos",
						In:   &state.Location{Kind: state.KindOutput, Name: "view"},
						Tag:  `anonymous:"true"`,
						Schema: &state.Schema{
							Name:     "Foos",
							DataType: "Foos",
						},
					},
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.Input, 1)
	require.Len(t, component.Output, 1)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Input[0].Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Input[0].Schema.PackagePath)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Output[0].Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Output[0].Schema.PackagePath)
}

func TestLoader_LoadComponent_InheritsTypeContextPackageForNamedViewSchemas(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			TypeContext: &typectx.Context{
				DefaultPackage: "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one",
				PackagePath:    "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one",
			},
			Views: []*plan.View{
				{
					Name:        "foos",
					Holder:      "Foos",
					SchemaType:  "*FoosView",
					Cardinality: string(state.Many),
					FieldType:   nil,
					ElementType: nil,
					SQL:         "SELECT * FROM FOOS",
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	root, err := artifact.Resource.Views.Index().Lookup("foos")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", root.Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", root.Schema.PackagePath)
}

func TestLoader_LoadComponent_PreservesComponentHolderTypes(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &typedRouteSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Equal(t, reflect.TypeOf(typedRouteInput{}), component.ComponentRoutes[0].InputType)
	assert.Equal(t, reflect.TypeOf(typedRouteOutput{}), component.ComponentRoutes[0].OutputType)
	assert.Empty(t, component.ComponentRoutes[0].InputName)
	assert.Empty(t, component.ComponentRoutes[0].OutputName)
}

func TestLoader_LoadComponent_PreservesDynamicComponentHolderTypes(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &dynamicRouteSource{
		Route: xdatly.Component[any, any]{
			Inout:  dynamicRouteInput{},
			Output: dynamicRouteOutput{},
		},
	}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Equal(t, reflect.TypeOf(dynamicRouteInput{}), component.ComponentRoutes[0].InputType)
	assert.Equal(t, reflect.TypeOf(dynamicRouteOutput{}), component.ComponentRoutes[0].OutputType)
}

func TestLoader_LoadComponent_PreservesDynamicComponentHolderExplicitNames(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		embeddedFS
		Rows  []reportRow                `view:"rows,table=REPORT" sql:"uri=testdata/report.sql"`
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
	}{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Nil(t, component.ComponentRoutes[0].InputType)
	assert.Nil(t, component.ComponentRoutes[0].OutputType)
	assert.Equal(t, "ReportInput", component.ComponentRoutes[0].InputName)
	assert.Equal(t, "ReportOutput", component.ComponentRoutes[0].OutputName)
}

func TestLoader_LoadComponent_PreservesDynamicComponentHolderExplicitNamesFromRegistry(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(namedDynamicRouteInput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/load"), x.WithName("ReportInput")))
	registry.Register(x.NewType(reflect.TypeOf(namedDynamicRouteOutput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/load"), x.WithName("ReportOutput")))

	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{
		Struct: &struct {
			Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
		}{},
		TypeRegistry: registry,
	})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ComponentRoutes, 1)
	assert.Equal(t, reflect.TypeOf(namedDynamicRouteInput{}), component.ComponentRoutes[0].InputType)
	assert.Equal(t, reflect.TypeOf(namedDynamicRouteOutput{}), component.ComponentRoutes[0].OutputType)
	assert.Equal(t, "ReportInput", component.ComponentRoutes[0].InputName)
	assert.Equal(t, "ReportOutput", component.ComponentRoutes[0].OutputName)
	require.Len(t, component.Input, 1)
	assert.Equal(t, "name", component.Input[0].Name)
	require.Len(t, component.Output, 2)
	require.Len(t, artifact.Resource.Views, 1)
	assert.Equal(t, "rows", artifact.Resource.Views[0].Name)
}

func TestLoader_LoadComponent_ErrorsOnMultipleComponentRoutes(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		embeddedFS
		Rows   []reportRow `view:"rows,table=REPORT" sql:"uri=testdata/report.sql"`
		RouteA struct{}    `component:",path=/v1/api/dev/report-a,method=GET"`
		RouteB struct{}    `component:",path=/v1/api/dev/report-b,method=POST"`
	}{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	_, err = loader.LoadComponent(context.Background(), planned)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple component routes are not supported")
}

func TestLoader_LoadComponent_RouterOnlySourceSynthesizesStatesAndViews(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &routerOnlySource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	assert.Equal(t, "/v1/api/dev/router-only", component.URI)
	assert.Equal(t, "GET", component.Method)
	require.Len(t, component.Input, 1)
	assert.Equal(t, "id", component.Input[0].Name)
	require.Len(t, component.Output, 2)
	require.Len(t, artifact.Resource.Views, 1)
	assert.Equal(t, "rows", artifact.Resource.Views[0].Name)
}

func TestLoader_LoadComponent_SynthesizesStatesFromRouteContractsWhenPlanStatesAreEmpty(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "team"},
		Plan: &plan.Result{
			Components: []*plan.ComponentRoute{
				{
					FieldName:  "Team",
					Name:       "Team",
					RoutePath:  "/v1/api/dev/team/{teamID}",
					Method:     "DELETE",
					InputType:  reflect.TypeOf(typedTeamRouteInput{}),
					OutputType: reflect.TypeOf(typedTeamRouteOutput{}),
					ViewName:   "Team",
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.Input, 1)
	assert.Equal(t, "TeamID", component.Input[0].Name)
	require.NotNil(t, component.Input[0].In)
	assert.Equal(t, state.KindPath, component.Input[0].In.Kind)
	assert.Equal(t, "teamID", component.Input[0].In.Name)
}

func TestLoader_LoadComponent_CacheProviderDoesNotBindRootView(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/shape/dev/vendors/"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]any{}),
					ElementType: reflect.TypeOf(map[string]any{}),
					SQL:         "SELECT * FROM VENDOR",
				},
			},
			Directives: &dqlshape.Directives{
				Cache: &dqlshape.CacheDirective{
					Enabled:      true,
					Name:         "aerospike",
					Provider:     "aerospike://127.0.0.1:3000/test",
					Location:     "${view.Name}",
					TimeToLiveMs: 3600000,
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)
	require.Len(t, artifact.Resource.Views, 1)
	require.NotEmpty(t, artifact.Resource.CacheProviders)

	root := artifact.Resource.Views[0]
	assert.Nil(t, root.Cache)
}

func TestLoader_LoadViews_DoesNotSeedPlaceholderColumnsFromLinkedType(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "districts"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "districts",
					Table:       "DISTRICT",
					Cardinality: "many",
					SchemaType:  "*DistrictsView",
					FieldType:   reflect.TypeOf([]*placeholderDistrictRow{}),
					ElementType: reflect.TypeOf(placeholderDistrictRow{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifact, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.Len(t, artifact.Views, 1)
	assert.Empty(t, artifact.Views[0].Columns)
}

func TestLoader_LoadViews_DefersMapBackedQuerySchemaType(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "cities"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "cities",
					Table:       "CITY",
					Mode:        string(view.ModeQuery),
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "SELECT * FROM CITY",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifact, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.Len(t, artifact.Views, 1)
	assert.Nil(t, artifact.Views[0].Schema.Type())
}

func TestLoader_LoadViews_PreservesChildSummaryGraph(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "SELECT * FROM VENDOR",
					Relations: []*plan.Relation{
						{
							Name:   "products",
							Parent: "vendor",
							Holder: "Products",
							Ref:    "products",
							Table:  "PRODUCT",
							On: []*plan.RelationLink{
								{ParentColumn: "ID", RefColumn: "VENDOR_ID"},
							},
						},
					},
				},
				{
					Name:        "products",
					Table:       "PRODUCT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "SELECT * FROM PRODUCT",
					Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) PROD_META GROUP BY VENDOR_ID",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 2)

	index := artifacts.Resource.Views.Index()
	products, err := index.Lookup("products")
	require.NoError(t, err)
	require.NotNil(t, products)
	require.NotNil(t, products.Template)
	require.NotNil(t, products.Template.Summary)
	assert.Contains(t, products.Template.Summary.Source, "TOTAL_PRODUCTS")
	assert.Contains(t, products.Template.Summary.Source, "$View.products.SQL")
}

func TestLoader_LoadComponent_ConstDirectiveCreatesInternalConstParameter(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/shape/dev/vendors-env/"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]any{}),
					ElementType: reflect.TypeOf(map[string]any{}),
					SQL:         "SELECT * FROM VENDOR",
				},
			},
			Directives: &dqlshape.Directives{
				Const: map[string]string{
					"Vendor": "VENDOR",
				},
			},
			Const: map[string]string{
				"Vendor": "VENDOR",
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)
	require.NotEmpty(t, artifact.Resource.Parameters)
	require.NotEmpty(t, artifact.Component)

	var constParam *state.Parameter
	for _, item := range artifact.Resource.Parameters {
		if item != nil && item.Name == "Vendor" && item.In != nil && item.In.Kind == state.KindConst {
			constParam = item
			break
		}
	}
	require.NotNil(t, constParam)
	assert.Equal(t, "VENDOR", constParam.Value)
	assert.Equal(t, `internal:"true"`, constParam.Tag)
	require.NotNil(t, constParam.Schema)
	assert.Equal(t, "string", constParam.Schema.DataType)
	assert.Equal(t, state.One, constParam.Schema.Cardinality)

	loaded, ok := ComponentFrom(artifact)
	require.True(t, ok)
	var constInput *plan.State
	for _, item := range loaded.Input {
		if item != nil && item.Name == "Vendor" && item.In != nil && item.In.Kind == state.KindConst {
			constInput = item
			break
		}
	}
	require.NotNil(t, constInput)
	assert.Equal(t, "VENDOR", constInput.Value)
	assert.Equal(t, `internal:"true"`, constInput.Tag)
	require.NotNil(t, constInput.Schema)
	assert.Equal(t, "string", constInput.Schema.DataType)
	assert.Equal(t, state.One, constInput.Schema.Cardinality)
}

func TestLoader_LoadComponent_ViewKindStateIsInput(t *testing.T) {
	required := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/auth/vendors/{vendorID}"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]any{}),
					ElementType: reflect.TypeOf(map[string]any{}),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:            "Jwt",
						In:              state.NewHeaderLocation("Authorization"),
						Required:        &required,
						ErrorStatusCode: 401,
						Schema:          &state.Schema{DataType: "string"},
					},
				},
				{
					Parameter: state.Parameter{
						Name:            "Authorization",
						In:              state.NewViewLocation("Authorization"),
						Required:        &required,
						ErrorStatusCode: 403,
						Schema:          &state.Schema{Cardinality: state.Many},
					},
				},
				{
					Parameter: state.Parameter{
						Name:     "VendorID",
						In:       state.NewPathLocation("vendorID"),
						Required: &required,
						Schema:   &state.Schema{DataType: "int"},
					},
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)

	require.Len(t, component.Input, 3)
	var hasViewInput bool
	for _, input := range component.Input {
		if input != nil && input.In != nil && input.In.Kind == state.KindView && input.Name == "Authorization" {
			hasViewInput = true
			assert.Equal(t, 403, input.ErrorStatusCode)
			break
		}
	}
	assert.True(t, hasViewInput)
}

func TestResolveTypeSpecs_ViewOverridesAndInheritance(t *testing.T) {
	result := &plan.Result{
		Directives: &dqlshape.Directives{Dest: "all.go"},
		Views: []*plan.View{
			{
				Name: "vendor",
				Path: "vendor",
				Declaration: &plan.ViewDeclaration{
					Dest:     "vendor.go",
					TypeName: "Vendor",
				},
			},
			{Name: "products", Path: "vendor.products"},
		},
	}
	specs := resolveTypeSpecs(result)
	require.NotNil(t, specs)
	require.NotNil(t, specs["view:vendor"])
	assert.Equal(t, "Vendor", specs["view:vendor"].TypeName)
	assert.Equal(t, "vendor.go", specs["view:vendor"].Dest)
	require.NotNil(t, specs["view:products"])
	assert.Equal(t, "vendor.go", specs["view:products"].Dest)
	assert.True(t, specs["view:products"].Inherited)
}

func TestLoader_LoadComponent_RelationFieldsPreserved(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/report"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "Rows",
					Name:        "rows",
					Table:       "REPORT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Relations: []*plan.Relation{
						{
							Name:   "detail",
							Holder: "Detail",
							Ref:    "detail",
							Table:  "REPORT_DETAIL",
							On: []*plan.RelationLink{
								{
									ParentField:     "ReportID",
									ParentNamespace: "rows",
									ParentColumn:    "REPORT_ID",
									RefField:        "ID",
									RefNamespace:    "detail",
									RefColumn:       "ID",
								},
							},
						},
					},
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ViewRelations, 1)
	require.Len(t, component.ViewRelations[0].On, 1)
	require.Len(t, component.ViewRelations[0].Of.On, 1)

	parent := component.ViewRelations[0].On[0]
	ref := component.ViewRelations[0].Of.On[0]
	assert.Equal(t, "ReportID", parent.Field)
	assert.Equal(t, "rows", parent.Namespace)
	assert.Equal(t, "REPORT_ID", parent.Column)
	assert.Equal(t, "ID", ref.Field)
	assert.Equal(t, "detail", ref.Namespace)
	assert.Equal(t, "ID", ref.Column)
}

func TestLoader_LoadComponent_AttachesRelationTreeByParent(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/tree"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "root",
					Name:        "root",
					Table:       "ROOT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]relationTreeRoot{}),
					ElementType: reflect.TypeOf(relationTreeRoot{}),
					Relations: []*plan.Relation{
						{
							Name:   "child",
							Parent: "root",
							Holder: "Child",
							Ref:    "child",
							Table:  "CHILD",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "root",
									ParentColumn:    "ID",
									RefNamespace:    "child",
									RefField:        "RootID",
									RefColumn:       "RootID",
								},
							},
						},
						{
							Name:   "grand_child",
							Parent: "child",
							Holder: "GrandChild",
							Ref:    "grand_child",
							Table:  "GRAND_CHILD",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "child",
									ParentColumn:    "ID",
									RefNamespace:    "grand_child",
									RefField:        "ChildID",
									RefColumn:       "ChildID",
								},
							},
						},
					},
				},
				{
					Path:        "child",
					Name:        "child",
					Table:       "CHILD",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]relationTreeChild{}),
					ElementType: reflect.TypeOf(relationTreeChild{}),
				},
				{
					Path:        "grand_child",
					Name:        "grand_child",
					Table:       "GRAND_CHILD",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]relationTreeGrandChild{}),
					ElementType: reflect.TypeOf(relationTreeGrandChild{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}
	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)

	index := artifact.Resource.Views.Index()
	root, err := index.Lookup("root")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 1)
	assert.Equal(t, "child", root.With[0].Of.View.Ref)

	child, err := index.Lookup("child")
	require.NoError(t, err)
	require.NotNil(t, child)
	require.Len(t, child.With, 1)
	assert.Equal(t, "grand_child", child.With[0].Of.View.Ref)
}

func TestLoader_LoadComponent_AugmentsRelationHolderField(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/districts"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "districts",
					Name:        "districts",
					Table:       "DISTRICT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Relations: []*plan.Relation{
						{
							Name:   "cities",
							Parent: "districts",
							Holder: "Cities",
							Ref:    "cities",
							Table:  "CITY",
							On: []*plan.RelationLink{
								{
									ParentField:  "ID",
									ParentColumn: "ID",
									RefField:     "DistrictID",
									RefColumn:    "DistrictID",
								},
							},
						},
					},
				},
				{
					Path:        "cities",
					Name:        "cities",
					Table:       "CITY",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]cityRow{}),
					ElementType: reflect.TypeOf(cityRow{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}
	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)

	index := artifact.Resource.Views.Index()
	root, err := index.Lookup("districts")
	require.NoError(t, err)
	require.NotNil(t, root)
	compType := root.ComponentType()
	require.NotNil(t, compType)
	field, ok := compType.FieldByName("Cities")
	require.True(t, ok)
	assert.Equal(t, reflect.Slice, field.Type.Kind())
	assert.Equal(t, reflect.Ptr, field.Type.Elem().Kind())
	assert.Equal(t, "cityRow", field.Type.Elem().Elem().Name())
	assert.Contains(t, string(field.Tag), `view:",table=CITY"`)
	assert.Contains(t, string(field.Tag), `on:"ID:ID=DistrictID:DistrictID"`)
}

func TestLoader_LoadComponent_AugmentsRelationHolderField_ForMapBackedChildView(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/districts"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "districts",
					Name:        "districts",
					Table:       "DISTRICT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]placeholderDistrictRow{}),
					ElementType: reflect.TypeOf(placeholderDistrictRow{}),
					Relations: []*plan.Relation{
						{
							Name:   "cities",
							Parent: "districts",
							Holder: "Cities",
							Ref:    "cities",
							Table:  "CITY",
							On: []*plan.RelationLink{
								{
									ParentField:  "ID",
									ParentColumn: "ID",
									RefField:     "DistrictID",
									RefColumn:    "DISTRICT_ID",
								},
							},
						},
					},
				},
				{
					Path:        "cities",
					Name:        "cities",
					Table:       "CITY",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	require.NotNil(t, artifact)

	root, err := artifact.Resource.Views.Index().Lookup("districts")
	require.NoError(t, err)
	require.NotNil(t, root)

	compType := root.ComponentType()
	require.NotNil(t, compType)

	field, ok := compType.FieldByName("Cities")
	require.True(t, ok)
	assert.Equal(t, reflect.Slice, field.Type.Kind())
	assert.Equal(t, reflect.Struct, field.Type.Elem().Kind())
	assert.Contains(t, string(field.Tag), `view:",table=CITY"`)
}

func TestLoader_LoadComponent_AugmentsResolvedParentRelationHolderField(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/vendor-details"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "wrapper",
					Name:        "wrapper",
					Table:       "WRAPPER",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Relations: []*plan.Relation{
						{
							Name:   "products",
							Holder: "Products",
							Ref:    "products",
							Table:  "PRODUCT",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "vendor",
									ParentField:     "ID",
									ParentColumn:    "ID",
									RefNamespace:    "products",
									RefField:        "VendorID",
									RefColumn:       "VENDOR_ID",
								},
							},
						},
					},
				},
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
				},
				{
					Path:        "products",
					Name:        "products",
					Table:       "PRODUCT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]vendorProductRow{}),
					ElementType: reflect.TypeOf(vendorProductRow{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	index := artifact.Resource.Views.Index()
	vendor, err := index.Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, vendor)
	compType := vendor.ComponentType()
	require.NotNil(t, compType)
	field, ok := compType.FieldByName("Products")
	require.True(t, ok)
	assert.Equal(t, reflect.Slice, field.Type.Kind())
	assert.Equal(t, reflect.Ptr, field.Type.Elem().Kind())
	assert.Equal(t, "vendorProductRow", field.Type.Elem().Elem().Name())
}

func TestLoader_LoadComponent_InfersOneToOneOnSameTableJoin(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/vendor-details"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "wrapper",
					Name:        "wrapper",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					Relations: []*plan.Relation{
						{
							Name:   "vendor",
							Parent: "wrapper",
							Holder: "Vendor",
							Ref:    "vendor",
							Table:  "VENDOR",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "wrapper",
									ParentColumn:    "ID",
									RefNamespace:    "vendor",
									RefColumn:       "ID",
								},
							},
						},
						{
							Name:   "setting",
							Parent: "wrapper",
							Holder: "Setting",
							Ref:    "setting",
							Table:  "T",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "wrapper",
									ParentColumn:    "ID",
									RefNamespace:    "setting",
									RefColumn:       "ID",
								},
							},
						},
					},
				},
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
				},
				{
					Path:        "setting",
					Name:        "setting",
					Table:       "T",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)

	index := artifact.Resource.Views.Index()
	root, err := index.Lookup("wrapper")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 2)

	assert.Equal(t, state.One, root.With[0].Cardinality)
	assert.Equal(t, state.Many, root.With[1].Cardinality)
}

func TestLoader_LoadComponent_IncludesComponentStateInInput(t *testing.T) {
	required := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/vendor"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:     "Auth",
						In:       state.NewComponent("GET:/v1/api/dev/auth"),
						Required: &required,
						Schema:   &state.Schema{DataType: "*Output", Package: "auth"},
					},
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)
	require.Len(t, component.Input, 1)
	assert.Equal(t, state.KindComponent, component.Input[0].In.Kind)
	assert.Equal(t, "Auth", component.Input[0].Name)
}

func TestLoader_LoadComponent_MaterializesOutputStatusSchema(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/user"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "user",
					Name:        "user",
					Table:       "USER",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Status",
						In:   state.NewOutputLocation("status"),
						Tag:  `anonymous:"true"`,
					},
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)

	param, err := artifact.Resource.LookupParameter("Status")
	require.NoError(t, err)
	require.NotNil(t, param)
	require.NotNil(t, param.Schema)
	require.NotNil(t, param.Schema.Type())
	assert.Equal(t, "Status", param.Schema.Type().Name())
}

func TestLoader_LoadComponent_PreservesExplicitOutputViewCardinality(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/auth/user-acl"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "user_acl",
					Name:        "user_acl",
					Table:       "USER_ACL",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Data",
						In:   state.NewOutputLocation("view"),
						Schema: &state.Schema{
							Cardinality: state.One,
						},
					},
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)
	require.Len(t, component.Output, 1)
	require.NotNil(t, component.Output[0].Schema)
	assert.Equal(t, state.One, component.Output[0].Schema.Cardinality)
}

func TestLoader_LoadComponent_RequiredViewInputDefaultsToOneCardinality(t *testing.T) {
	required := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/auth/vendor"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "Authorization",
					Name:        "Authorization",
					Table:       "AUTH",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:     "Authorization",
						In:       state.NewViewLocation("Authorization"),
						Required: &required,
					},
				},
			},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)
	require.Len(t, component.Input, 1)
	require.NotNil(t, component.Input[0].Schema)
	assert.Equal(t, state.One, component.Input[0].Schema.Cardinality)
}

func TestLoader_LoadComponent_UsesPlannedRefCardinality(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/vendor-meta"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "vendor",
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					Relations: []*plan.Relation{
						{
							Name:   "products_meta",
							Parent: "vendor",
							Holder: "ProductsMeta",
							Ref:    "products_meta",
							Table:  "PRODUCT",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "vendor",
									ParentColumn:    "ID",
									RefNamespace:    "products_meta",
									RefColumn:       "VENDOR_ID",
								},
							},
						},
					},
				},
				{
					Path:        "products_meta",
					Name:        "products_meta",
					Table:       "PRODUCT",
					Cardinality: "one",
					FieldType:   reflect.TypeOf(map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)

	index := artifact.Resource.Views.Index()
	root, err := index.Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 1)
	assert.Equal(t, state.One, root.With[0].Cardinality)
}

func TestLoader_LoadComponent_SynthesizesRootViewFromComponentRoute(t *testing.T) {
	baseDir := t.TempDir()
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(reportRow{}), x.WithPkgPath("example.com/routes"), x.WithName("ReportView")))

	artifact, err := New().LoadComponent(context.Background(), &shape.PlanResult{
		Source: &shape.Source{
			Path:         filepath.Join(baseDir, "router.go"),
			TypeRegistry: registry,
		},
		Plan: &plan.Result{
			Components: []*plan.ComponentRoute{{
				Name:      "Report",
				RoutePath: "/v1/api/report",
				Method:    "DELETE",
				Connector: "dev",
				ViewName:  "example.com/routes.ReportView",
				SourceURL: "report/report.sql",
			}},
		},
	})
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Equal(t, "Report", component.RootView)
	require.Len(t, artifact.Resource.Views, 1)
	require.Equal(t, "Report", artifact.Resource.Views[0].Name)
	require.NotNil(t, artifact.Resource.Views[0].Template)
	require.Equal(t, filepath.Join(baseDir, "report", "report.sql"), artifact.Resource.Views[0].Template.SourceURL)
}

func TestLoader_LoadComponent_AllowsViewlessComponentRoute(t *testing.T) {
	artifact, err := New().LoadComponent(context.Background(), &shape.PlanResult{
		Source: &shape.Source{Name: "delete_team"},
		Plan: &plan.Result{
			Components: []*plan.ComponentRoute{
				{
					Name:      "Team",
					Method:    "DELETE",
					RoutePath: "/v1/api/dev/team/{teamID}",
					Connector: "dev",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, artifact)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	assert.Equal(t, "DELETE", component.Method)
	assert.Equal(t, "/v1/api/dev/team/{teamID}", component.URI)
	assert.Empty(t, artifact.Resource.Views)
}
