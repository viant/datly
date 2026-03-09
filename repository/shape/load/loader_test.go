package load

import (
	"context"
	"embed"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type metaSummaryRow struct {
	PageCnt int `sqlx:"name=PAGE_CNT"`
	Cnt     int `sqlx:"name=CNT"`
}

type productsMetaSummaryRow struct {
	VendorID      int `sqlx:"name=VENDOR_ID"`
	PageCnt       int `sqlx:"name=PAGE_CNT"`
	TotalProducts int `sqlx:"name=TOTAL_PRODUCTS"`
}

type productsOwnerPointerRow struct {
	VendorID *int `sqlx:"name=VENDOR_ID"`
}

type vendorSummaryParentRow struct {
	ID int `sqlx:"name=ID"`
}

type vendorSummaryChildRow struct {
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

type selectorHolderSource struct {
	Rows       []reportRow                                         `view:"rows,table=REPORT" sql:"uri=testdata/report.sql"`
	Route      xdatly.Component[typedRouteInput, typedRouteOutput] `component:",path=/v1/api/dev/report,method=GET"`
	ViewSelect struct {
		Fields []string `parameter:"fields,kind=query,in=_fields,cacheable=false"`
		Page   int      `parameter:"page,kind=query,in=_page,cacheable=false"`
	} `querySelector:"rows"`
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

func TestLoader_LoadResource(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "report"},
		Plan: &plan.Result{
			EmbedFS: &testFS,
			Views: []*plan.View{
				{
					Name:        "rows",
					Table:       "REPORT",
					Connector:   "dev",
					SQL:         "SELECT ID, NAME FROM REPORT",
					SQLURI:      "testdata/report.sql",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
				},
			},
			ViewsByName: map[string]*plan.View{"rows": {Name: "rows"}},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadResource(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)
	require.Len(t, artifacts.Resource.Views, 1)
	assert.Equal(t, "rows", artifacts.Resource.Views[0].Name)
	assert.Equal(t, "REPORT", artifacts.Resource.Views[0].Table)
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
	groupable := true
	criteria := true
	projection := true
	orderBy := true
	offset := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:               "items",
					Table:              "ITEMS",
					Module:             "platform/items",
					AllowNulls:         &allowNulls,
					Groupable:          &groupable,
					SelectorNamespace:  "it",
					SelectorNoLimit:    &noLimit,
					SelectorCriteria:   &criteria,
					SelectorProjection: &projection,
					SelectorOrderBy:    &orderBy,
					SelectorOffset:     &offset,
					SelectorFilterable: []string{"*"},
					SelectorOrderByColumns: map[string]string{
						"accountId": "ACCOUNT_ID",
					},
					SchemaType:  "*ItemView",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "SELECT * FROM ITEMS",
					Declaration: &plan.ViewDeclaration{
						ColumnsConfig: map[string]*plan.ViewColumnConfig{
							"AUTHORIZED": {
								DataType:  "bool",
								Tag:       `internal:"true"`,
								Groupable: &groupable,
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
	assert.True(t, actual.Groupable)
	require.NotNil(t, actual.Selector)
	assert.Equal(t, "it", actual.Selector.Namespace)
	assert.True(t, actual.Selector.NoLimit)
	require.NotNil(t, actual.Selector.Constraints)
	assert.True(t, actual.Selector.Constraints.Limit)
	assert.True(t, actual.Selector.Constraints.Criteria)
	assert.True(t, actual.Selector.Constraints.Projection)
	assert.True(t, actual.Selector.Constraints.OrderBy)
	assert.True(t, actual.Selector.Constraints.Offset)
	assert.Equal(t, []string{"*"}, actual.Selector.Constraints.Filterable)
	assert.Equal(t, "ACCOUNT_ID", actual.Selector.Constraints.OrderByColumn["accountId"])
	require.NotNil(t, actual.Schema)
	assert.Equal(t, "*ItemView", actual.Schema.DataType)
	require.NotNil(t, actual.ColumnsConfig)
	require.Contains(t, actual.ColumnsConfig, "AUTHORIZED")
	require.NotNil(t, actual.ColumnsConfig["AUTHORIZED"].DataType)
	assert.Equal(t, "bool", *actual.ColumnsConfig["AUTHORIZED"].DataType)
	require.NotNil(t, actual.ColumnsConfig["AUTHORIZED"].Tag)
	assert.Equal(t, `internal:"true"`, *actual.ColumnsConfig["AUTHORIZED"].Tag)
	require.NotNil(t, actual.ColumnsConfig["AUTHORIZED"].Groupable)
	assert.True(t, *actual.ColumnsConfig["AUTHORIZED"].Groupable)
}

func TestLoader_LoadViews_SelectorLimitEnablesConstraint(t *testing.T) {
	limit := 2
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "district"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:              "cities",
					Table:             "CITY",
					SelectorLimit:     &limit,
					SelectorNamespace: "ci",
					SchemaType:        "*CitiesView",
					Cardinality:       "many",
					FieldType:         reflect.TypeOf([]map[string]interface{}{}),
					ElementType:       reflect.TypeOf(map[string]interface{}{}),
					SQL:               "SELECT * FROM CITY",
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
	actual := artifacts.Views[0]
	require.NotNil(t, actual.Selector)
	require.NotNil(t, actual.Selector.Constraints)
	assert.Equal(t, 2, actual.Selector.Limit)
	assert.True(t, actual.Selector.Constraints.Limit)
}

func TestCloneRelationView_SelectorLimitUsesSingleParentBatch(t *testing.T) {
	ref, err := view.New("cities", "CITY")
	require.NoError(t, err)
	ref.Selector = &view.Config{
		Limit: 2,
		Constraints: &view.Constraints{
			Limit: true,
		},
	}
	cloned := cloneRelationView(ref, view.View{})
	require.NotNil(t, cloned.Batch)
	assert.Equal(t, 1, cloned.Batch.Size)
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
		TemplateType:     "patch",
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
	assert.Equal(t, "patch", component.Directives.TemplateType)
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
				PackageName:    "patch_basic_one",
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
	assert.Equal(t, "patch_basic_one", component.Input[0].Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Input[0].Schema.PackagePath)
	assert.Equal(t, "patch_basic_one", component.Output[0].Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one", component.Output[0].Schema.PackagePath)
}

func TestLoader_LoadComponent_DoesNotInheritTypeContextPackageForPrimitiveStateSchemas(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "district_pagination"},
		Plan: &plan.Result{
			TypeContext: &typectx.Context{
				DefaultPackage: "github.com/viant/datly/e2e/v1/shape/dev/district/pagination",
				PackagePath:    "github.com/viant/datly/e2e/v1/shape/dev/district/pagination",
			},
			Views: []*plan.View{
				{
					Name:        "districts",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Cardinality: string(state.Many),
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:   "IDs",
						In:     state.NewQueryLocation("IDs"),
						Schema: &state.Schema{DataType: "[]int"},
					},
				},
				{
					Parameter: state.Parameter{
						Name:   "Page",
						In:     state.NewQueryLocation("page"),
						Schema: &state.Schema{DataType: "int"},
					},
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.Input, 2)
	assert.Empty(t, component.Input[0].Schema.Package)
	assert.Empty(t, component.Input[0].Schema.PackagePath)
	assert.Empty(t, component.Input[1].Schema.Package)
	assert.Empty(t, component.Input[1].Schema.PackagePath)
}

func TestLoader_LoadComponent_MaterializesAnonymousBodySchemaIntoResourceParameters(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "foos",
					Holder:      "Foos",
					SchemaType:  "*FoosView",
					Cardinality: string(state.Many),
					SQL:         "SELECT * FROM FOOS",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Foos",
						In:   state.NewBodyLocation(""),
						Tag:  `anonymous:"true"`,
					},
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	param, err := artifact.Resource.LookupParameter("Foos")
	require.NoError(t, err)
	require.NotNil(t, param)
	require.NotNil(t, param.Schema)
	assert.Equal(t, "FoosView", param.Schema.Name)
	assert.Equal(t, "*FoosView", param.Schema.DataType)
}

func TestLoader_LoadComponent_InheritsTypeContextPackageForNamedViewSchemas(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			TypeContext: &typectx.Context{
				PackageName:    "patch_basic_one",
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
	assert.Equal(t, "patch_basic_one", root.Schema.Package)
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

func TestLoader_LoadComponent_SynthesizesMutableHelpersForPatchBodyRoute(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "foos",
					Holder:      "Foos",
					SchemaType:  "*FoosView",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Cardinality: string(state.Many),
					Table:       "FOOS",
					SQL:         "SELECT * FROM FOOS",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Foos",
						In:   state.NewBodyLocation(""),
						Tag:  `anonymous:"true"`,
						Schema: &state.Schema{
							Name:        "FoosView",
							DataType:    "*FoosView",
							Cardinality: state.One,
						},
					},
					EmitOutput: true,
				},
			},
			Components: []*plan.ComponentRoute{
				{
					Method:    "PATCH",
					RoutePath: "/v1/api/shape/dev/basic/foos",
					ViewName:  "FoosView",
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)

	root := lookupNamedResourceView(artifact.Resource, component.RootView)
	require.NotNil(t, root)
	assert.Equal(t, view.ModeExec, root.Mode)
	require.NotNil(t, root.Template)
	assert.True(t, root.Template.UseParameterStateType)
	require.NotNil(t, root.Template.Parameters.Lookup("CurFoosId"))
	require.NotNil(t, root.Template.Parameters.Lookup("CurFoos"))
	assert.Contains(t, root.Template.Source, `$sequencer.Allocate("FOOS", $Unsafe.Foos, "Id")`)
	assert.Contains(t, root.Template.Source, `#if($CurFoosById.HasKey($Unsafe.Foos.Id) == true)`)
	assert.Contains(t, root.Template.Source, `$sql.Update($Unsafe.Foos, "FOOS");`)
	assert.Contains(t, root.Template.Source, `$sql.Insert($Unsafe.Foos, "FOOS");`)
	assert.Equal(t, state.Many, root.Template.Parameters.Lookup("CurFoos").Schema.Cardinality)

	require.Nil(t, component.InputParameters().Lookup("CurFoosId"))
	require.Nil(t, component.InputParameters().Lookup("CurFoos"))
	require.NotNil(t, artifact.Resource.Parameters.Lookup("CurFoosId"))
	require.NotNil(t, artifact.Resource.Parameters.Lookup("CurFoos"))
	assert.Equal(t, "*FoosView", artifact.Resource.Parameters.Lookup("CurFoosId").Schema.DataType)
	require.NotNil(t, artifact.Resource.Parameters.Lookup("CurFoosId").Output)
	assert.Equal(t, "structql", artifact.Resource.Parameters.Lookup("CurFoosId").Output.Name)
	assert.Contains(t, artifact.Resource.Parameters.Lookup("CurFoosId").Output.Body, "SELECT ARRAY_AGG(Id) AS Values")
	assert.Equal(t, state.Many, artifact.Resource.Parameters.Lookup("CurFoosId").Schema.Cardinality)
	assert.Equal(t, state.One, artifact.Resource.Parameters.Lookup("CurFoosId").Output.Schema.Cardinality)
	assert.Equal(t, state.Many, artifact.Resource.Parameters.Lookup("CurFoos").Schema.Cardinality)
	require.Len(t, component.Output, 1)
	require.Equal(t, "Foos", component.Output[0].Name)
	require.Equal(t, state.KindRequestBody, component.Output[0].In.Kind)

	curFoos, err := artifact.Resource.View("CurFoos")
	require.NoError(t, err)
	require.NotNil(t, curFoos)
	require.NotNil(t, curFoos.Template)
	assert.Equal(t, "foos/cur_foos.sql", curFoos.Template.SourceURL)
	require.True(t, curFoos.Template.UseParameterStateType)
	require.True(t, curFoos.Template.DeclaredParametersOnly)
	require.True(t, curFoos.Template.UseResourceParameterLookup)
	require.NotNil(t, curFoos.Template.Parameters.Lookup("CurFoosId"))
	require.Nil(t, curFoos.Template.Parameters.Lookup("Foos"))
}

func TestLoader_LoadComponent_SynthesizesMutableHelpersForPatchManyBodyRoute(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_many"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "foos",
					Holder:      "Foos",
					SchemaType:  "*FoosView",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Cardinality: string(state.Many),
					Table:       "FOOS",
					SQL:         "SELECT * FROM FOOS",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Foos",
						In:   state.NewBodyLocation(""),
						Tag:  `anonymous:"true"`,
						Schema: &state.Schema{
							Name:        "FoosView",
							DataType:    "*FoosView",
							Cardinality: state.Many,
						},
					},
					EmitOutput: true,
				},
			},
			Components: []*plan.ComponentRoute{
				{
					Method:    "PATCH",
					RoutePath: "/v1/api/shape/dev/basic/foos-many",
					ViewName:  "FoosView",
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	curFoosID := artifact.Resource.Parameters.Lookup("CurFoosId")
	require.NotNil(t, curFoosID)
	require.NotNil(t, curFoosID.Schema)
	require.NotNil(t, curFoosID.Output)
	require.NotNil(t, curFoosID.Output.Schema)
	assert.Equal(t, state.Many, curFoosID.Schema.Cardinality)
	assert.Equal(t, state.One, curFoosID.Output.Schema.Cardinality)
	assert.Equal(t, "*FoosView", curFoosID.Schema.DataType)
	assert.Contains(t, curFoosID.Output.Body, "SELECT ARRAY_AGG(Id) AS Values")
	root := lookupNamedResourceView(artifact.Resource, "foos")
	require.NotNil(t, root)
	require.NotNil(t, root.Template)
	assert.Contains(t, root.Template.Source, `$sequencer.Allocate("FOOS", $Unsafe.Foos, "Id")`)
	assert.Contains(t, root.Template.Source, `#foreach($RecFoos in $Unsafe.Foos)`)
	assert.Contains(t, root.Template.Source, `#if($CurFoosById.HasKey($RecFoos.Id) == true)`)
	assert.Contains(t, root.Template.Source, `$sql.Update($RecFoos, "FOOS");`)
	assert.Contains(t, root.Template.Source, `$sql.Insert($RecFoos, "FOOS");`)
	require.NotNil(t, root.TableBatches)
	assert.True(t, root.TableBatches["FOOS"])
}

func TestLoader_LoadComponent_DoesNotSynthesizeMutableHelpersForScalarBodyRoute(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "product_update"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "product_update",
					Holder:      "ProductUpdate",
					SchemaType:  "*ProductUpdateView",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Cardinality: string(state.Many),
					Table:       "PRODUCT",
					SQL:         "UPDATE PRODUCT SET STATUS = $Status WHERE ID IN ($Ids)",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Ids",
						In:   state.NewBodyLocation("Ids"),
						Schema: &state.Schema{
							DataType:    "int",
							Cardinality: state.Many,
						},
					},
				},
				{
					Parameter: state.Parameter{
						Name: "Status",
						In:   state.NewBodyLocation("Status"),
						Schema: &state.Schema{
							DataType:    "int",
							Cardinality: state.One,
						},
					},
				},
				{
					Parameter: state.Parameter{
						Name: "Records",
						In:   state.NewViewLocation("Records"),
						Schema: &state.Schema{
							Name:        "RecordsView",
							DataType:    "*RecordsView",
							Cardinality: state.Many,
						},
					},
				},
			},
			Components: []*plan.ComponentRoute{
				{
					Method:    "POST",
					RoutePath: "/v1/api/shape/dev/auth/products",
					ViewName:  "ProductUpdateView",
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)

	root := lookupNamedResourceView(artifact.Resource, component.RootView)
	require.NotNil(t, root)
	require.NotNil(t, root.Template)
	require.Nil(t, root.Template.Parameters.Lookup("CurIdsId"))
	require.Nil(t, artifact.Resource.Parameters.Lookup("CurIdsId"))
	require.Nil(t, artifact.Resource.Parameters.Lookup("CurIds"))
}

func TestLoader_LoadComponent_UserMetadataPreservesBitColumnsFromSchemaType(t *testing.T) {
	projectRoot := t.TempDir()
	err := os.WriteFile(filepath.Join(projectRoot, "go.mod"), []byte("module github.com/acme/app\n\ngo 1.23.0\n"), 0o644)
	require.NoError(t, err)
	packageDir := filepath.Join(projectRoot, "shape", "dev", "user", "mysql_boolean")
	err = os.MkdirAll(packageDir, 0o755)
	require.NoError(t, err)
	sourcePath := filepath.Join(projectRoot, "routes", "dev", "user_metadata.dql")
	err = os.MkdirAll(filepath.Dir(sourcePath), 0o755)
	require.NoError(t, err)
	err = os.WriteFile(sourcePath, []byte("SELECT * FROM USER_METADATA"), 0o644)
	require.NoError(t, err)
	typeFile := `package mysql_boolean

import "github.com/viant/sqlx/types"

type UserMetadataView struct {
	Id int ` + "`sqlx:\"ID\"`" + `
	UserId *int ` + "`sqlx:\"USER_ID\"`" + `
	IsEnabled *types.BitBool ` + "`sqlx:\"IS_ENABLED\"`" + `
	IsActivated *types.BitBool ` + "`sqlx:\"IS_ACTIVATED\"`" + `
}
`
	err = os.WriteFile(filepath.Join(packageDir, "user_metadata.go"), []byte(typeFile), 0o644)
	require.NoError(t, err)

	artifact, err := New().LoadComponent(context.Background(), &shape.PlanResult{
		Source: &shape.Source{Name: "user_metadata", Path: sourcePath},
		Plan: &plan.Result{
			TypeContext: &typectx.Context{
				PackagePath: "github.com/acme/app/shape/dev/user/mysql_boolean",
			},
			Views: []*plan.View{
				{
					Name:        "user_metadata",
					Table:       "USER_METADATA",
					SchemaType:  "*UserMetadataView",
					Cardinality: string(state.Many),
					SQL:         "SELECT user_metadata.* FROM (SELECT * FROM USER_METADATA t) user_metadata",
				},
			},
		},
	})
	require.NoError(t, err)
	root, err := artifact.Resource.Views.Index().Lookup("user_metadata")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	require.NotNil(t, root.Schema.Type())

	names := make([]string, 0, len(root.Columns))
	for _, column := range root.Columns {
		if column == nil {
			continue
		}
		names = append(names, column.Name)
	}
	assert.Contains(t, names, "IS_ENABLED")
	assert.Contains(t, names, "IS_ACTIVATED")
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

func TestLoader_LoadViews_DefersPlaceholderStructQuerySchemaType(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "districts"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "districts",
					Table:       "DISTRICT",
					Mode:        string(view.ModeQuery),
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]placeholderDistrictRow{}),
					ElementType: reflect.TypeOf(placeholderDistrictRow{}),
					SQL:         "SELECT t.* FROM DISTRICT t",
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

func TestLoader_LoadComponent_DoesNotMaterializePlaceholderOutputViewSchema(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "districts"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "districts",
					Table:       "DISTRICT",
					Mode:        string(view.ModeQuery),
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]placeholderDistrictRow{}),
					ElementType: reflect.TypeOf(placeholderDistrictRow{}),
					SQL:         "SELECT t.* FROM DISTRICT t",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name: "Data",
						In:   state.NewOutputLocation("view"),
					},
				},
			},
		},
	}

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.Output, 1)
	require.NotNil(t, component.Output[0].Schema)
	assert.Nil(t, component.Output[0].Schema.Type())
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

func TestLoader_LoadViews_AttachesChildSummaryFieldToParentSchema(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(productsMetaSummaryRow{}), x.WithName("ProductsMetaView")))

	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta", TypeRegistry: registry},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]vendorSummaryParentRow{}),
					ElementType: reflect.TypeOf(vendorSummaryParentRow{}),
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
					FieldType:   reflect.TypeOf([]vendorSummaryChildRow{}),
					ElementType: reflect.TypeOf(vendorSummaryChildRow{}),
					SQL:         "SELECT * FROM PRODUCT",
					Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) PROD_META GROUP BY VENDOR_ID",
					SummaryName: "ProductsMeta",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 2)

	index := artifacts.Resource.Views.Index()
	root, err := index.Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	compType := root.Schema.CompType()
	require.NotNil(t, compType)
	field, ok := compType.FieldByName("ProductsMeta")
	require.True(t, ok)
	assert.Equal(t, `json:",omitempty" yaml:",omitempty" sqlx:"-"`, string(field.Tag))
}

func TestLoader_LoadResource_AssignsSummarySchemas(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(metaSummaryRow{}), x.WithName("MetaView")))
	registry.Register(x.NewType(reflect.TypeOf(productsMetaSummaryRow{}), x.WithName("ProductsMetaView")))

	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta", TypeRegistry: registry},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]map[string]interface{}{}),
					ElementType: reflect.TypeOf(map[string]interface{}{}),
					SQL:         "SELECT * FROM VENDOR",
					Summary:     "SELECT COUNT(*) AS CNT FROM ($View.NonWindowSQL) t",
					SummaryName: "Meta",
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
					SummaryName: "ProductsMeta",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:   "Meta",
						In:     state.NewOutputLocation("summary"),
						Schema: &state.Schema{DataType: "?"},
					},
				},
			},
			Components: []*plan.ComponentRoute{
				{
					RoutePath: "/v1/api/dev/meta/vendors-nested",
					Method:    "GET",
					ViewName:  "vendor",
					Name:      "vendor",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadResource(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)

	index := artifacts.Resource.Views.Index()
	root, err := index.Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Template)
	require.NotNil(t, root.Template.Summary)
	require.NotNil(t, root.Template.Summary.Schema)
	assert.Equal(t, "*load.metaSummaryRow", root.Template.Summary.Schema.Type().String())

	products, err := index.Lookup("products")
	require.NoError(t, err)
	require.NotNil(t, products)
	require.NotNil(t, products.Template)
	require.NotNil(t, products.Template.Summary)
	require.NotNil(t, products.Template.Summary.Schema)
	assert.Equal(t, "*load.productsMetaSummaryRow", products.Template.Summary.Schema.Type().String())
	productsSummaryType := products.Template.Summary.Schema.Type()
	if productsSummaryType.Kind() == reflect.Ptr {
		productsSummaryType = productsSummaryType.Elem()
	}
	productsSummaryField, ok := productsSummaryType.FieldByName("VendorID")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf(int(0)), productsSummaryField.Type)

	metaParam, err := artifacts.Resource.LookupParameter("Meta")
	require.NoError(t, err)
	require.NotNil(t, metaParam)
	require.NotNil(t, metaParam.Schema)
	require.NotNil(t, metaParam.Schema.Type())
	assert.Equal(t, "*load.metaSummaryRow", metaParam.Schema.Type().String())

	componentArtifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, componentArtifact)
	component, ok := componentArtifact.Component.(*Component)
	require.True(t, ok)
	require.NotEmpty(t, component.Output)
	require.NotNil(t, component.Output[0].Schema)
	require.NotNil(t, component.Output[0].Schema.Type())
	assert.Equal(t, "*load.metaSummaryRow", component.Output[0].Schema.Type().String())
}

func TestRefineSummarySchemas_PrefersOwnerSchemaFieldTypeOverDiscoveredColumn(t *testing.T) {
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name: "products",
				Schema: &state.Schema{
					Name:        "ProductsView",
					DataType:    "*ProductsView",
					Cardinality: state.Many,
				},
				Columns: []*view.Column{
					{Name: "VENDOR_ID", DatabaseColumn: "VENDOR_ID", DataType: "int"},
				},
				Template: &view.Template{
					Summary: &view.TemplateSummary{
						Name: "ProductsMeta",
						Schema: &state.Schema{
							Name:        "ProductsMetaView",
							DataType:    "*ProductsMetaView",
							Cardinality: state.One,
						},
					},
				},
			},
		},
	}
	resource.Views[0].Schema.SetType(reflect.TypeOf([]productsOwnerPointerRow{}))
	resource.Views[0].Template.Summary.Schema.SetType(reflect.TypeOf(productsMetaSummaryRow{}))

	RefineSummarySchemas(resource)

	summaryType := resource.Views[0].Template.Summary.Schema.Type()
	require.NotNil(t, summaryType)
	if summaryType.Kind() == reflect.Ptr {
		summaryType = summaryType.Elem()
	}
	field, ok := summaryType.FieldByName("VendorID")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)
}

func TestLoader_LoadResource_AttachesChildSummaryTemplateToRelationView(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(metaSummaryRow{}), x.WithName("MetaView")))
	registry.Register(x.NewType(reflect.TypeOf(productsMetaSummaryRow{}), x.WithName("ProductsMetaView")))

	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta", TypeRegistry: registry},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]vendorSummaryParentRow{}),
					ElementType: reflect.TypeOf(vendorSummaryParentRow{}),
					SQL:         "SELECT * FROM VENDOR",
					Summary:     "SELECT COUNT(*) AS CNT FROM ($View.NonWindowSQL) t",
					SummaryName: "Meta",
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
					FieldType:   reflect.TypeOf([]vendorSummaryChildRow{}),
					ElementType: reflect.TypeOf(vendorSummaryChildRow{}),
					SQL:         "SELECT * FROM PRODUCT",
					Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) PROD_META GROUP BY VENDOR_ID",
					SummaryName: "ProductsMeta",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadResource(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	root, err := artifacts.Resource.Views.Index().Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 1)
	require.NotNil(t, root.With[0].Of.Template)
	require.NotNil(t, root.With[0].Of.Template.Summary)
	require.NotNil(t, root.With[0].Of.Template.Summary.Schema)
	assert.Equal(t, "ProductsMetaView", root.With[0].Of.Template.Summary.Schema.Name)

	products, err := artifacts.Resource.Views.Index().Lookup("products")
	require.NoError(t, err)
	require.NotNil(t, products)
	require.NotNil(t, products.Template)
	require.NotNil(t, products.Template.Summary)
	require.NotNil(t, products.Template.Summary.Schema)

	relationSummaryType := root.With[0].Of.Template.Summary.Schema.Type()
	require.NotNil(t, relationSummaryType)
	if relationSummaryType.Kind() == reflect.Ptr {
		relationSummaryType = relationSummaryType.Elem()
	}
	relationField, ok := relationSummaryType.FieldByName("VendorID")
	require.True(t, ok)
	assert.NotEqual(t, "true", relationField.Tag.Get("internal"))

	standaloneSummaryType := products.Template.Summary.Schema.Type()
	require.NotNil(t, standaloneSummaryType)
	if standaloneSummaryType.Kind() == reflect.Ptr {
		standaloneSummaryType = standaloneSummaryType.Elem()
	}
	standaloneField, ok := standaloneSummaryType.FieldByName("VendorID")
	require.True(t, ok)
	assert.NotEqual(t, "true", standaloneField.Tag.Get("internal"))
	assert.Equal(t, standaloneSummaryType, relationSummaryType)
}

func TestLoader_LoadResource_MaterializesNamedResourceTypes(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(metaSummaryRow{}), x.WithName("MetaView")))
	registry.Register(x.NewType(reflect.TypeOf(productsMetaSummaryRow{}), x.WithName("ProductsMetaView")))

	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta", TypeRegistry: registry},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]vendorSummaryParentRow{}),
					ElementType: reflect.TypeOf(vendorSummaryParentRow{}),
					SQL:         "SELECT * FROM VENDOR",
					Summary:     "SELECT COUNT(*) AS CNT FROM ($View.NonWindowSQL) t",
					SummaryName: "Meta",
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
					FieldType:   reflect.TypeOf([]vendorSummaryChildRow{}),
					ElementType: reflect.TypeOf(vendorSummaryChildRow{}),
					SQL:         "SELECT * FROM PRODUCT",
					Summary:     "SELECT VENDOR_ID, COUNT(*) AS TOTAL_PRODUCTS FROM ($View.products.SQL) PROD_META GROUP BY VENDOR_ID",
					SummaryName: "ProductsMeta",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadResource(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)

	var actual []string
	for _, item := range artifacts.Resource.Types {
		if item == nil {
			continue
		}
		actual = append(actual, item.Name)
	}
	assert.ElementsMatch(t, []string{"VendorView", "MetaView", "ProductsView", "ProductsMetaView"}, actual)
}

func TestLoader_LoadViews_PreservesSummarySourceURL(t *testing.T) {
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
					SQLURI:      "vendor/vendor.sql",
					SummaryURL:  "vendor/vendor_summary.sql",
					SummaryName: "Meta",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 1)
	require.NotNil(t, artifacts.Views[0].Template)
	require.NotNil(t, artifacts.Views[0].Template.Summary)
	assert.Equal(t, "vendor/vendor_summary.sql", artifacts.Views[0].Template.Summary.SourceURL)
}

func TestLoader_LoadResource_TypedViewDefinitionsPreferSchemaFieldsOverColumns(t *testing.T) {
	type foosViewHas struct {
		Id       bool
		Name     bool
		Quantity bool
	}
	type foosView struct {
		Id       int          `sqlx:"ID" velty:"names=ID|Id"`
		Name     *string      `sqlx:"NAME" velty:"names=NAME|Name"`
		Quantity *int         `sqlx:"QUANTITY" velty:"names=QUANTITY|Quantity"`
		Has      *foosViewHas `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-" typeName:"FoosViewHas"`
	}

	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "patch_basic_one"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "foos",
					Table:       "FOOS",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]foosView{}),
					ElementType: reflect.TypeOf(foosView{}),
					SQL:         "SELECT * FROM FOOS",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadResource(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)

	var actual []string
	for _, item := range artifacts.Resource.Types {
		if item == nil || item.Name != "FoosView" {
			continue
		}
		for _, field := range item.Fields {
			actual = append(actual, field.Name)
		}
	}
	assert.Equal(t, []string{"Id", "Name", "Quantity", "Has"}, actual)
}

func TestLoader_LoadViews_InferRelationLinkFieldsFromSchemaTypes(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "vendor"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:        "vendor",
					Table:       "VENDOR",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					SQL:         "SELECT * FROM VENDOR",
					Relations: []*plan.Relation{
						{
							Name:   "products",
							Parent: "vendor",
							Holder: "Products",
							Ref:    "products",
							Table:  "PRODUCT",
							On: []*plan.RelationLink{
								{
									ParentNamespace: "vendor",
									ParentColumn:    "ID",
									RefNamespace:    "products",
									RefColumn:       "VENDOR_ID",
								},
							},
						},
					},
				},
				{
					Name:        "products",
					Table:       "PRODUCT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]vendorProductRow{}),
					ElementType: reflect.TypeOf(vendorProductRow{}),
					SQL:         "SELECT * FROM PRODUCT",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)

	root, err := artifacts.Resource.Views.Index().Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 1)
	require.Len(t, root.With[0].On, 1)
	require.Len(t, root.With[0].Of.On, 1)

	assert.Equal(t, "ID", root.With[0].On[0].Column)
	assert.Equal(t, "ID", root.With[0].On[0].Field)
	assert.Empty(t, root.With[0].On[0].Namespace)
	assert.Equal(t, "VENDOR_ID", root.With[0].Of.On[0].Column)
	assert.Equal(t, "VendorID", root.With[0].Of.On[0].Field)
	assert.Empty(t, root.With[0].Of.On[0].Namespace)
}

func TestLoader_LoadViews_FallsBackToColumnNamesForRelationLinkFields(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "vendor"},
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
								{
									ParentColumn: "ID",
									RefColumn:    "VENDOR_ID",
								},
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
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	artifacts, err := New().LoadViews(context.Background(), planned)
	require.NoError(t, err)

	root, err := artifacts.Resource.Views.Index().Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, root.With, 1)
	require.Len(t, root.With[0].On, 1)
	require.Len(t, root.With[0].Of.On, 1)

	assert.Equal(t, "Id", root.With[0].On[0].Field)
	assert.Equal(t, "VendorId", root.With[0].Of.On[0].Field)
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

func TestLoader_LoadComponent_ExecViewInputPreservesVeltyAliases(t *testing.T) {
	required := false
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "user_team"},
		Plan: &plan.Result{
			Components: []*plan.ComponentRoute{
				{
					Name:     "user_team",
					Method:   "PUT",
					Path:     "/v1/api/shape/dev/teams",
					ViewName: "user_team",
				},
			},
			States: []*plan.State{
				{
					Parameter: state.Parameter{
						Name:     "TeamIDs",
						In:       state.NewQueryLocation("TeamIDs"),
						Required: &required,
						Schema:   &state.Schema{DataType: "[]int", Cardinality: state.Many},
					},
				},
				{
					Parameter: state.Parameter{
						Name:     "TeamStats",
						In:       state.NewViewLocation("TeamStats"),
						Required: &required,
						Schema:   &state.Schema{Cardinality: state.Many},
					},
				},
			},
			Views: []*plan.View{
				{
					Name:        "user_team",
					Table:       "TEAM",
					Cardinality: "many",
					SQL:         "UPDATE TEAM SET ACTIVE = false",
					Mode:        string(view.ModeExec),
					FieldType: reflect.TypeOf([]struct {
						Id int `sqlx:"ID"`
					}{}),
					ElementType: reflect.TypeOf(struct {
						Id int `sqlx:"ID"`
					}{}),
				},
				{
					Name:             "TeamStats",
					Table:            "TEAM",
					Cardinality:      "many",
					Mode:             string(view.ModeQuery),
					ColumnsDiscovery: true,
				},
			},
			ByPath:      map[string]*plan.Field{},
			ViewsByName: map[string]*plan.View{},
		},
	}
	plannedResult, ok := plan.ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, plannedResult.Views, 2)
	plannedResult.Views[1].FieldType = reflect.TypeOf([]struct {
		Id          int     `sqlx:"ID"`
		TeamMembers int     `sqlx:"TEAM_MEMBERS"`
		Name        *string `sqlx:"NAME"`
	}{})
	plannedResult.Views[1].ElementType = plannedResult.Views[1].FieldType.Elem()

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)

	resourceView, err := artifact.Resource.View("TeamStats")
	require.NoError(t, err)
	require.NotNil(t, resourceView)
	require.NotNil(t, resourceView.Schema)
	resourceType := resourceView.Schema.Type()
	require.NotNil(t, resourceType)
	if resourceType.Kind() == reflect.Slice {
		resourceType = resourceType.Elem()
	}
	if resourceType.Kind() == reflect.Ptr {
		resourceType = resourceType.Elem()
	}
	require.Equal(t, reflect.Struct, resourceType.Kind())

	var inputParam *plan.State
	for _, item := range component.Input {
		if item != nil && strings.EqualFold(item.Name, "TeamStats") {
			inputParam = item
			break
		}
	}
	require.NotNil(t, inputParam)
	require.NotNil(t, inputParam.Schema)
	inputType := inputParam.Schema.Type()
	require.NotNil(t, inputType)
	if inputType.Kind() == reflect.Slice {
		inputType = inputType.Elem()
	}
	if inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}
	require.Equal(t, reflect.Struct, inputType.Kind())
	idField, ok := inputType.FieldByName("Id")
	require.True(t, ok)
	assert.Equal(t, "names=ID|Id", idField.Tag.Get("velty"))
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
	require.NotNil(t, component.Input[0].Schema)
	assert.Equal(t, reflect.TypeOf((*interface{})(nil)).Elem(), component.Input[0].Schema.Type())
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

func TestLoader_LoadComponent_QuerySelectorHolder(t *testing.T) {
	scanned, err := scan.New().Scan(context.Background(), &shape.Source{Struct: &selectorHolderSource{}})
	require.NoError(t, err)
	planned, err := plan.New().Plan(context.Background(), scanned)
	require.NoError(t, err)

	artifact, err := New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)

	require.Equal(t, []string{"fields", "page"}, component.QuerySelectors["rows"])
	fields := component.InputParameters().Lookup("fields")
	require.NotNil(t, fields)
	assert.Equal(t, state.KindQuery, fields.In.Kind)
	assert.Equal(t, "_fields", fields.In.Name)
	page := component.InputParameters().Lookup("page")
	require.NotNil(t, page)
	assert.Equal(t, "_page", page.In.Name)
}
