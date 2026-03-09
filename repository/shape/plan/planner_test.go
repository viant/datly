package plan

import (
	"context"
	"embed"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	asynckeys "github.com/viant/datly/repository/locator/async/keys"
	metakeys "github.com/viant/datly/repository/locator/meta/keys"
	outputkeys "github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/scan"
	"github.com/viant/datly/view/tags"
	"github.com/viant/x"
	"github.com/viant/xdatly"
)

//go:embed testdata/*.sql
var testFS embed.FS

type embeddedFS struct{}

func (embeddedFS) EmbedFS() *embed.FS {
	return &testFS
}

type reportRow struct {
	ID int
}

type reportSource struct {
	embeddedFS
	Rows   []reportRow `view:"rows,table=REPORT,connector=dev" sql:"uri=testdata/report.sql"`
	Status interface{} `parameter:"status,kind=output,in=status"`
	Job    interface{} `parameter:"job,kind=async,in=job"`
	VName  interface{} `parameter:"viewName,kind=meta,in=view.name"`
	ID     int         `parameter:"id,kind=query,in=id"`
	Route  struct{}    `component:",path=/v1/api/dev/report,method=GET,connector=dev"`
}

type relationRow struct {
	ID int
}

type relationSource struct {
	Rows []relationRow `view:"rows,table=REPORT" on:"rows.report_id=report.id"`
}

type relationSourceWithFields struct {
	Rows []relationRow `view:"rows,table=REPORT" on:"ReportID:rows.report_id=ID:report.id"`
}

type viewTypeDestSource struct {
	Rows []relationRow `view:"rows,table=REPORT,type=ReportRow,dest=rows.go"`
}

type typedRouteInput struct {
	ID int
}

type typedRouteOutput struct {
	Data []reportRow
}

type typedRouteSource struct {
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
	Count int `parameter:"count,kind=output,in=view"`
}

type taggedComponentStateSource struct {
	Auth interface{} `parameter:",kind=component,in=GET:/v1/api/dev/auth" typeName:"github.com/acme/auth.UserAclOutput"`
}

type constStateSource struct {
	Product string `parameter:",kind=const,in=Product" value:"PRODUCT" internal:"true"`
}

type codecStateSource struct {
	Jwt string `parameter:",kind=header,in=Authorization,errorCode=401" codec:"JwtClaim"`
	Run string `parameter:",kind=body,in=run" handler:"Exec"`
}

type selectorHolderSource struct {
	Route      xdatly.Component[typedRouteInput, typedRouteOutput] `component:",path=/v1/api/dev/report,method=GET"`
	ViewSelect struct {
		Fields []string `parameter:"fields,kind=query,in=_fields"`
		Page   int      `parameter:"page,kind=query,in=_page"`
	} `querySelector:"rows"`
}

type summaryViewSource struct {
	embeddedFS
	Rows []relationRow `view:"rows,table=REPORT,summaryURI=testdata/report_summary.sql" sql:"uri=testdata/report.sql"`
}

func TestPlanner_Plan(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.NotNil(t, result)
	require.NotNil(t, result.EmbedFS)

	require.Len(t, result.Views, 1)
	rows := result.Views[0]
	assert.Equal(t, "rows", rows.Name)
	assert.Equal(t, "REPORT", rows.Table)
	assert.Equal(t, "dev", rows.Connector)
	assert.Equal(t, "many", rows.Cardinality)
	assert.Equal(t, "Rows", rows.Holder)
	assert.Contains(t, rows.SQL, "SELECT ID")

	stateByPath := map[string]*State{}
	for _, item := range result.States {
		stateByPath[strings.ToLower(item.Name)] = item
	}

	require.NotNil(t, stateByPath["status"])
	assert.Equal(t, outputkeys.Types["status"], stateByPath["status"].Schema.Type())
	require.NotNil(t, stateByPath["job"])
	assert.Equal(t, asynckeys.Types["job"], stateByPath["job"].Schema.Type())
	require.NotNil(t, stateByPath["viewname"])
	assert.Equal(t, metakeys.Types["view.name"], stateByPath["viewname"].Schema.Type())

	require.NotNil(t, stateByPath["id"])
	assert.Equal(t, "query", stateByPath["id"].KindString())
	assert.Equal(t, "id", stateByPath["id"].InName())
	require.Len(t, result.Components, 1)
	assert.Equal(t, "Route", result.Components[0].FieldName)
	assert.Equal(t, "/v1/api/dev/report", result.Components[0].RoutePath)
	assert.Equal(t, "GET", result.Components[0].Method)
	assert.Equal(t, "dev", result.Components[0].Connector)
}

func TestPlanner_Plan_QuerySelectorHolder(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &selectorHolderSource{}})
	require.NoError(t, err)

	planned, err := New().Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)

	byName := map[string]*State{}
	for _, item := range result.States {
		byName[item.Name] = item
	}
	require.NotNil(t, byName["fields"])
	assert.Equal(t, "rows", byName["fields"].QuerySelector)
	require.NotNil(t, byName["page"])
	assert.Equal(t, "rows", byName["page"].QuerySelector)
}

func TestPlanner_Plan_ViewSummaryURI(t *testing.T) {
	scanned, err := scan.New().Scan(context.Background(), &shape.Source{Struct: &summaryViewSource{}})
	require.NoError(t, err)

	planned, err := New().Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	assert.Equal(t, "testdata/report_summary.sql", result.Views[0].SummaryURL)
}

func TestPlanner_Plan_LinkOnProducesStructuredRelations(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &relationSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	viewPlan := result.Views[0]
	require.Len(t, viewPlan.Relations, 1)
	relation := viewPlan.Relations[0]
	require.Len(t, relation.On, 1)
	assert.Equal(t, "rows", relation.On[0].ParentNamespace)
	assert.Equal(t, "report_id", relation.On[0].ParentColumn)
	assert.Equal(t, "report", relation.On[0].RefNamespace)
	assert.Equal(t, "id", relation.On[0].RefColumn)
}

func TestPlanner_Plan_ComponentHolderTypes(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &typedRouteSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Components, 1)
	assert.Equal(t, reflect.TypeOf(typedRouteInput{}), result.Components[0].InputType)
	assert.Equal(t, reflect.TypeOf(typedRouteOutput{}), result.Components[0].OutputType)
	assert.Empty(t, result.Components[0].InputName)
	assert.Empty(t, result.Components[0].OutputName)
}

func TestPlanner_Plan_DynamicComponentHolderTypes(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET"`
	}{
		Route: xdatly.Component[any, any]{
			Inout:  dynamicRouteInput{},
			Output: dynamicRouteOutput{},
		},
	}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Components, 1)
	assert.Equal(t, reflect.TypeOf(dynamicRouteInput{}), result.Components[0].InputType)
	assert.Equal(t, reflect.TypeOf(dynamicRouteOutput{}), result.Components[0].OutputType)
}

func TestPlanner_Plan_DynamicComponentHolderExplicitNames(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &struct {
		Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
	}{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Components, 1)
	assert.Nil(t, result.Components[0].InputType)
	assert.Nil(t, result.Components[0].OutputType)
	assert.Equal(t, "ReportInput", result.Components[0].InputName)
	assert.Equal(t, "ReportOutput", result.Components[0].OutputName)
}

func TestPlanner_Plan_PreservesConstValueTag(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &constStateSource{}})
	require.NoError(t, err)

	planned, err := New().Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.States, 1)
	require.NotNil(t, result.States[0].In)
	assert.Equal(t, "const", result.States[0].KindString())
	assert.Equal(t, "Product", result.States[0].InName())
	assert.Equal(t, "PRODUCT", result.States[0].Value)
}

func TestPlanner_Plan_PreservesCodecAndHandlerTags(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &codecStateSource{}})
	require.NoError(t, err)

	planned, err := New().Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.States, 2)
	stateByName := map[string]*State{}
	for _, item := range result.States {
		stateByName[item.Name] = item
	}
	require.NotNil(t, stateByName["Jwt"])
	require.NotNil(t, stateByName["Run"])
	if stateByName["Jwt"].Output == nil || stateByName["Jwt"].Output.Name != "JwtClaim" {
		t.Fatalf("expected Jwt codec to be preserved, got %#v", stateByName["Jwt"].Output)
	}
	if stateByName["Run"].Handler == nil || stateByName["Run"].Handler.Name != "Exec" {
		t.Fatalf("expected Run handler to be preserved, got %#v", stateByName["Run"].Handler)
	}
}

func TestPlanner_Plan_DynamicComponentHolderExplicitNamesFromRegistry(t *testing.T) {
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(namedDynamicRouteInput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/plan"), x.WithName("ReportInput")))
	registry.Register(x.NewType(reflect.TypeOf(namedDynamicRouteOutput{}), x.WithPkgPath("github.com/viant/datly/repository/shape/plan"), x.WithName("ReportOutput")))

	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{
		Struct: &struct {
			Route xdatly.Component[any, any] `component:",path=/v1/api/dev/report,method=GET,input=ReportInput,output=ReportOutput"`
		}{},
		TypeRegistry: registry,
	})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Components, 1)
	assert.Equal(t, reflect.TypeOf(namedDynamicRouteInput{}), result.Components[0].InputType)
	assert.Equal(t, reflect.TypeOf(namedDynamicRouteOutput{}), result.Components[0].OutputType)
	assert.Equal(t, "ReportInput", result.Components[0].InputName)
	assert.Equal(t, "ReportOutput", result.Components[0].OutputName)
}

func TestPlanner_Plan_StateTypeNameOverridesInterfaceType(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &taggedComponentStateSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.States, 1)
	assert.Equal(t, "github.com/acme/auth", result.States[0].Schema.Package)
	assert.Equal(t, "github.com/acme/auth", result.States[0].Schema.PackagePath)
	assert.Equal(t, "UserAclOutput", result.States[0].Schema.Name)
}

func TestPlanner_Plan_AssignsNestedRelationParent(t *testing.T) {
	scanned := &shape.ScanResult{
		Source: &shape.Source{Name: "nested"},
		Descriptors: &scan.Result{
			RootType: reflect.TypeOf(struct{}{}),
			ViewFields: []*scan.Field{
				{
					Path: "Route.Output.Data",
					Name: "Data",
					Type: reflect.TypeOf([]struct{}{}),
					ViewTag: &tags.Tag{
						View: &tags.View{Name: "vendor"},
						SQL:  tags.NewViewSQL("", "vendor.sql"),
					},
				},
				{
					Path: "Route.Output.Data.Products",
					Name: "Products",
					Type: reflect.TypeOf([]struct{}{}),
					ViewTag: &tags.Tag{
						View:   &tags.View{Table: "PRODUCT"},
						SQL:    tags.NewViewSQL("", "products.sql"),
						LinkOn: []string{"Id:ID=VendorId:VENDOR_ID"},
					},
				},
			},
		},
	}
	result, err := New().Plan(context.Background(), scanned)
	require.NoError(t, err)
	planned, ok := result.Plan.(*Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 2)
	require.Len(t, planned.Views[1].Relations, 1)
	require.Equal(t, "vendor", planned.Views[1].Relations[0].Parent)
}

func TestPlanner_Plan_LinkOnPreservesFieldSelectors(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &relationSourceWithFields{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	viewPlan := result.Views[0]
	require.Len(t, viewPlan.Relations, 1)
	relation := viewPlan.Relations[0]
	require.Len(t, relation.On, 1)
	assert.Equal(t, "ReportID", relation.On[0].ParentField)
	assert.Equal(t, "rows", relation.On[0].ParentNamespace)
	assert.Equal(t, "report_id", relation.On[0].ParentColumn)
	assert.Equal(t, "ID", relation.On[0].RefField)
	assert.Equal(t, "report", relation.On[0].RefNamespace)
	assert.Equal(t, "id", relation.On[0].RefColumn)
}

func TestPlanner_Plan_ViewTypeDestDeclaration(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &viewTypeDestSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	viewPlan := result.Views[0]
	require.NotNil(t, viewPlan.Declaration)
	assert.Equal(t, "ReportRow", viewPlan.Declaration.TypeName)
	assert.Equal(t, "rows.go", viewPlan.Declaration.Dest)
}

// stubScanSpec is a non-scan-Result implementation of shape.ScanSpec used to
// verify that Plan() returns an error when given an unexpected descriptor type.
type stubScanSpec struct{}

func (s *stubScanSpec) ShapeSpecKind() string { return "stub" }

func TestPlanner_Plan_InvalidDescriptors(t *testing.T) {
	planner := New()
	_, err := planner.Plan(context.Background(), &shape.ScanResult{Source: &shape.Source{Name: "x"}, Descriptors: &stubScanSpec{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported descriptors kind")
}
