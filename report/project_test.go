package report

import (
	"go/ast"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestProjectCompilerGeneratedInput(t *testing.T) {
	project, catalog := generatedProject(t, &spec.ReportSettings{Enabled: true})
	derived := project.Derived()
	if len(derived) != 1 {
		t.Fatalf("Derived() count = %d, want 1", len(derived))
	}
	actual := derived[0]
	if actual.Component.Routes[0].Path != "/spend/cube" || actual.Component.Routes[0].Method != "POST" {
		t.Fatalf("derived route = %+v", actual.Component.Routes[0])
	}
	if len(actual.Component.Routes[0].MCP) != 1 || actual.Component.Routes[0].MCP[0].Kind != spec.MCPExposureTool {
		t.Fatalf("derived MCP exposure = %+v", actual.Component.Routes[0].MCP)
	}
	if actual.Component.RootView != nil || actual.Component.Settings.Report != nil {
		t.Fatalf("derived component retained reader/report recursion metadata: %+v", actual.Component)
	}
	input := actual.InputType
	for _, name := range []string{"Dimensions", "Measures", "Filters", "OrderBy", "Limit", "Offset"} {
		if _, ok := input.FieldByName(name); !ok {
			t.Fatalf("generated input missing %s", name)
		}
	}
	filters, _ := input.FieldByName("Filters")
	accountIDs, ok := filters.Type.FieldByName(typecatalog.ExportedFieldName("AccountIDs"))
	if !ok || accountIDs.Type != reflect.TypeOf((*[]int)(nil)) {
		t.Fatalf("filter type = %v, want *[]int", accountIDs.Type)
	}
	if _, ok := filters.Type.FieldByName("Fields"); ok {
		t.Fatal("query-selector parameter leaked into report filters")
	}
	if actual.Type == nil || actual.Type.SynteticType == nil || actual.Type.SynteticType.TypeSpec == nil {
		t.Fatal("generated report input has no synthetic descriptor")
	}
	if !ast.IsExported(actual.Type.Name) || !strings.Contains(actual.Type.SynteticType.Body(), "struct") {
		t.Fatalf("synthetic descriptor = %+v", actual.Type.SynteticType)
	}
	resolved, ok, err := catalog.Resolve(typecatalog.PackageAuthority, actual.Type.Key())
	if err != nil || !ok || resolved.Type != actual.InputType {
		t.Fatalf("catalog generated type = %+v, %v, %v", resolved, ok, err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: actual.Component, InputType: actual.InputType, OutputType: actual.OutputType, Types: catalog,
	})
	if err != nil {
		t.Fatalf("ordinary BuildArtifact() rejected derived component: %v", err)
	}
	if artifact.Input == nil || artifact.Reader != nil {
		t.Fatalf("derived artifact = %+v", artifact)
	}
}

type linkedCubeInput struct {
	Groups struct {
		AccountID bool
	}
	Metrics struct {
		TotalSpend bool
	}
	Predicates struct {
		AccountIDs []int
	}
	Sort   []string
	Take   *int
	Cursor *int
}

func TestProjectCompilerLinkedInputAndMCPOptOut(t *testing.T) {
	disabled := false
	settings := &spec.ReportSettings{
		Enabled: true, MCPTool: &disabled, LinkedInputType: "LinkedCubeInput",
		InputLayout: &spec.ReportInputLayout{
			Dimensions: "Groups", Measures: "Metrics", Filters: "Predicates",
			OrderBy: "Sort", Limit: "Take", Offset: "Cursor",
		},
	}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, &x.Type{
		Type: reflect.TypeOf(linkedCubeInput{}), PkgPath: "example.com/acme/reporting", Name: "LinkedCubeInput",
	}); err != nil {
		t.Fatal(err)
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: catalog}).Compile([]Source{reportSource(t, settings)})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	actual := project.Derived()[0]
	if actual.InputType != reflect.TypeOf(linkedCubeInput{}) {
		t.Fatalf("linked input = %v", actual.InputType)
	}
	if len(actual.Component.Routes[0].MCP) != 0 {
		t.Fatalf("MCP opt-out ignored: %+v", actual.Component.Routes[0].MCP)
	}
}

func TestProjectCompilerDerivedRoutesInheritInternalVisibility(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	source.Component.Routes[0].Internal = true
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	derived := project.Derived()
	if len(derived) != 1 {
		t.Fatalf("Derived() count = %d, want cube", len(derived))
	}
	for _, item := range derived {
		if len(item.Component.Routes) != 1 || !item.Component.Routes[0].Internal {
			t.Fatalf("derived route did not inherit internal visibility: %+v", item.Component.Routes)
		}
	}
}

type dateReportInput struct {
	From *time.Time `format:"dateFormat=YYYY-MM-DD"`
}

func TestProjectCompilerPropagatesFormattedDateWireSchemaToCubeFilters(t *testing.T) {
	groupable := true
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme/reporting", Name: "DateSpend"},
		Name: "DateSpend", Description: "Date Spend",
		Settings:    &spec.Settings{Report: &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true}}, InputType: "dateReportInput", OutputType: "reportSourceOutput"},
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/acme/reporting"},
		Routes:      []*spec.Route{{Method: "GET", Path: "/date-spend", Name: "Date Spend"}},
		Parameters: []*spec.Parameter{
			{Name: "From", Source: spec.BindSource{Kind: "query", Name: "from"}, Predicates: []*spec.Predicate{{Name: "gte", Args: []string{"spend", "event_date"}}}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/acme/reporting", Name: "date_spend"}, Name: "date_spend", Groupable: &groupable,
			Source: &spec.ViewSource{SQL: "SELECT account_id, total_spend FROM spend"},
			Columns: []*spec.Column{
				{Name: "AccountID", Source: "account_id", Groupable: &groupable},
				{Name: "TotalSpend", Source: "total_spend"},
			},
		},
	}
	compiled, err := handlercompiler.New(handlercompiler.Input{
		Component: component, InputType: reflect.TypeOf(dateReportInput{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile source input: %v", err)
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{
		{Component: component, Input: compiled.Input, OutputType: reflect.TypeOf(reportSourceOutput{})},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	derived := project.Derived()
	if len(derived) != 2 {
		t.Fatalf("derived count = %d, want cube and compose", len(derived))
	}
	for _, item := range derived {
		var schemas map[string]*spec.WireSchema
		switch item.Plan.inputType.Field(0).Name {
		case "Dimensions":
			schemas = parameterByName(item.Component.Parameters, "Filters").WireSchemas
			assertWireDateSchema(t, schemas["Filters.From"], true)
		case "Cubes":
			schemas = parameterByName(item.Component.Parameters, "Cubes").WireSchemas
			assertWireDateSchema(t, schemas["Cubes.Filters.From"], true)
		default:
			t.Fatalf("unexpected derived input: %v", item.Plan.inputType)
		}
	}
}

func TestProjectCompilerRejectsDerivedRouteCollision(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	collision := Source{Component: &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme/reporting", Name: "Collision"},
		Routes: []*spec.Route{{Method: "POST", Path: "/spend/cube"}},
	}}
	_, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source, collision})
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("Compile() error = %v, want route collision", err)
	}
}

func parameterByName(params []*spec.Parameter, name string) *spec.Parameter {
	for _, param := range params {
		if param != nil && param.Name == name {
			return param
		}
	}
	return nil
}

func assertWireDateSchema(t *testing.T, schema *spec.WireSchema, nullable bool) {
	t.Helper()
	if schema == nil || schema.Type != "string" || schema.Format != "date" || schema.Nullable != nullable {
		t.Fatalf("wire schema = %+v, want nullable=%v date string", schema, nullable)
	}
}
