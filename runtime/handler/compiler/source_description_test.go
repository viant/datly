package compiler_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/report"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
	"github.com/viant/xdatly"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

func TestSourceDescriptionDoesNotAuthorizeRuntime(t *testing.T) {
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	inputType := reflect.TypeFor[fixture.Input]()
	original := compiler.Input{Component: component, InputType: inputType}
	baseline, err := compiler.New(original).Describe()
	require.NoError(t, err)
	originalRoute, ok := baseline.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
	require.True(t, ok)
	require.Len(t, originalRoute.Fields(), 1)
	tag := "parameter:\"ID,kind=query,in=id,required=false\" choice:\"1\" enumAccept:\"scalar\" enumStyle:\"literal\""
	fields := []reflect.StructField{inputType.Field(0), inputType.Field(1)}
	fields[0].Tag = reflect.StructTag(tag)
	prospective := reflect.StructOf(fields)
	expression, err := parser.ParseExpr("struct {ID int `" + tag + "`;Has *InputHas `setMarker:\"true\" json:\"-\"`}")
	require.NoError(t, err)
	input := &x.Type{Name: "Input", PkgPath: pkg, SynteticType: &smodel.Type{Name: "Input", PkgPath: pkg, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Input"), Type: expression}}}
	output := x.NewType(reflect.TypeFor[fixture.Output]())
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[fixture.InputHas]()), output))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginGenerated, input))
	types, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	require.NoError(t, func() error {
		_, e := selectorcompanion.SelectContracts(component, input, output, types.Descriptor)
		return e
	}())
	_, err = selectorcompanion.Select(component, prospective)
	require.Error(t, err)
	_, err = selectorcompanion.CheckContracts(component, x.NewType(prospective), output, types.Descriptor)
	require.Error(t, err)
	for _, firstRuntime := range []bool{false, true} {
		t.Run(map[bool]string{false: "authoring-first", true: "runtime-first"}[firstRuntime], func(t *testing.T) {
			c := compiler.New(compiler.Input{Component: component, InputType: prospective})
			runtimeChecks := func() {
				_, err := c.Compile()
				require.Error(t, err)
				_, err = c.CompileBindings()
				require.Error(t, err)
				_, err = c.Describe()
				require.Error(t, err)
			}
			describe := func() {
				result, err := c.DescribeContracts(input, output, types)
				require.NoError(t, err)
				require.Nil(t, result.SelectorPlan)
				route, ok := result.Input.ForRoute(originalRoute.Route())
				require.True(t, ok)
				require.Nil(t, route.SelectorPlan())
				require.Len(t, route.Fields(), 1)
				require.Equal(t, originalRoute.Fields()[0].Path(), route.Fields()[0].Path())
				require.Equal(t, originalRoute.Fields()[0].Binding().Location, route.Fields()[0].Binding().Location)
				field, err := route.Fields()[0].StructField()
				require.NoError(t, err)
				require.Equal(t, reflect.StructTag(tag), field.Tag)
				require.NotNil(t, component.SelectorCompanion)
			}
			if firstRuntime {
				runtimeChecks()
			}
			describe()
			runtimeChecks()
			describe()
		})
	}
	t.Run("mismatched shape", func(t *testing.T) {
		_, err := compiler.New(original).DescribeContracts(input, output, types)
		require.ErrorContains(t, err, "metadata mismatch")
	})
	for _, tc := range []struct {
		name   string
		mutate func(*spec.Component)
	}{
		{"foreign package", func(c *spec.Component) { c.Key.Scope = "example.com/foreign" }},
		{"route", func(c *spec.Component) { c.Routes[0].Path = "/forged" }},
		{"method", func(c *spec.Component) { c.Routes[0].Method = "POST" }},
		{"owner", func(c *spec.Component) { c.Settings.Mutation = "patch" }},
		{"duplicate public", func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := component.Clone()
			tc.mutate(changed)
			_, err := compiler.New(compiler.Input{Component: changed, InputType: prospective}).DescribeContracts(input, output, types)
			require.Error(t, err)
		})
	}
	for _, signature := range []string{"func (Component) DatlyQuerySelectors(arg int) selectors {}", "func (Component) DatlyQuerySelectors() *selectors {}", "func (Component) DatlyQuerySelectors() Output {}"} {
		t.Run(signature, func(t *testing.T) {
			expression, err := parser.ParseExpr("struct {Contract xdatly.Component[Input, Output] `component:\"ReadRecords,path=/selector-records,method=GET\"`}")
			require.NoError(t, err)
			file, err := parser.ParseFile(token.NewFileSet(), "holder.go", "package selectorcompanion;"+signature, 0)
			require.NoError(t, err)
			holder := &x.Type{Name: "Component", PkgPath: pkg, SynteticType: &smodel.Type{Name: "Component", PkgPath: pkg, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Component"), Type: expression}, Imports: map[string]*smodel.ImportRef{"xdatly": {Path: "github.com/viant/xdatly", Alias: "xdatly"}}, MethodsAST: []*ast.FuncDecl{file.Decls[0].(*ast.FuncDecl)}}}
			companion, err := component.SelectorCompanion.Companion()
			require.NoError(t, err)
			association, err := selectorModel.New(holder, "Contract", companion)
			require.NoError(t, err)
			changed := component.Clone()
			changed.SelectorCompanion = association
			_, err = compiler.New(compiler.Input{Component: changed, InputType: prospective}).DescribeContracts(input, output, types)
			require.Error(t, err)
		})
	}
	wrong := x.Type{Name: "ForeignInput", PkgPath: pkg, SynteticType: input.SynteticType}
	_, err = compiler.New(compiler.Input{Component: component, InputType: prospective}).DescribeContracts(&wrong, output, types)
	require.Error(t, err)
}

// Side controls remain source-owned; report metadata consumes public Fields.
type SourceBusiness struct {
	Mode       string   `parameter:"Mode,kind=query,in=mode,required=false"`
	Criteria   string   `parameter:"Criteria,kind=query,in=criteria,required=false" querySelector:"view=rows,property=criteria"`
	ChildOrder []string `parameter:"ChildOrder,kind=query,in=child_order,required=false" querySelector:"view=children,property=orderBy"`
}
type SourceOutput struct{}
type sourceControls struct {
	Fields     []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	OrderBy    []string `parameter:"OrderBy,kind=query,in=order_by,required=false" querySelector:"view=rows,property=orderBy"`
	Page       int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
	Limit      int      `parameter:"Limit,kind=query,in=limit,required=false" querySelector:"view=rows,property=limit"`
	Offset     int      `parameter:"Offset,kind=query,in=offset,required=false" querySelector:"view=rows,property=offset"`
	ChildLimit int      `parameter:"ChildLimit,kind=query,in=child_limit,required=false" querySelector:"view=children,property=limit"`
}
type SourceHolder struct {
	Contract xdatly.Component[SourceBusiness, SourceOutput] `component:"SourceRecords,path=/source-records,method=GET"`
}

func (SourceHolder) DatlyQuerySelectors() sourceControls { panic("declaration only") }

func TestSourceDescriptionPreservesReportPublicBusinessFields(t *testing.T) {
	input := x.NewType(reflect.TypeFor[SourceBusiness]())
	output := x.NewType(reflect.TypeFor[SourceOutput]())
	association, err := selectorModel.New(x.NewType(reflect.TypeFor[SourceHolder]()), "Contract", x.NewType(reflect.TypeFor[sourceControls]()))
	require.NoError(t, err)
	yes := true
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: input.PkgPath, Name: "SourceRecords"}, Name: "SourceRecords", Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/source-records"}}, RootView: &spec.View{Name: "rows", Groupable: &yes, Source: &spec.ViewSource{SQL: "SELECT category, COUNT(*) AS amount FROM records GROUP BY category"}, Columns: []*spec.Column{{Name: "Category", Source: "category", Groupable: &yes}, {Name: "Amount", Source: "amount"}}}, SelectorCompanion: association}
	component, err = (bootstrap.ContractResolver{Component: component, InputType: input, OutputType: output}).Resolve()
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, input, output))
	types, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	native, err := compiler.New(compiler.Input{Component: component, InputType: input.Type}).Describe()
	require.NoError(t, err)
	source, err := compiler.New(compiler.Input{Component: component, InputType: input.Type}).DescribeContracts(input, output, types)
	require.NoError(t, err)
	require.Nil(t, source.SelectorPlan)
	ref := spec.RouteRef{Method: "GET", Path: "/source-records"}
	nativeRoute, ok := native.Input.ForRoute(ref)
	require.True(t, ok)
	sourceRoute, ok := source.Input.ForRoute(ref)
	require.True(t, ok)
	require.NotNil(t, nativeRoute.SelectorPlan())
	require.Nil(t, sourceRoute.SelectorPlan())
	require.Len(t, sourceRoute.Fields(), 3)
	for i, field := range nativeRoute.Fields() {
		other := sourceRoute.Fields()[i]
		require.Equal(t, field.Path(), other.Path())
		require.Equal(t, field.Binding().Location, other.Binding().Location)
		left, e := field.StructField()
		require.NoError(t, e)
		right, e := other.StructField()
		require.NoError(t, e)
		require.Equal(t, left, right)
	}
	selected, err := selectorcompanion.SelectContracts(component, input, output, types.Descriptor)
	require.NoError(t, err)
	require.Len(t, selected.Parameters, 6)
	nativeReport, err := report.NewProjectCompiler(report.ProjectConfig{Types: catalog}).Compile([]report.Source{{Component: component, Input: native.Input, OutputType: output.Type}})
	require.NoError(t, err)
	sourceReport, err := report.NewProjectCompiler(report.ProjectConfig{Types: catalog}).Compile([]report.Source{{Component: component, Input: source.Input, OutputType: output.Type}})
	require.NoError(t, err)
	require.Equal(t, nativeReport.Derived()[0].Definition, sourceReport.Derived()[0].Definition)
	require.Equal(t, nativeReport.Derived()[0].InputType, sourceReport.Derived()[0].InputType)
	filter, _ := sourceReport.Derived()[0].InputType.FieldByName("Filters")
	for _, name := range []string{"Mode", "Criteria", "ChildOrder"} {
		_, ok := filter.Type.FieldByName(name)
		require.True(t, ok, name)
	}
	for _, name := range []string{"Fields", "OrderBy", "Page", "Limit", "Offset", "ChildLimit"} {
		_, ok := filter.Type.FieldByName(name)
		require.False(t, ok, name)
	}
	require.Same(t, association, component.SelectorCompanion)
}
