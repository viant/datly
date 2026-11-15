package generate

import (
	"errors"
	"github.com/viant/datly/internal/testharness"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/internal/sourcecontract"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func genericCubeSourceInput(t *testing.T, includePage bool) Input {
	t.Helper()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	const pkg = "example.com/generated/records"
	const modelPkg = "example.com/generated/models"
	source := "package records\nimport (\"github.com/viant/xdatly\"; models \"" + modelPkg + "\")\n" +
		"type RecordsInput struct{Mode string `parameter:\"Mode,kind=query,in=mode,dataType=string,required=false\"`;Cursor *models.Page[models.Row] `parameter:\"Cursor,kind=body,in=cursor,required=false\"`;Has *RecordsInputHas `setMarker:\"true\" typeName:\"RecordsInputHas\" json:\"-\" sqlx:\"-\"`}\n" +
		"type RecordsInputHas struct{Mode bool;Cursor bool}\n" +
		"type RecordsOutput struct{Rows []*Record `parameter:\"Rows,kind=output,in=view\" view:\"rows,type=Record\"`}\n" +
		"type Record struct{Category string `sqlx:\"category\"`;Amount int `sqlx:\"amount\"`}\n" +
		"type recordsSelectors struct{Limit int `parameter:\"Limit,kind=query,in=limit,dataType=int,required=false,cacheable=false\" querySelector:\"view=rows,property=limit\"`}\n" +
		"type RecordsComponent struct{Contract xdatly.Component[RecordsInput,RecordsOutput] `component:\"Records,path=/records,method=GET\"`}\n"
	declarations, err := sourceDescriptors(pkg, source)
	require.NoError(t, err)
	byName := map[string]*x.Type{}
	for _, d := range declarations {
		byName[d.Name] = d
	}
	method, err := parser.ParseFile(token.NewFileSet(), "method.go", "package records;func(RecordsComponent)DatlyQuerySelectors()recordsSelectors{}", 0)
	require.NoError(t, err)
	byName["RecordsComponent"].SynteticType.MethodsAST = []*ast.FuncDecl{method.Decls[0].(*ast.FuncDecl)}
	modelSource := "package models;type Row struct{ID int}"
	if includePage {
		modelSource += ";type Page[T any]struct{Value T}"
	} else {
		modelSource += ";type Page[T any]struct{Value T;Missing *Absent[T]};type Absent[A,B any]struct{Value A}"
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "models"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "models", "models.go"), []byte(modelSource), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "records"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "records", "contracts.go"), []byte("package records\ntype RecordsOutput struct{Rows []*Record `parameter:\"Rows,kind=output,in=view\" view:\"rows,type=Record\"`}\ntype Record struct{Category string `sqlx:\"category\"`;Amount int `sqlx:\"amount\"`}\n"), 0600))
	models, err := sourceDescriptors(modelPkg, modelSource)
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, append(declarations, models...)...))
	types, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: pkg, Imports: []typecatalog.PackageImport{{Alias: "models", Package: modelPkg}}})
	require.NoError(t, err)
	association, err := selectorModel.New(byName["RecordsComponent"], "Contract", byName["recordsSelectors"])
	require.NoError(t, err)
	optional, noCache, on := false, false, true
	original := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Records"}, Name: "Records", TypeContext: &spec.TypeContext{PackagePath: pkg, Imports: []spec.ImportSpec{{Alias: "models", Package: modelPkg}}}, Settings: &spec.Settings{InputType: pkg + ".RecordsInput", OutputType: pkg + ".RecordsOutput", Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "rows", TypeName: "Record", Groupable: &on, Source: &spec.ViewSource{SQL: "SELECT category,SUM(amount) AS amount FROM records GROUP BY category"}, Columns: []*spec.Column{{Name: "Category", Source: "category", Type: spec.TypeRef{Name: "string"}, Groupable: &on}, {Name: "Amount", Source: "amount", Type: spec.TypeRef{Name: "int"}}}}, Parameters: []*spec.Parameter{
		{Name: "Mode", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "mode"}, Required: &optional},
		{Name: "Cursor", TypeExpr: "*models.Page[models.Row]", Source: spec.BindSource{Kind: "body", Name: "cursor"}, Required: &optional},
		{Name: "Limit", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "limit"}, Required: &optional, Cacheable: &noCache, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyLimit}},
		{Name: "Rows", TypeExpr: "[]*Record", Source: spec.BindSource{Kind: "output", Name: "view"}, EmitOutput: true},
	}, SelectorCompanion: association}
	current := original.Clone()
	current.Settings.InputType = "RecordsInput"
	current.Parameters[0].Tag = `choice:"day" choice:"hour" enumAccept:"scalar" enumStyle:"literal"`
	proof, err := sourcecontract.NewSelector(original, current, byName["RecordsInput"], byName["RecordsOutput"], catalog, types)
	require.NoError(t, err)
	return Input{ProjectRoot: root, Component: current, TypeResolver: types, TargetPackage: pkg, PreparedSelector: proof, Contracts: ContractReferences{Output: &ContractReference{Expression: pkg + ".RecordsOutput", DescriptorKey: pkg + ".RecordsOutput"}}, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: pkg + ".Record"}}}
}

func TestCubeSourceGenericBusinessDependencyClosure(t *testing.T) {
	input := genericCubeSourceInput(t, true)
	generated, err := New(input).Generate(filepath.Join(input.ProjectRoot, "records"))
	require.NoError(t, err)
	require.Len(t, generated.Plan.Cubes, 1)
	filters, ok := generated.Plan.Cubes[0].InputType.FieldByName("Filters")
	require.True(t, ok)
	cursor, ok := filters.Type.FieldByName("Cursor")
	require.True(t, ok)
	require.Equal(t, reflect.Pointer, cursor.Type.Kind())
	value, ok := cursor.Type.Elem().FieldByName("Value")
	require.True(t, ok)
	id, ok := value.Type.FieldByName("ID")
	require.True(t, ok)
	require.Equal(t, reflect.Int, id.Type.Kind())
	command := exec.Command("go", "test", "./records", "-count=1")
	command.Dir = input.ProjectRoot
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestCubeSourceUnresolvableGenericDependencyPreservesCause(t *testing.T) {
	input := genericCubeSourceInput(t, false)
	_, err := New(input).Generate(filepath.Join(input.ProjectRoot, "records"))
	require.Error(t, err)
	require.ErrorContains(t, err, "example.com/generated/models.Absent")
	require.ErrorContains(t, err, "prepared selector dependency")
	cause := errors.Unwrap(err)
	require.NotNil(t, cause, "cube boundary must retain the dependency cause")
	require.NotNil(t, errors.Unwrap(cause), "dependency annotation must retain the native resolver cause")
	_, nativeErr := input.TypeResolver.Descriptor("example.com/generated/models.Absent[example.com/generated/models.Row]")
	require.Error(t, nativeErr)
	require.ErrorContains(t, err, nativeErr.Error())
}
