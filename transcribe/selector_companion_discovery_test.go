package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/x"
)

func TestSelectorCompanionActualColumnDiscovery(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, label TEXT)"))
	refiner := column.New(column.Connections{"main": h.DB})
	const dql = `#package('selectorcompanion')
#setting($_ = $route('/selector-records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $ID<int>(query/id).Optional().WithPredicate(0,'equal','r','id'))
SELECT r.* FROM (SELECT id,label FROM records) r WHERE id=$ID`
	t.Run("reflected", func(t *testing.T) {
		found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
		require.NoError(t, err)
		result, err := (&PackageCompilation{Source: &Source{Scope: reflect.TypeFor[fixture.Component]().PkgPath(), Name: "ReadRecords", Text: strings.ReplaceAll(strings.ReplaceAll(dql, "input_type('Input')", "input_type('"+reflect.TypeFor[fixture.Component]().PkgPath()+".Input')"), "output_type('Output')", "output_type('"+reflect.TypeFor[fixture.Component]().PkgPath()+".Output')"), Connector: "main", ColumnRefiner: refiner}, Component: found.Components[0], InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()}).Compile(ctx)
		require.NoError(t, err)
		assertSelectorDiscovery(t, result)
		assertSelectorDiscoveryRejections(t, result)
	})
	t.Run("source", func(t *testing.T) {
		root := t.TempDir()
		const module = "example.com/selector-discovery"
		writeSourceFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
		data, err := os.ReadFile(filepath.Join("..", "bootstrap", "testdata", "selectorcompanion", "fixture.go"))
		require.NoError(t, err)
		writeSourceFile(t, root, "reader/fixture.go", string(data))
		writeSourceFile(t, root, "reader/ReadRecords.dql", strings.ReplaceAll(strings.ReplaceAll(dql, "input_type('Input')", "input_type('"+module+"/reader.Input')"), "output_type('Output')", "output_type('"+module+"/reader.Output')"))
		project, err := (&Discovery{BaseDir: root, Include: []string{module + "/reader"}, Connector: "main", ColumnRefiner: refiner}).Compile(ctx)
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		assertSelectorDiscovery(t, project.Components[0])
	})
}

func assertSelectorDiscovery(t *testing.T, result *Result) {
	t.Helper()
	require.NotNil(t, result.Component.SelectorCompanion)
	require.Len(t, result.Component.RootView.Columns, 2)
	require.NotEmpty(t, result.Component.RootView.Columns[1].DatabaseType)
	input, err := (&discoveryInputCompiler{component: result.Component, source: result.Source, resolver: result.TypeResolver, declarations: result.Declarations, viewBindings: result.ViewBindings, views: result.Views}).compile()
	require.NoError(t, err)
	for _, name := range []string{"Fields", "Page"} {
		_, present := input.Value.Type().FieldByName(name)
		require.False(t, present)
		_, present, err := input.ParameterResolver(name)
		require.NoError(t, err)
		require.False(t, present)
		for _, v := range input.Variables {
			require.NotEqual(t, name, v.Name)
		}
	}
	value, present, err := input.ParameterResolver("ID")
	require.NoError(t, err)
	require.True(t, present)
	require.IsType(t, int(0), value)
	count := 0
	for _, p := range spec.EffectiveParameters(result.Component.Parameters) {
		if p.QuerySelector != nil {
			count++
		}
	}
	require.Equal(t, 2, count)
	require.False(t, strings.Contains(input.Value.Type().String(), "Fields"))
	marker, present := input.Value.Type().FieldByName("Has")
	require.True(t, present)
	for _, name := range []string{"Fields", "Page"} {
		_, present := marker.Type.Elem().FieldByName(name)
		require.False(t, present)
	}

}

func assertSelectorDiscoveryRejections(t *testing.T, result *Result) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		mutate func(*spec.Component)
	}{
		{"selector predicate", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].Predicates = []*spec.Predicate{{}} }},
		{"selector codec", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].Codec = &spec.Codec{} }},
		{"selector default", func(c *spec.Component) { v := "1"; c.Parameters[len(c.Parameters)-1].Value = &v }},
		{"selector type", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].TypeExpr = "int64" }},
		{"selector source", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].Source.Name = "other" }},
		{"selector view", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].QuerySelector.View = "other" }},
		{"selector property", func(c *spec.Component) { c.Parameters[len(c.Parameters)-1].QuerySelector.Property = "fields" }},
		{"selector requiredness", func(c *spec.Component) { v := true; c.Parameters[len(c.Parameters)-1].Required = &v }},
		{"ordinary missing field", func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Missing", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "missing"}})
		}},
		{"foreign input", func(c *spec.Component) { c.Settings.InputType = "Output" }},
		{"foreign output", func(c *spec.Component) { c.Settings.OutputType = "Input" }},
		{"execution owner", func(c *spec.Component) { c.Settings.Mutation = "insert" }},
		{"route", func(c *spec.Component) { c.Routes[0].Path = "/other" }},
		{"component kind", func(c *spec.Component) { c.Key.Kind = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := result.Component.Clone()
			// Select a known selector regardless of canonical parameter ordering.
			for i, p := range c.Parameters {
				if p.QuerySelector != nil && p.QuerySelector.Property == "page" {
					c.Parameters[i], c.Parameters[len(c.Parameters)-1] = c.Parameters[len(c.Parameters)-1], c.Parameters[i]
					break
				}
			}
			tc.mutate(c)
			_, err := (&discoveryInputCompiler{component: c, source: result.Source, resolver: result.TypeResolver, declarations: result.Declarations, viewBindings: result.ViewBindings, views: result.Views}).compile()
			require.Error(t, err)
		})
	}
	c := result.Component.Clone()
	output := &spec.Parameter{Name: "Fields", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "string", EmitOutput: true}
	c.Parameters = append(c.Parameters, output)
	selected, err := selectorcompanion.SelectContracts(c, x.NewType(reflect.TypeFor[fixture.Input]()), x.NewType(reflect.TypeFor[fixture.Output]()), result.TypeResolver.Descriptor)
	require.NoError(t, err)
	require.False(t, selected.Contains(output))
	for _, p := range selected.Parameters {
		require.True(t, p.QuerySelector != nil)
	}
}
