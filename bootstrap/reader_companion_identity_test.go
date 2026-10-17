package bootstrap_test

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	f "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/constant/identifier"
	"github.com/viant/datly/internal/readergraph"
	sm "github.com/viant/datly/internal/selectorcompanion/model"
	hc "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	rc "github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/x"
	"reflect"
	"testing"
)

func admittedCompanion(t *testing.T) *spec.Component {
	t.Helper()
	_, _ = reflect.TypeFor[f.Component]().MethodByName("DatlyQuerySelectors")
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[f.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	c, err := found.Components[0].Resolve(reflect.TypeFor[f.Input](), reflect.TypeFor[f.Output]())
	require.NoError(t, err)
	require.NoError(t, bootstrap.CompileOutputColumns(c, reflect.TypeFor[f.Output]()))
	c.Settings.Const = map[string]string{"project": "authored"}
	c.RootView.Source.Table = "$project.records"
	c, err = readergraph.Admit(c, x.NewType(reflect.TypeFor[f.Input]()), x.NewType(reflect.TypeFor[f.Output]()), nil)
	require.NoError(t, err)
	c.ReaderGraph = c.ReaderGraph.WithSelector(c.SelectorCompanion)
	return c
}
func TestCheckedReaderCompanionPreservesBindingIdentity(t *testing.T) {
	c := admittedCompanion(t)
	before, _ := json.Marshal(c)
	for _, overlay := range []map[string]string{nil, {"project": "isolated"}} {
		var values *constant.Values
		if overlay != nil {
			var err error
			values, err = constant.New(overlay)
			require.NoError(t, err)
		}
		_, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[f.Input](), OutputType: reflect.TypeFor[f.Output](), Resources: resource.New(), Const: values})
		require.NoError(t, err)
		runtime := values.Apply(c)
		effective, err := values.For(c)
		require.NoError(t, err)
		runtime, err = (identifier.Renderer{Values: effective}).Component(runtime)
		require.NoError(t, err)
		compiled, err := hc.New(hc.Input{Component: runtime, InputType: reflect.TypeFor[f.Input](), Resources: resource.New()}).Compile()
		require.NoError(t, err)
		require.True(t, compiled.SelectorPlan.Matches(runtime))
		plan, err := rc.Compile(rc.Input{Component: runtime, AuthoredComponent: c, InputType: reflect.TypeFor[f.Input](), OutputType: reflect.TypeFor[f.Output](), Resources: resource.New(), Const: values, Bindings: compiled.Bindings, SelectorCompanion: compiled.SelectorPlan})
		require.NoError(t, err)
		require.True(t, compiled.SelectorPlan.Matches(runtime))
		require.Len(t, plan.SelectorBindings, 2)
		after, _ := json.Marshal(c)
		require.Equal(t, string(before), string(after))
	}
}
func TestCheckedReaderRuntimePredecessorRejectsMutation(t *testing.T) {
	for _, kind := range []string{"value", "schema", "source", "selector", "authority", "unadmitted", "contract", "no-predecessor"} {
		t.Run(kind, func(t *testing.T) {
			c := admittedCompanion(t)
			values, _ := constant.New(map[string]string{"project": "isolated"})
			runtime := values.Apply(c)
			effective, err := values.For(c)
			require.NoError(t, err)
			runtime, err = (identifier.Renderer{Values: effective}).Component(runtime)
			require.NoError(t, err)
			input := rc.Input{Component: runtime, AuthoredComponent: c, InputType: reflect.TypeFor[f.Input](), OutputType: reflect.TypeFor[f.Output](), Resources: resource.New(), Const: values}
			switch kind {
			case "value":
				runtime.Settings.Const["project"] = "other"
			case "schema":
				runtime.RootView.Columns[0].Name = "Other"
			case "source":
				runtime.RootView.Source.SQL = "SELECT 2 AS id"
			case "selector":
				runtime.SelectorCompanion = &sm.Association{}
			case "authority":
				runtime.ReaderGraph = nil
			case "unadmitted":
				runtime.ReaderGraph = nil
				c.ReaderGraph = nil
			case "contract":
				input.InputType = reflect.TypeFor[f.Output]()
			case "no-predecessor":
				input.AuthoredComponent = nil
			}
			_, err = rc.CompileViewMetadata(input)
			require.Error(t, err)
		})
	}
	// Even a valid predecessor cannot hide changed original resource bytes.
	c, resources, files := checkedConstantFixture(t)
	values, _ := constant.New(map[string]string{"project": "isolated"})
	runtime := values.Apply(c)
	files["records.sql"].Data = []byte("SELECT 2 AS ID")
	_, err := rc.CompileViewMetadata(rc.Input{Component: runtime, AuthoredComponent: c, InputType: reflect.TypeFor[checkedConstantInput](), OutputType: reflect.TypeFor[checkedConstantOutput](), Resources: resources, Const: values})
	require.ErrorContains(t, err, "changed after validation")
}
