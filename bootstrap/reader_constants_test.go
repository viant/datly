package bootstrap_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/readergraph"
	sm "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	rc "github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/x"
	xcodec "github.com/viant/xdatly/codec"
	"reflect"
	"testing"
	"testing/fstest"
)

type checkedConstantInput struct {
	Project string `parameter:",kind=const,in=project,value=authored"`
}
type checkedConstantRow struct {
	ID int `sqlx:"id"`
}
type checkedConstantOutput struct {
	Rows []*checkedConstantRow `parameter:",kind=output,in=view" view:"records" sql:"uri=records.sql"`
}

func checkedConstantFixture(t *testing.T) (*spec.Component, *resource.Store, fstest.MapFS) {
	t.Helper()
	value := "authored"
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[checkedConstantInput]().PkgPath(), Name: "records"}, Name: "records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Settings: &spec.Settings{}, Parameters: []*spec.Parameter{{Name: "Project", TypeExpr: "string", Source: spec.BindSource{Kind: "const", Name: "project"}, Value: &value}, {Name: "Rows", TypeExpr: "[]*checkedConstantRow", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "records", TypeName: "checkedConstantRow", Source: &spec.ViewSource{URI: "records.sql"}, Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}}}}}
	body := []byte("SELECT 1 AS ID")
	in, out := reflect.TypeFor[checkedConstantInput](), reflect.TypeFor[checkedConstantOutput]()
	admitted, err := readergraph.Admit(c, x.NewType(in), x.NewType(out), map[string][]byte{"records.sql": body})
	require.NoError(t, err)
	resources := resource.New()
	files := fstest.MapFS{"records.sql": {Data: body}}
	require.NoError(t, resources.Register("", files))
	return admitted, resources, files
}

func TestCheckedReaderAcceptsTrustedInstanceConstant(t *testing.T) {
	admitted, resources, _ := checkedConstantFixture(t)
	before, _ := json.Marshal(admitted)
	authority := admitted.ReaderGraph
	for _, value := range []string{"first", "second"} {
		values, err := constant.New(map[string]string{"project": value})
		require.NoError(t, err)
		a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: admitted, InputType: reflect.TypeFor[checkedConstantInput](), OutputType: reflect.TypeFor[checkedConstantOutput](), Const: values, Resources: resources})
		require.NoError(t, err)
		route, ok := a.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/records"})
		require.True(t, ok)
		require.Equal(t, value, route.Fields()[0].Binding().DefaultValue)
		require.Equal(t, value, *a.Component.Parameters[0].Value)
		require.NotNil(t, a.Reader.Root)
		// A runtime clone is not a new admitted source and cannot be fed back.
		require.Error(t, checkedCompile(a.Component, resources, values))
	}
	after, _ := json.Marshal(admitted)
	require.Equal(t, string(before), string(after))
	require.Same(t, authority, admitted.ReaderGraph)
}

func checkedCompile(c *spec.Component, resources *resource.Store, values *constant.Values) error {
	_, err := rc.CompileViewMetadata(rc.Input{Component: c, InputType: reflect.TypeFor[checkedConstantInput](), OutputType: reflect.TypeFor[checkedConstantOutput](), Const: values, Resources: resources})
	return err
}
func TestCheckedReaderPreservesAdmissionGuards(t *testing.T) {
	for _, kind := range []string{"schema", "source", "contract", "selector", "resource", "independent"} {
		t.Run(kind, func(t *testing.T) {
			c, resources, files := checkedConstantFixture(t)
			require.NoError(t, checkedCompile(c, resources, nil))
			switch kind {
			case "schema":
				c.RootView.Columns[0].Name = "Other"
			case "source":
				c.RootView.Source.URI = "foreign.sql"
			case "contract":
				_, err := rc.CompileViewMetadata(rc.Input{Component: c, InputType: reflect.TypeFor[checkedConstantOutput](), OutputType: reflect.TypeFor[checkedConstantInput](), Resources: resources})
				require.Error(t, err)
				return
			case "selector":
				c.SelectorCompanion = &sm.Association{}
			case "resource":
				files["records.sql"] = &fstest.MapFile{Data: []byte("SELECT 2 AS ID")}
			case "independent":
				c.Views = []*spec.View{{Name: "extra"}}
				_, err := rc.CompileViewDependencies(rc.Input{Component: c})
				require.ErrorContains(t, err, "independent input views")
				return
			}
			require.Error(t, checkedCompile(c, resources, nil))
		})
	}
}
func TestCheckedReaderRuntimeIdentifierMetadata(t *testing.T) {
	c, resources, files := checkedConstantFixture(t)
	// Admit authored identifier sites, using the same metadata renderer as bootstrap.
	c.RootView.Source.Table = "$project.records"
	c.RootView.Columns[0].Tag = `sqlx:"ID,table=$project.records"`
	var err error
	c, err = readergraph.Admit(c, x.NewType(reflect.TypeFor[checkedConstantInput]()), x.NewType(reflect.TypeFor[checkedConstantOutput]()), map[string][]byte{"records.sql": files["records.sql"].Data})
	require.NoError(t, err)
	before, _ := json.Marshal(c)
	for _, value := range []string{"first", "second"} {
		values, _ := constant.New(map[string]string{"project": value})
		calls := 0
		v, err := rc.CompileViewMetadata(rc.Input{Component: c, InputType: reflect.TypeFor[checkedConstantInput](), OutputType: reflect.TypeFor[checkedConstantOutput](), Const: values, Resources: resources, IdentifierProducts: func(string) (string, error) { calls++; return "MySQL", nil }})
		require.NoError(t, err)
		require.Contains(t, v.Spec.Source.Table, value)
		require.Contains(t, v.Spec.Columns[0].Tag, value)
		require.Greater(t, calls, 0)
	}
	values, _ := constant.New(map[string]string{"project": "bad;DROP TABLE records"})
	require.Error(t, checkedCompile(c, resources, values))
	after, _ := json.Marshal(c)
	require.Equal(t, string(before), string(after))
}

type checkedCodecFactory struct{ calls int }

func (f *checkedCodecFactory) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	f.calls++
	return checkedCodec{}, nil
}

type checkedCodec struct{}

func (checkedCodec) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	return raw, nil
}
func TestCheckedReaderKeepsCompiledCodecAndPredicate(t *testing.T) {
	c, resources, files := checkedConstantFixture(t)
	c.Parameters[0].Codec = &spec.Codec{Body: "checkedIdentity"}
	c.Parameters[0].Predicates = []*spec.Predicate{{Name: "equal", Args: []string{"r", "ID"}}}
	var err error
	c, err = readergraph.Admit(c, x.NewType(reflect.TypeFor[checkedConstantInput]()), x.NewType(reflect.TypeFor[checkedConstantOutput]()), map[string][]byte{"records.sql": files["records.sql"].Data})
	require.NoError(t, err)
	before, _ := json.Marshal(c)
	values, _ := constant.New(map[string]string{"project": "isolated"})
	factory := &checkedCodecFactory{}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[checkedConstantInput](), OutputType: reflect.TypeFor[checkedConstantOutput](), Const: values, Resources: resources, CodecFactory: factory})
	require.NoError(t, err)
	require.Greater(t, factory.calls, 0)
	require.NotNil(t, a.Reader)
	after, _ := json.Marshal(c)
	require.Equal(t, string(before), string(after))
}

func TestCheckedReaderDirectConstantValidation(t *testing.T) {
	for _, kind := range []string{"type", "conflicting-default", "non-const"} {
		t.Run(kind, func(t *testing.T) {
			c, resources, files := checkedConstantFixture(t)
			switch kind {
			case "type":
				c.Parameters[0].TypeExpr = "int"
			case "conflicting-default":
				c.Settings.Const = map[string]string{"project": "different"}
			case "non-const":
				c.Parameters[0].Source.Kind = "query"
			}
			var err error
			c, err = readergraph.Admit(c, x.NewType(reflect.TypeFor[checkedConstantInput]()), x.NewType(reflect.TypeFor[checkedConstantOutput]()), map[string][]byte{"records.sql": files["records.sql"].Data})
			require.NoError(t, err)
			values, _ := constant.New(map[string]string{"project": "not-an-int"})
			require.Error(t, checkedCompile(c, resources, values))
		})
	}
}
