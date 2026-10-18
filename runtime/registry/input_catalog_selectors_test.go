package registry_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcatalog"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestInputCatalogRootAndDependencySelectorOwnership(t *testing.T) {
	_, _ = reflect.TypeFor[fixture.Root]().MethodByName("DatlyQuerySelectors")
	_, _ = reflect.TypeFor[fixture.Child]().MethodByName("DatlyQuerySelectors")
	_, _ = reflect.TypeFor[fixture.Conflict]().MethodByName("DatlyQuerySelectors")
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Root]().PkgPath()}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 3)
	var entries []*registry.RegisteredComponent
	var parent, child, conflict *registry.RegisteredComponent
	for _, source := range found.Components {
		input, output := reflect.TypeFor[fixture.RootInput](), reflect.TypeFor[fixture.RootOutput]()
		if source.Tag.Name == "Child" {
			input, output = reflect.TypeFor[fixture.ChildInput](), reflect.TypeFor[fixture.ChildOutput]()
		}
		component, e := source.Resolve(input, output)
		require.NoError(t, e)
		compiled, e := compiler.New(compiler.Input{Component: component, InputType: input}).Compile()
		require.NoError(t, e)
		entry := &registry.RegisteredComponent{Component: component, Input: compiled.Input}
		entries = append(entries, entry)
		if source.Tag.Name == "Child" {
			child = entry
		} else if source.Tag.Name == "Conflict" {
			conflict = entry
		} else {
			parent = entry
		}
	}
	catalog, err := registry.NewInputCatalog(entries)
	require.NoError(t, err)
	rootRef := spec.RouteRef{Method: "GET", Path: "/catalog-root"}
	childRef := spec.RouteRef{Method: "GET", Path: "/catalog-child"}
	rootFields, err := catalog.FieldsFor(parent.Component.Key, rootRef)
	require.NoError(t, err)
	rootContract, ok := parent.Input.ForRoute(rootRef)
	require.True(t, ok)
	_, err = tool.NewCompiler().Compile(tool.Input{Fields: rootFields, Component: parent.Component.Key, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "catalog.root"}, Contract: rootContract})
	require.NoError(t, err, "optional child controls must not collide with root public selector names")
	require.Len(t, rootFields, 4)
	rootSources := map[string]registry.InputField{}
	for _, field := range rootFields {
		rootSources[field.Binding().Location.In] = field
	}
	for _, name := range []string{"root_limit", "root_page", "child_offset", "token"} {
		require.Contains(t, rootSources, name)
	}
	for _, name := range []string{"child_limit", "child_fields"} {
		require.NotContains(t, rootSources, name)
	}
	require.Equal(t, childRef, rootSources["child_offset"].Origin())
	require.True(t, *rootSources["child_offset"].Binding().Required)
	require.True(t, *rootSources["token"].Binding().Required)

	conflictRef := spec.RouteRef{Method: "GET", Path: "/catalog-conflict"}
	conflictFields, e := catalog.FieldsFor(conflict.Component.Key, conflictRef)
	require.NoError(t, e)
	conflictContract, ok := conflict.Input.ForRoute(conflictRef)
	require.True(t, ok)
	_, e = tool.NewCompiler().Compile(tool.Input{Fields: conflictFields, Component: conflict.Component.Key, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "catalog.conflict"}, Contract: conflictContract})
	require.ErrorContains(t, e, `duplicate public argument "Offset"`, "required child input must not be hidden to suppress an existing conflict")
	childFields, err := catalog.FieldsFor(child.Component.Key, childRef)
	require.NoError(t, err)
	require.Len(t, childFields, 4, "selected child retains its complete standalone side schema")
	sources := map[string]bool{}
	for _, field := range childFields {
		sources[field.Binding().Location.In] = true
	}
	for _, name := range []string{"child_limit", "child_fields", "child_offset", "token"} {
		require.True(t, sources[name], name)
	}
}
