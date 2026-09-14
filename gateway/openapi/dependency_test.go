package openapi_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type dependencyResult struct {
	Message string `json:"message"`
}

func TestTransitivePrivateDependencyInputs(t *testing.T) {
	type leafInput struct {
		Name string `parameter:"Name,kind=query,in=name,required=true"`
	}
	type middleInput struct {
		Leaf *dependencyResult `parameter:"Leaf,kind=component,in=GET:/leaf"`
	}
	type parentInput struct {
		Name   string            `parameter:"Name,kind=query,in=name"`
		Middle *dependencyResult `parameter:"Middle,kind=component,in=GET:/middle"`
		Leaf   *dependencyResult `parameter:"Leaf,kind=component,in=GET:/leaf"`
	}
	component := func(name, path, scope string) *spec.Component {
		return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: name, Scope: scope}, Routes: []*spec.Route{{Method: "GET", Path: path}}}
	}
	leaf := (fixture{component: component("Leaf", "/leaf", "private"), input: reflect.TypeFor[leafInput](), output: reflect.TypeFor[dependencyResult](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		return &dependencyResult{Message: inv.Input.(*leafInput).Name}, nil
	}}).registration(t)
	middle := (fixture{component: component("Middle", "/middle", "private"), input: reflect.TypeFor[middleInput](), output: reflect.TypeFor[dependencyResult](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		return inv.Input.(*middleInput).Leaf, nil
	}}).registration(t)
	parent := (fixture{component: component("Parent", "/parent", "public"), input: reflect.TypeFor[parentInput](), output: reflect.TypeFor[dependencyResult](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		return inv.Input.(*parentInput).Middle, nil
	}}).registration(t)
	// Internal invocation does not run the child's HTTP API-key gate.
	leaf.Component.Routes[0].APIKeyHeader = "X-Private-Key"
	leaf.Component.Routes[0].APIKeyValue = "secret"
	entries := []*registry.RegisteredComponent{parent, middle, leaf}
	rt, err := druntime.NewRuntime(entries, druntime.WithExposedPackages([]string{"public"}, nil))
	require.NoError(t, err)
	request := documentRequest(entries...)
	request.Visibility = rt
	doc, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, doc.Paths, 1)
	require.NotContains(t, doc.Paths, "/leaf")
	require.NotContains(t, doc.Paths, "/middle")
	operation := doc.Paths["/parent"].Get
	require.Len(t, operation.Parameters, 1, "diamond dependency and parent consumption share one wire input")
	require.Equal(t, "name", operation.Parameters[0].Name)
	require.True(t, operation.Parameters[0].Required)
	require.Nil(t, operation.Security, "private HTTP gates are not inherited by component invocation")
	for _, tc := range []struct {
		url string
		ok  bool
	}{{"/parent", false}, {"/parent?name=Ada", true}} {
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", tc.url, nil))
		if !tc.ok {
			require.GreaterOrEqual(t, recorder.Code, 400)
			continue
		}
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
		require.JSONEq(t, `{"message":"Ada"}`, recorder.Body.String())
	}
	// The catalog's results must not mutate the underlying route contracts.
	catalog, err := registry.NewInputCatalog(entries)
	require.NoError(t, err)
	fields, err := catalog.Fields(spec.RouteRef{Method: "GET", Path: "/parent"})
	require.NoError(t, err)
	value := fields[0].Binding()
	*value.Required = false
	again, err := catalog.Fields(spec.RouteRef{Method: "GET", Path: "/parent"})
	require.NoError(t, err)
	require.True(t, *again[0].Binding().Required)
	direct, _ := parent.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/parent"})
	require.Nil(t, direct.Fields()[0].Binding().Required, "transitive merge must not rewrite a parent's optional binding")
}

func TestDependencyRouteEffectiveActivation(t *testing.T) {
	type childInput struct {
		ID   *int   `parameter:"ID,kind=path,in=id,uri=/{id},required=true"`
		Name string `parameter:"Name,kind=query,in=name,required=true"`
	}
	type parentInput struct {
		ID    *int              `parameter:"ID,kind=path,in=id,uri=/{id},required=true"`
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/child/{id},uri=/parent/{id}"`
	}
	child := (fixture{input: reflect.TypeFor[childInput](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Child"}, Routes: []*spec.Route{{Method: "GET", Path: "/child"}}}, execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*childInput)
		require.Equal(t, 7, *input.ID)
		return &dependencyResult{Message: input.Name}, nil
	}}).registration(t)
	parent := (fixture{input: reflect.TypeFor[parentInput](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Routes: []*spec.Route{{Method: "GET", Path: "/parent"}}}, execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*parentInput)
		if input.Child == nil {
			return &dependencyResult{Message: "base"}, nil
		}
		return input.Child, nil
	}}).registration(t)
	entries := []*registry.RegisteredComponent{parent, child}
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entries...))
	require.NoError(t, err)
	require.Empty(t, doc.Paths["/parent"].Get.Parameters)
	require.Len(t, doc.Paths["/parent/{id}"].Get.Parameters, 2)
	rt, err := druntime.NewRuntime(entries)
	require.NoError(t, err)
	for _, tc := range []struct {
		url string
		ok  bool
	}{{"/parent", true}, {"/parent/7", false}, {"/parent/7?name=Ada", true}} {
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", tc.url, nil))
		if tc.ok {
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
		} else {
			require.GreaterOrEqual(t, recorder.Code, 400)
		}
	}
	// Source metadata cannot redirect a previously compiled dependency.
	for _, param := range parent.Component.Parameters {
		if param.Source.Kind == "component" {
			param.Source.Name = "GET:/missing"
		}
	}
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entries...))
	require.NoError(t, err)
}

func TestDependencyFailures(t *testing.T) {
	type parentInput struct {
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/child"`
	}
	parent := (fixture{input: reflect.TypeFor[parentInput](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Routes: []*spec.Route{{Method: "GET", Path: "/parent"}}}}).registration(t)
	_, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(parent))
	require.ErrorContains(t, err, "dependency route contract not found")
	type cyclicInput struct {
		Parent *dependencyResult `parameter:"Parent,kind=component,in=GET:/parent"`
	}
	cyclic := (fixture{input: reflect.TypeFor[cyclicInput](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Child"}, Routes: []*spec.Route{{Method: "GET", Path: "/child"}}}}).registration(t)
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(parent, cyclic))
	require.ErrorContains(t, err, "dependency cycle")
	type conflictInput struct {
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/child"`
		Name  int               `parameter:"Name,kind=query,in=name"`
	}
	type leafInput struct {
		Name string `parameter:"Name,kind=query,in=name"`
	}
	conflicting := (fixture{input: reflect.TypeFor[conflictInput](), output: reflect.TypeFor[dependencyResult](), component: parent.Component}).registration(t)
	child := (fixture{input: reflect.TypeFor[leafInput](), output: reflect.TypeFor[dependencyResult](), component: cyclic.Component}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(conflicting, child))
	require.ErrorContains(t, err, "conflicting transitive transport input")
	require.Nil(t, doc)
}

func TestDependencyBetweenDistinctRoutesOfSameComponent(t *testing.T) {
	type input struct {
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/same/leaf,uri=/same"`
		Name  string            `parameter:"Name,kind=query,in=name,required=true,uri=/same/leaf"`
	}
	entry := (fixture{input: reflect.TypeFor[input](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Same"}, Routes: []*spec.Route{{Method: "GET", Path: "/same"}, {Method: "GET", Path: "/same/leaf"}}}, execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*input)
		if input.Child != nil {
			return input.Child, nil
		}
		return &dependencyResult{Message: input.Name}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	require.Len(t, doc.Paths["/same"].Get.Parameters, 1)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("GET", "/same?name=Ada", nil))
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	var result dependencyResult
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Equal(t, "Ada", result.Message)
}
