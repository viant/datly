package openapi_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestFixedQueryArrayRetainsRuntimeBounds(t *testing.T) {
	type input struct {
		IDs [2]int `parameter:"IDs,kind=query,in=ids,required=true"`
	}
	entry := (fixture{input: reflect.TypeFor[input](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		require.Equal(t, [2]int{1, 2}, inv.Input.(*input).IDs)
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	parameter := doc.Paths["/records"].Post.Parameters[0]
	require.Equal(t, "array", parameter.Schema.Type)
	require.EqualValues(t, 2, parameter.Schema.MinItems)
	require.EqualValues(t, 2, *parameter.Schema.MaxItems)
	require.True(t, *parameter.Explode)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, tc := range []struct {
		query  string
		length int
		ok     bool
	}{{"?ids=1", 1, false}, {"?ids=1&ids=2", 2, true}, {"?ids=1&ids=2&ids=3", 3, false}} {
		admitted := uint64(tc.length) >= parameter.Schema.MinItems && uint64(tc.length) <= *parameter.Schema.MaxItems
		require.Equal(t, tc.ok, admitted)
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("POST", "/records"+tc.query, nil))
		require.Equal(t, tc.ok, recorder.Code == 200, recorder.Body.String())
	}
}

func TestNamedByteFormUsesRepeatedNumbers(t *testing.T) {
	type namedBytes []byte
	type input struct {
		Data namedBytes `parameter:"Data,kind=form,in=data,required=true"`
	}
	entry := (fixture{input: reflect.TypeFor[input](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		require.Equal(t, namedBytes{1, 2}, inv.Input.(*input).Data)
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	schema := doc.Paths["/records"].Post.RequestBody.Content["application/x-www-form-urlencoded"].Schema.Properties["data"]
	require.Equal(t, "array", schema.Type)
	require.Equal(t, "integer", schema.Items.Type)
	require.Empty(t, schema.Format)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, tc := range []struct {
		body string
		ok   bool
	}{{"data=1&data=2", true}, {"data=AQI%3D", false}} {
		req := httptest.NewRequest("POST", "/records", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
		require.Equal(t, tc.ok, recorder.Code == 200, recorder.Body.String())
	}
	type exactInput struct {
		Data []byte `parameter:"Data,kind=form,in=data"`
	}
	exact := (fixture{input: reflect.TypeFor[exactInput]()}).registration(t)
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(exact))
	require.ErrorContains(t, err, "byte input requires explicit wire encoding support")
}

func TestCompiledOutputPlanControlsDocumentation(t *testing.T) {
	for _, compiledExclusion := range []bool{true, false} {
		t.Run(map[bool]string{true: "transformed plan and plain source", false: "plain plan and transformed source"}[compiledExclusion], func(t *testing.T) {
			entry := (fixture{execute: func(context.Context, handler.Invocation) (any, error) {
				return &envelope{Data: []child{{Value: "visible"}}}, nil
			}}).registration(t)
			source := entry.Component.Clone()
			source.Settings = &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Detail"}}}
			if compiledExclusion {
				var err error
				entry.Output, err = (output.Compiler{}).Compile(output.CompileInput{Component: source, Type: entry.OutputType})
				require.NoError(t, err)
				source.Settings.Output.Exclude[0] = "Data" // encoding authority is already compiled
			} else {
				entry.Component.Settings = source.Settings
			}
			doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
			require.NoError(t, err)
			root := (documentAssertion{t, doc}).deref(doc.Paths["/records"].Post.Responses["200"].Content["application/json"].Schema)
			if compiledExclusion {
				require.NotContains(t, root.Properties, "detail")
				require.NotContains(t, root.Required, "detail")
			} else {
				require.Contains(t, root.Required, "detail")
			}

			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, httptest.NewRequest("POST", "/records", nil))
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			var value map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value))
			_, present := value["detail"]
			require.Equal(t, !compiledExclusion, present)
		})
	}
}
