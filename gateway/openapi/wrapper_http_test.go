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
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
)

type wrapperHTTPInput struct {
	Body []int `parameter:"Body,kind=body" jsonx:"wrapper=ids"`
}

type requiredWrapperHTTPInput struct {
	Body []int `parameter:"Body,kind=body,required=true" jsonx:"wrapper=ids"`
}

func TestWrapperWholeBodyHTTP(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			typ := reflect.TypeFor[wrapperHTTPInput]()
			if required {
				typ = reflect.TypeFor[requiredWrapperHTTPInput]()
			}
			var actual []int
			calls := 0
			entry := (fixture{input: typ, execute: func(_ context.Context, inv handler.Invocation) (any, error) {
				calls++
				actual = reflect.ValueOf(inv.Input).Elem().FieldByName("Body").Interface().([]int)
				return &envelope{}, nil
			}}).registration(t)
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			for _, tc := range []struct {
				raw   string
				want  []int
				valid bool
			}{
				{"", nil, !required}, {`null`, nil, !required}, {`[]`, []int{}, !required},
				{`[1,2]`, []int{1, 2}, true}, {`{"ids":[1,2]}`, []int{1, 2}, true},
				{`{"ids":[]}`, []int{}, !required}, {`{"IDS":[3]}`, []int{3}, true},
				{`{"ids":[7],"ids":[null]}`, []int{7}, true},
				{`{}`, nil, false}, {`{"ids":null}`, nil, false},
				{`{"ids":[1],"other":2}`, nil, false}, {`{"other":[1]}`, nil, false},
				{`{"ids":[1.5]}`, nil, false}, {`{"ids":"bad","ids":[1]}`, nil, false},
				{`{"ids":[1]} {}`, nil, false}, {`1`, nil, false},
			} {
				t.Run(tc.raw, func(t *testing.T) {
					before := calls
					req := httptest.NewRequest("POST", "/records", strings.NewReader(tc.raw))
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					gatewayhttp.NewHandler(runtime, nil, "test").ServeHTTP(rec, req)
					if !tc.valid {
						require.Equal(t, 400, rec.Code, rec.Body.String())
						require.Equal(t, before, calls)
						return
					}
					require.Equal(t, 200, rec.Code, rec.Body.String())
					require.Equal(t, before+1, calls)
					require.Equal(t, tc.want, actual)
				})
			}
		})
	}
}

func TestWrapperWholeBodyHTTPSchema(t *testing.T) {
	for _, required := range []bool{false, true} {
		typ := reflect.TypeFor[wrapperHTTPInput]()
		if required {
			typ = reflect.TypeFor[requiredWrapperHTTPInput]()
		}
		entry := (fixture{input: typ}).registration(t)
		document, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
		require.NoError(t, err)
		request := document.Paths["/records"].Post.RequestBody
		require.Equal(t, required, request.Required)
		schema := request.Content["application/json"].Schema
		var object, array *openapi3.Schema
		for _, branch := range schema.AnyOf {
			if branch.Type == "object" {
				object = branch
			}
			if branch.Type == "array" {
				array = branch
			}
		}
		require.NotNil(t, object, "wrapper object must be advertised")
		require.NotNil(t, array)
		require.Equal(t, []string{"ids"}, object.Required)
		require.NotNil(t, object.AdditionalPropertiesAllowed)
		require.False(t, *object.AdditionalPropertiesAllowed)
		member := object.Properties["ids"]
		require.NotNil(t, member)
		require.Equal(t, "array", member.Type)
		require.False(t, member.Nullable)
		require.Equal(t, array.Items, member.Items)
		require.Equal(t, array.MinItems, member.MinItems)
		if required {
			require.EqualValues(t, 1, member.MinItems)
		}
		encoded, err := json.Marshal(object)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"additionalProperties":false`)
	}
}
