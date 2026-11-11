package openapi_test

import (
	"context"
	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type requiredCollectionHTTPInput struct {
	Body []collectionHTTPRow `parameter:"Body,kind=body,required=true" jsonx:"objectToSlice"`
}

func TestObjectToSliceNullContract(t *testing.T) {
	for _, required := range []bool{false, true} {
		typ := reflect.TypeFor[collectionHTTPInput]()
		if required {
			typ = reflect.TypeFor[requiredCollectionHTTPInput]()
		}
		entry := (fixture{input: typ}).registration(t)
		document, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
		require.NoError(t, err)
		request := document.Paths["/records"].Post.RequestBody
		require.Equal(t, required, request.Required)
		schema := request.Content["application/json"].Schema
		proof := plainProof{t, document}
		runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
		require.NoError(t, err)
		for _, tc := range []struct {
			raw   string
			value any
			valid bool
		}{
			{`null`, nil, !required},
			{`[]`, []any{}, !required},
			{`[null]`, []any{nil}, true},
			{`{"creativeId":1}`, map[string]any{"creativeId": float64(1)}, true},
			{`[{"creativeId":1}]`, []any{map[string]any{"creativeId": float64(1)}}, true},
		} {
			t.Run(tc.raw+map[bool]string{true: " required", false: " optional"}[required], func(t *testing.T) {
				req := httptest.NewRequest("POST", "/records", strings.NewReader(tc.raw))
				req.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				gatewayhttp.NewHandler(runtime, nil, "test").ServeHTTP(recorder, req)
				require.Equal(t, tc.valid, recorder.Code == 200, recorder.Body.String())
				require.Equal(t, tc.valid, proof.validate(schema, tc.value), "schema disagrees with HTTP")
			})
		}
	}
}
