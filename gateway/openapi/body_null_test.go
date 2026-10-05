package openapi_test

import (
	"context"
	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type nullHTTPRow struct {
	ID   int                      `json:"id"`
	Name string                   `json:"name"`
	Has  *struct{ ID, Name bool } `json:"-" setMarker:"true"`
}
type nullHTTPInput struct {
	View *nullHTTPRow         `parameter:"View,kind=body,required=true,bodyNullPolicy=empty-record"`
	Has  *struct{ View bool } `json:"-" setMarker:"true"`
}

func TestRequiredNullableBodyPolicyHTTP(t *testing.T) {
	calls := 0
	entry := (fixture{input: reflect.TypeFor[nullHTTPInput](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		calls++
		input := inv.Input.(*nullHTTPInput)
		require.NotNil(t, input.View)
		require.NotNil(t, input.View.Has)
		require.True(t, input.Has.View)
		if input.View.Name == "" {
			require.False(t, input.View.Has.ID)
			require.False(t, input.View.Has.Name)
		} else {
			require.True(t, input.View.Has.Name)
		}
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	body := doc.Paths["/records"].Post.RequestBody
	require.True(t, body.Required)
	require.Len(t, body.Content["application/json"].Schema.AnyOf, 2)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, tc := range []struct {
		body string
		code int
	}{{"", 400}, {"null", 200}, {" \n null \t", 200}, {"{}", 200}, {`{"name":"x"}`, 200}, {"n", 400}, {"null {}", 400}, {"3", 400}} {
		before := calls
		req := httptest.NewRequest("POST", "/records", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(rec, req)
		require.Equal(t, tc.code, rec.Code, rec.Body.String())
		require.Equal(t, tc.code == 200, calls > before, "body=%q", tc.body)
	}
}
