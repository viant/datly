package openapi_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
)

type collectionHTTPRow struct {
	CreativeID *int `json:"creativeId"`
}
type collectionHTTPInput struct {
	Body []collectionHTTPRow `parameter:"Body,kind=body" jsonx:"objectToSlice"`
}

func TestObjectToSliceWholeBodyHTTP(t *testing.T) {
	var actual []collectionHTTPRow
	calls := 0
	entry := (fixture{input: reflect.TypeFor[collectionHTTPInput](), execute: func(_ context.Context, inv handler.Invocation) (any, error) {
		calls++
		actual = inv.Input.(*collectionHTTPInput).Body
		return &envelope{}, nil
	}}).registration(t)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	request := doc.Paths["/records"].Post.RequestBody.Content["application/json"].Schema
	require.GreaterOrEqual(t, len(request.AnyOf), 2, "whole body must advertise object and array")
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, tc := range []struct {
		body        string
		code, count int
		nilSlice    bool
	}{
		{`{"creativeId":1}`, 200, 1, false},
		{`[{"creativeId":1},{"creativeId":2}]`, 200, 2, false},
		{`[]`, 200, 0, false},
		{`null`, 200, 0, true},
		{`{"creativeId":"bad"}`, 400, 0, false},
		{`1`, 400, 0, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			before := calls
			req := httptest.NewRequest("POST", "/records", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(rec, req)
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			require.Equal(t, tc.code == 200, calls > before)
			if tc.code == 200 {
				require.Len(t, actual, tc.count)
				require.Equal(t, tc.nilSlice, actual == nil)
			}
		})
	}
}
