package gateway

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterNewOpenAPIAggregateRoute(t *testing.T) {
	router := &Router{config: &Config{}}

	route := router.NewOpenAPIAggregateRoute("/v1/api/meta/openapi", nil)

	require.NotNil(t, route)
	require.Equal(t, "/v1/api/meta/openapi", route.Path.URI)
	require.Equal(t, http.MethodGet, route.Path.Method)
	require.Equal(t, RouteOpenAPIKind, route.Kind)
	require.NotNil(t, route.Handler)
}

func TestRouterNewOpenAPIDocRoute(t *testing.T) {
	router := &Router{config: &Config{}}

	route := router.NewOpenAPIDocRoute("/v1/api/meta/doc", "/v1/api/meta/openapi")

	require.NotNil(t, route)
	require.Equal(t, "/v1/api/meta/doc", route.Path.URI)
	require.Equal(t, http.MethodGet, route.Path.Method)
	require.NotNil(t, route.Handler)

	recorder := httptest.NewRecorder()
	route.Handler(nil, recorder, nil)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Header().Get("Content-Type"), "text/html")
	body := recorder.Body.String()
	require.Contains(t, body, "swagger-ui")
	require.Contains(t, body, `"/v1/api/meta/openapi"`)
}

func TestWantsYAML(t *testing.T) {
	testCases := []struct {
		description string
		rawQuery    string
		accept      string
		expect      bool
	}{
		{description: "default is json", expect: false},
		{description: "format=yaml", rawQuery: "format=yaml", expect: true},
		{description: "format=yml", rawQuery: "format=yml", expect: true},
		{description: "format=json overrides accept", rawQuery: "format=json", accept: "application/yaml", expect: false},
		{description: "accept yaml", accept: "application/yaml", expect: true},
		{description: "accept json", accept: "application/json", expect: false},
	}

	for _, testCase := range testCases {
		request := &http.Request{
			URL:    &url.URL{RawQuery: testCase.rawQuery},
			Header: http.Header{},
		}
		if testCase.accept != "" {
			request.Header.Set("Accept", testCase.accept)
		}
		require.Equalf(t, testCase.expect, wantsYAML(request), testCase.description)
	}

	require.False(t, wantsYAML(nil))
}
