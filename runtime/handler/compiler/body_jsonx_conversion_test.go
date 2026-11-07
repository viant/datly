package compiler

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type jsonxHTTPBody struct {
	Enabled *bool    `json:"enabled" jsonx:"intToBool"`
	Catalog *string  `json:"catalog" jsonx:"numberToString"`
	Names   []string `json:"names" jsonx:"commaStringToSlice"`
	Billed  *string  `json:"billed" jsonx:"boolIntToYN"`
}
type jsonxHTTPInput struct {
	Body *jsonxHTTPBody `parameter:"Body,kind=body"`
}

func TestNativeJSONXHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/jsonx"}}}, InputType: reflect.TypeFor[jsonxHTTPInput]()}).Compile()
	require.NoError(t, err)
	route, found := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/jsonx"})
	require.True(t, found)
	for _, tc := range []struct {
		raw  string
		fail bool
	}{
		{`{"enabled":1,"catalog":9007199254740993,"names":"a, b,,","billed":-7}`, false},
		{`{"enabled":2}`, true}, {`{"enabled":"1"}`, true}, {`{"catalog":true}`, true}, {`{"names":[1]}`, true}, {`{"billed":"true"}`, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/jsonx", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			provider, err := requestprovider.New(req)
			require.NoError(t, err)
			defer provider.Close()
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := injector.ForScope(provider.Providers()...)
			require.NoError(t, err)
			var actual jsonxHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, true, *actual.Body.Enabled)
			require.Equal(t, "9007199254740993", *actual.Body.Catalog)
			require.Equal(t, []string{"a", "b"}, actual.Body.Names)
			require.Equal(t, "Y", *actual.Body.Billed)
		})
	}
}
