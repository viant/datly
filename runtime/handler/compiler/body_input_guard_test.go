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

type guardHTTPChild struct {
	Value *int                  `json:"value" format:"inputUnless=../Canonical" jsonx:"boolToInt"`
	Has   *struct{ Value bool } `json:"-" setMarker:"true"`
}
type guardHTTPBody struct {
	Canonical *int                            `json:"canonical"`
	Rows      map[string][]*guardHTTPChild    `json:"rows"`
	Has       *struct{ Canonical, Rows bool } `json:"-" setMarker:"true"`
}
type guardHTTPInput struct {
	Body *guardHTTPBody `parameter:"Body,kind=body"`
}

func TestInputGuardHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/guards"}}}, InputType: reflect.TypeFor[guardHTTPInput]()}).Compile()
	require.NoError(t, err)
	route, found := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/guards"})
	require.True(t, found)
	for _, tc := range []struct {
		raw              string
		suppressed, fail bool
	}{
		{`{"canonical":null,"rows":{"a":[{"value":"ignored","Has":{"Value":true}}]}}`, true, false},
		{`{"rows":{"a":[{"value":{}}]},"canonical":2}`, true, false},
		{`{"rows":{"a":[{"value":true}]}}`, false, false},
		{`{"rows":{"a":[{"value":"bad"}]}}`, false, true},
		{`{"Has":{"Canonical":true},"rows":{"a":[{"value":"bad"}]}}`, false, true},
		{`{"canonical":2,"rows":{"a":[{"value":invalid}]}}`, true, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/guards", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			provider, err := requestprovider.New(req)
			require.NoError(t, err)
			defer provider.Close()
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := injector.ForScope(provider.Providers()...)
			require.NoError(t, err)
			var actual guardHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, actual.Body.Has)
			row := actual.Body.Rows["a"][0]
			require.NotNil(t, row.Has)
			require.Equal(t, tc.suppressed, actual.Body.Has.Canonical)
			require.Equal(t, !tc.suppressed, row.Has.Value)
			if tc.suppressed {
				require.Nil(t, row.Value)
			} else {
				require.Equal(t, 1, *row.Value)
			}
		})
	}
}
