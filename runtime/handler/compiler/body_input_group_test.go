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

type groupHTTPBody struct {
	Low  *int                      `json:"low" format:"inputGroup=source,inputPriority=0,inputCase=exact"`
	High *int                      `json:"high" format:"inputGroup=source,inputPriority=1,inputCase=exact"`
	Has  *struct{ Low, High bool } `json:"-" setMarker:"true"`
}
type groupHTTPInput struct {
	Body *groupHTTPBody `parameter:"Body,kind=body"`
}

func TestInputGroupHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/groups"}}}, InputType: reflect.TypeFor[groupHTTPInput]()}).Compile()
	require.NoError(t, err)
	route, found := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/groups"})
	require.True(t, found)
	for _, tc := range []struct {
		raw             string
		low, high, fail bool
		want            int
		null            bool
	}{
		{`{"low":"ignored","high":2}`, false, true, false, 2, false},
		{`{"high":2,"low":"ignored"}`, false, true, false, 2, false},
		{`{"low":"ignored","high":null}`, false, true, false, 0, true},
		{`{"low":1,"HIGH":2}`, true, false, false, 1, false},
		{`{"low":1,"high":"bad"}`, false, false, true, 0, false},
		{`{"low":invalid,"high":2}`, false, false, true, 0, false},
		{`{"Has":{"High":true},"low":1}`, true, false, false, 1, false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/groups", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			provider, err := requestprovider.New(req)
			require.NoError(t, err)
			defer provider.Close()
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := injector.ForScope(provider.Providers()...)
			require.NoError(t, err)
			var actual groupHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, actual.Body.Has)
			require.Equal(t, tc.low, actual.Body.Has.Low)
			require.Equal(t, tc.high, actual.Body.Has.High)
			if tc.null {
				require.Nil(t, actual.Body.High)
			} else if tc.high {
				require.Equal(t, tc.want, *actual.Body.High)
			} else {
				require.Equal(t, tc.want, *actual.Body.Low)
			}
		})
	}
}
