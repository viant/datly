package compiler

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
)

type inputAliasHTTPConstraint struct {
	Limit int `json:"limit"`
}
type inputAliasHTTPBody struct {
	Frequency *inputAliasHTTPConstraint `json:"frequencyCapping,omitempty" format:"inputNames=frequencyCap,inputNamesPriority=canonical,inputNamesKinds=object|null,inputNamesCase=fold"`
	Has       *struct{ Frequency bool } `json:"-" setMarker:"true"`
}
type inputAliasHTTPInput struct {
	Body *inputAliasHTTPBody `parameter:"Body,kind=body"`
}

func TestInputAliasHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/aliases"}}}, InputType: reflect.TypeFor[inputAliasHTTPInput]()}).Compile()
	require.NoError(t, err)
	route, found := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/aliases"})
	require.True(t, found)
	for _, tc := range []struct {
		raw                 string
		limit               int
		present, null, fail bool
	}{
		{`{}`, 0, false, true, false},
		{`{"frequencyCap":5}`, 0, false, true, false},
		{`{"FREQUENCYCAP":{"limit":2}}`, 2, true, false, false},
		{`{"frequencyCap":null}`, 0, true, true, false},
		{`{"frequencyCap":{"limit":"ignored"},"frequencyCapping":{"limit":3}}`, 3, true, false, false},
		{`{"frequencyCapping":{"limit":3},"frequencyCap":{"limit":"ignored"}}`, 3, true, false, false},
		{`{"FREQUENCYCAPPING":{"limit":3},"frequencyCap":{"limit":"ignored"}}`, 3, true, false, false},
		{`{"frequencyCapping":{"limit":"invalid"},"frequencyCap":{"limit":3}}`, 0, false, false, true},
		{`{"frequencyCapping":null,"frequencyCap":{"limit":3}}`, 0, true, true, false},
		{`{"frequencyCapping":{"limit":3},"frequencyCap":invalid}`, 0, false, false, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/aliases", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			provider, err := requestprovider.New(req)
			require.NoError(t, err)
			defer provider.Close()
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := injector.ForScope(provider.Providers()...)
			require.NoError(t, err)
			var actual inputAliasHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, actual.Body)
			require.NotNil(t, actual.Body.Has)
			require.Equal(t, tc.present, actual.Body.Has.Frequency)
			require.Equal(t, tc.null, actual.Body.Frequency == nil)
			if !tc.null {
				require.Equal(t, tc.limit, actual.Body.Frequency.Limit)
			}
		})
	}
}
