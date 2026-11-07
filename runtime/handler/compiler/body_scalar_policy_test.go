package compiler

import (
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type scalarPolicyHTTPRow struct {
	Mode *int64               `json:"mode" choice:"1:FIXED_BUDGET" choice:"2:AUTO_BUDGET" enumAccept:"scalar" enumInteger:"passthrough" enumUnknownString:"0" enumCase:"upper"`
	Has  *struct{ Mode bool } `json:"-" setMarker:"true"`
}
type scalarPolicyHTTPInput struct {
	Body *scalarPolicyHTTPRow `parameter:"Body,kind=body"`
}

func TestOpenScalarHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/modes"}}}, InputType: reflect.TypeFor[scalarPolicyHTTPInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/modes"})
	if !ok {
		t.Fatal("missing route")
	}
	for _, tc := range []struct {
		raw                string
		want               int64
		null, absent, fail bool
	}{
		{`{}`, 0, false, true, false}, {`{"mode":null}`, 0, true, false, false},
		{`{"mode":9223372036854775807}`, 9223372036854775807, false, false, false},
		{`{"mode":-9223372036854775808}`, -9223372036854775808, false, false, false},
		{`{"mode":"fıxed_budget"}`, 1, false, false, false}, {`{"mode":"1"}`, 0, false, false, false},
		{`{"mode":"AUTO_BUDGET"}`, 2, false, false, false}, {`{"mode":" FIXED_BUDGET "}`, 0, false, false, false},
		{`{"mode":3,"mode":null}`, 0, true, false, false}, {`{"mode":true,"mode":1}`, 0, false, false, true},
		{`{"mode":1e0}`, 0, false, false, true}, {`{"mode":1.0}`, 0, false, false, true},
		{`{"mode":9223372036854775808}`, 0, false, false, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/modes", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			provider, err := requestprovider.New(req)
			if err != nil {
				t.Fatal(err)
			}
			defer provider.Close()
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(provider.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var actual scalarPolicyHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				return
			}
			if actual.Body == nil {
				t.Fatal("nil body")
			}
			present := actual.Body.Has != nil && actual.Body.Has.Mode
			if present == tc.absent {
				t.Fatalf("presence=%v", present)
			}
			if tc.null || tc.absent {
				if actual.Body.Mode != nil {
					t.Fatal("expected nil")
				}
			} else if actual.Body.Mode == nil || *actual.Body.Mode != tc.want {
				t.Fatalf("mode=%v expected=%d", actual.Body.Mode, tc.want)
			}
		})
	}
}
