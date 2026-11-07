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

type scalarFallbackHTTPRow struct {
	Mode *string              `json:"mode" choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumStyle:"literal" enumUnknownString:"0" enumUnknownInteger:"0" enumCase:"fold"`
	Has  *struct{ Mode bool } `json:"-" setMarker:"true"`
}
type scalarFallbackHTTPInput struct {
	Body *scalarFallbackHTTPRow `parameter:"Body,kind=body"`
}

func TestScalarFallbackHTTPBinding(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/modes"}}}, InputType: reflect.TypeFor[scalarFallbackHTTPInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/modes"})
	if !ok {
		t.Fatal("missing route")
	}
	for _, tc := range []struct {
		raw                string
		want               string
		null, absent, fail bool
	}{
		{`{}`, "", false, true, false}, {`{"mode":null}`, "", true, false, false},
		{`{"mode":1}`, "SPEND_EVENLY", false, false, false},
		{`{"mode":9223372036854775807}`, "ASAP", false, false, false},
		{`{"mode":-9223372036854775808}`, "ASAP", false, false, false},
		{`{"mode":"spend_evenly"}`, "SPEND_EVENLY", false, false, false},
		{`{"mode":"Even"}`, "ASAP", false, false, false},
		{`{"mode":"1"}`, "ASAP", false, false, false},
		{`{"mode":3,"mode":null}`, "", true, false, false},
		{`{"mode":true,"mode":1}`, "", false, false, true},
		{`{"mode":1e0}`, "", false, false, true}, {`{"mode":1.0}`, "", false, false, true},
		{`{"mode":9223372036854775808}`, "", false, false, true},
		{`{"mode":-9223372036854775809}`, "", false, false, true},
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
			var actual scalarFallbackHTTPInput
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
				t.Fatalf("mode=%v expected=%s", actual.Body.Mode, tc.want)
			}
		})
	}
}
