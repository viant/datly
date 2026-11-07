package compiler

import (
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
)

func TestDeclaredBodyTimePolicyHasSingleDecoderOwner(t *testing.T) {
	type row struct {
		Stamp *time.Time `json:"stamp" format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02T15:04:05.999999999Z07:00|2006-01-02,timeEpoch=milliseconds,timeEmpty=preserve,timezone=UTC,nullable=true"`
	}
	type input struct{ Body []row }
	field := reflect.TypeFor[input]().Field(0)
	binding := bindly.BindingSpec{Location: bindstate.Location{Kind: "body"}}
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Transformer != nil {
		t.Fatal("declared native time policy must not acquire a second format transformer")
	}
}

type declaredTimeRow struct {
	Stamp *time.Time            `json:"stamp,omitempty" format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02T15:04:05.999999999Z07:00|2006-01-02T15:04:05|2006-01-02 15:04:05|2006-01-02,timeEpoch=milliseconds,timeEmpty=preserve,timezone=UTC,nullable=true"`
	Has   *struct{ Stamp bool } `json:"-" setMarker:"true"`
}
type declaredTimeHTTPInput struct {
	Body *declaredTimeRow `parameter:"Body,kind=body"`
}

func TestDeclaredTimeHTTPBindingAndOutput(t *testing.T) {
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/times"}}}, InputType: reflect.TypeFor[declaredTimeHTTPInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/times"})
	if !ok {
		t.Fatal("missing route")
	}
	out, err := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeFor[declaredTimeRow](), Component: &spec.Component{}})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := out.JSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	stampSchema := properties["stamp"].(map[string]any)
	if stampSchema["format"] != "date-time" || stampSchema["anyOf"] != nil {
		t.Fatalf("output inherited input alternatives: %+v", stampSchema)
	}
	for _, tc := range []struct {
		raw, want string
		fail      bool
	}{
		{`{"stamp":0}`, `{"stamp":"1970-01-01T00:00:00Z"}`, false},
		{`{"stamp":""}`, `{"stamp":null}`, false},
		{`{"stamp":"2026-11-07T08:00:00-08:00"}`, `{"stamp":"2026-11-07T16:00:00Z"}`, false},
		{`{"stamp":"2026-11-07","stamp":""}`, `{"stamp":"2026-11-07T00:00:00Z"}`, false},
		{`{"stamp":"2026-11-07","stamp":null}`, `{}`, false},
		{`{"stamp":"bad","stamp":"2026-11-07"}`, "", true},
		{`{"stamp":1e3}`, "", true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("PATCH", "/times", strings.NewReader(tc.raw))
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
			var actual declaredTimeHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				return
			}
			if actual.Body.Has == nil || !actual.Body.Has.Stamp {
				t.Fatal("lost presence")
			}
			encoded, err := out.Encode(t.Context(), "json", actual.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded.Data) != tc.want {
				t.Fatalf("got %s want %s stamp=%#v zero=%v", encoded.Data, tc.want, actual.Body.Stamp, actual.Body.Stamp != nil && actual.Body.Stamp.IsZero())
			}
		})
	}
}
