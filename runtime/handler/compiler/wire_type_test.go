package compiler

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/mcp/tool/testdata/customjson"
	"github.com/viant/datly/spec"
)

type wireHTTPBody struct {
	Raw     json.RawMessage `json:"raw"`
	Label   string          `json:"label"`
	decoded bool
}

func (b *wireHTTPBody) UnmarshalJSON(data []byte) error {
	type plain wireHTTPBody
	if err := json.Unmarshal(data, (*plain)(b)); err != nil {
		return err
	}
	b.decoded = true
	return nil
}

type wireHTTPRepresentation wireHTTPBody
type wireHTTPInput struct {
	Body *wireHTTPBody `parameter:",kind=body"`
}

func TestWireTypePreservesHTTPBodyDecoder(t *testing.T) {
	for _, authority := range []bool{false, true} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/requests"}}, Parameters: []*spec.Parameter{{Name: "Body", Source: spec.BindSource{Kind: "body"}}}}
		if authority {
			component.Parameters[0].WireTypeExpr = "Representation"
		}
		compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[wireHTTPInput](), TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeFor[wireHTTPRepresentation](), nil }}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/requests"})
		if route.Fields()[0].SourceType() != reflect.TypeFor[*wireHTTPBody]() || route.Fields()[0].DestinationType() != reflect.TypeFor[*wireHTTPBody]() || route.Fields()[0].Binding().Transformer != nil {
			t.Fatal("binding replaced")
		}
		for _, payload := range []string{`{"raw":{"children":[1]},"label":"value"}`, `{"raw":[{"ancestor":2}]}`, `{"raw":{"groups":[{"value":3}]}}`, `{"label":"value"}`, `{"raw":null}`, `null`, ``} {
			request := httptest.NewRequest("POST", "/requests", strings.NewReader(payload))
			request.Header.Set("Content-Type", "application/json")
			providers, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(providers.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var actual wireHTTPInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan()))
			providers.Close()
			if err != nil {
				t.Fatalf("authority %v payload %q: %v", authority, payload, err)
			}
			var expected *wireHTTPBody
			if payload != "" {
				if err = json.Unmarshal([]byte(payload), &expected); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(actual.Body, expected) {
				t.Fatalf("authority %v payload %q decoder changed: %#v != %#v", authority, payload, actual.Body, expected)
			}
		}
	}
}

func TestWireTypeEffectiveRouteFiltering(t *testing.T) {
	type input struct {
		Body  *customjson.Request `parameter:",kind=body,uri=/requests" wireType:"Representation"`
		Query string              `parameter:",kind=query,in=q,uri=/search"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/requests"}, {Method: "GET", Path: "/search"}}}, InputType: reflect.TypeFor[input](), TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeFor[customjson.RequestWire](), nil }}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []spec.RouteRef{{Method: "POST", Path: "/requests"}, {Method: "GET", Path: "/search"}} {
		route, _ := compiled.Input.ForRoute(ref)
		fields := route.Fields()
		if len(fields) != 1 {
			t.Fatalf("effective fields: %+v", fields)
		}
		if ref.Method == "POST" && fields[0].WireType() != reflect.TypeFor[customjson.RequestWire]() || ref.Method == "GET" && fields[0].WireType() != nil {
			t.Fatalf("wire metadata crossed route: %+v", fields)
		}
	}
}

func TestWireTypeAuthoredRuntimeStruct(t *testing.T) {
	type input struct {
		Values json.RawMessage `parameter:",kind=body,in=values"`
	}
	for _, expression := range []string{
		"struct { Threshold int64 `json:\"threshold\"`; Metric int `json:\"metric\" choice:\"1\" choice:\"2\"` }",
		"GeneratedControls",
	} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/values"}}, Parameters: []*spec.Parameter{{Name: "Values", Source: spec.BindSource{Kind: "body", Name: "values"}, WireTypeExpr: expression}}}
		lookup := func(name string) (reflect.Type, error) {
			if name == "GeneratedControls" {
				return reflect.TypeFor[struct {
					Threshold int64 `json:"threshold"`
					Metric    int   `json:"metric" choice:"1" choice:"2"`
				}](), nil
			}
			return nil, nil
		}
		compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), TypeLookup: lookup}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/values"})
		field := route.Fields()[0]
		if field.SourceType() != reflect.TypeFor[json.RawMessage]() || field.WireType() == nil || field.WireType().Kind() != reflect.Struct || field.WireType().Field(0).Type != reflect.TypeFor[int64]() {
			t.Fatalf("authored shape replaced binding or lost types: %+v", field)
		}
		for _, bad := range []string{"[]int", "map[string]int", "*struct { Threshold int }", "struct { Threshold unknown }", "struct { broken"} {
			component.Parameters[0].WireTypeExpr = bad
			if _, err = New(Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile(); err == nil {
				t.Fatalf("admitted invalid shape %s", bad)
			}
		}
	}
}
