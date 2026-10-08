package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestDescribeResolvesCodecWireTypesWithoutFactory(t *testing.T) {
	type decoded struct{ ID int }
	type input struct{ Value decoded }
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/data"}}, Parameters: []*spec.Parameter{{Name: "Value", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "X-Value"}, Codec: &spec.Codec{Body: "deploymentOwnedCodec"}}}}
	described, err := New(Input{Component: component, InputType: reflect.TypeFor[input]()}).Describe()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := described.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/data"})
	if !ok {
		t.Fatal("route missing")
	}
	fields := route.Fields()
	if len(fields) != 1 || fields[0].SourceType() != reflect.TypeFor[string]() || fields[0].DestinationType() != reflect.TypeFor[decoded]() {
		t.Fatalf("wire contract not preserved: %+v", fields)
	}
	if _, err := New(Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile(); err == nil {
		t.Fatal("execution compiler silently accepted a missing codec")
	}
}
