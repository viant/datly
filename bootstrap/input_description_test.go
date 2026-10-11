package bootstrap

import (
	"reflect"
	"testing"

	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
)

func TestAuthoringInputDescriptionKeepsCodecWireContract(t *testing.T) {
	type decoded struct{ ID int }
	type input struct{ Value decoded }
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/data"}}, Parameters: []*spec.Parameter{{Name: "Value", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "X-Value"}, Codec: &spec.Codec{Body: "deploymentOwnedCodec"}}}}
	described, err := DescribeInputContract(component, reflect.TypeFor[input](), nil)
	if err != nil {
		t.Fatal(err)
	}
	route, ok := described.ForRoute(spec.RouteRef{Method: "GET", Path: "/data"})
	if !ok {
		t.Fatal("route missing")
	}
	fields := route.Fields()
	if len(fields) != 1 || fields[0].SourceType() != reflect.TypeFor[string]() || fields[0].DestinationType() != reflect.TypeFor[decoded]() {
		t.Fatalf("codec wire authority changed: %+v", fields)
	}
	if _, err = handlercompiler.New(handlercompiler.Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile(); err == nil {
		t.Fatal("metadata description bypassed missing runtime codec provider")
	}
}
