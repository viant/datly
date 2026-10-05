package compiler

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBodyNullPolicyRejectsAuthoredBodyFormats(t *testing.T) {
	type input struct {
		View *formattedBodyRow `parameter:"View,kind=body,bodyNullPolicy=empty-record,required=true"`
	}
	_, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/rows"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if err == nil || !strings.Contains(err.Error(), "BodyNullPolicy does not support authored body date formats") {
		t.Fatalf("err=%v", err)
	}
}
func TestBodyNullPolicyCompilerRejectsTargetMatrix(t *testing.T) {
	type row struct{ Name string }
	for _, target := range []reflect.Type{reflect.TypeFor[row](), reflect.TypeFor[**row](), reflect.TypeFor[*int](), reflect.TypeFor[[]*row](), reflect.TypeFor[map[string]row](), reflect.TypeFor[[1]row]()} {
		typ := reflect.StructOf([]reflect.StructField{{Name: "View", Type: target}})
		_, err := BuildBindingSpecs(&spec.Component{Parameters: []*spec.Parameter{{Name: "View", Source: spec.BindSource{Kind: "body"}, BodyNullPolicy: "empty-record"}}}, typ, nil)
		if err == nil {
			t.Fatalf("accepted %s", target)
		}
	}
}

func TestBodyNullPolicySupportsOrdinaryTimeBody(t *testing.T) {
	type row struct{ When *time.Time }
	type input struct {
		View *row `parameter:"View,kind=body,bodyNullPolicy=empty-record"`
	}
	bindings, err := BuildBindingSpecs(&spec.Component{}, reflect.TypeFor[input](), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Transformer != nil || bindings[0].SourceType != nil || bindings[0].BodyNullPolicy != "empty-record" {
		t.Fatalf("bindings=%+v", bindings)
	}
}
