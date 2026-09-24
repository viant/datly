package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

type codecPathScope struct {
	IDs   []string
	inner []string
}

type codecPathContext struct {
	Scope *codecPathScope
}

// A derived param may address a field inside another bound parameter, such as
// a component dependency output; the codec must receive that field's type as
// its source and the declared contract as its destination.
func TestBuildParamCodecsResolvesDottedParamSource(t *testing.T) {
	type input struct {
		Auth *codecPathContext
		IDs  []int
	}
	required := true
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Auth", Source: spec.BindSource{Kind: "component", Name: "GET:/context"}, Required: &required},
		{Name: "IDs", Source: spec.BindSource{Kind: "param", Name: "Auth.Scope.IDs"}, TypeExpr: "[]string", OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: "EntityIDs"}},
	}}
	factory := &transportCodecFactory{codec: &transportCodec{}}
	codecs, err := (ParamCodecCompiler{Component: component, InputType: reflect.TypeOf(input{}), Factory: factory}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := codecs["IDs"]; !ok || factory.config == nil {
		t.Fatalf("codecs=%+v config=%+v", codecs, factory.config)
	}
	if factory.config.SourceType != reflect.TypeOf([]string{}) || factory.config.DestinationType != reflect.TypeOf([]int{}) {
		t.Fatalf("codec config = %+v", factory.config)
	}
}

func TestBuildParamCodecsRejectsUnresolvableDottedSource(t *testing.T) {
	type input struct {
		Auth *codecPathContext
		IDs  []int
	}
	for name, source := range map[string]string{
		"unknown field":    "Auth.Scope.Missing",
		"unexported field": "Auth.Scope.inner",
		"through slice":    "Auth.Scope.IDs.Value",
		"unknown root":     "Other.Scope.IDs",
		"empty segment":    "Auth..IDs",
	} {
		t.Run(name, func(t *testing.T) {
			component := &spec.Component{Parameters: []*spec.Parameter{
				{Name: "Auth", Source: spec.BindSource{Kind: "component", Name: "GET:/context"}},
				{Name: "IDs", Source: spec.BindSource{Kind: "param", Name: source}, TypeExpr: "[]string", OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: "EntityIDs"}},
			}}
			_, err := (ParamCodecCompiler{Component: component, InputType: reflect.TypeOf(input{}), Factory: &transportCodecFactory{codec: &transportCodec{}}}).Build()
			if err == nil || !strings.Contains(err.Error(), `codec param "IDs" source`) {
				t.Fatalf("source %q error = %v", source, err)
			}
		})
	}
}
