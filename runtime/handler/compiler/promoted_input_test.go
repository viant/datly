package compiler

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type PromotedInputFields struct {
	Tenant string `parameter:"Tenant,kind=header,in=Tenant,required"`
}
type promotedInput struct{ PromotedInputFields }

func TestBindingSpecsIncludePromotedInput(t *testing.T) {
	c := &spec.Component{Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "header", Name: "Tenant"}}}}
	bindings, err := BuildBindingSpecs(c, reflect.TypeOf(promotedInput{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 || bindings[0].Path != "Tenant" {
		t.Fatalf("promoted binding=%+v", bindings)
	}
}

type promotedPointerInput struct{ *PromotedInputFields }
type promotedShadowInput struct {
	PromotedInputFields
	Tenant string
}
type promotedOtherFields struct{ Tenant string }
type promotedAmbiguousInput struct {
	PromotedInputFields
	promotedOtherFields
}

func TestPromotedInputBindsThroughNativePlan(t *testing.T) {
	for _, input := range []any{&promotedInput{}, &promotedPointerInput{}, &promotedShadowInput{}} {
		typ := reflect.TypeOf(input).Elem()
		t.Run(typ.Name(), func(t *testing.T) {
			c := &spec.Component{Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "header", Name: "Tenant"}}}}
			specs, err := BuildBindingSpecs(c, typ, nil)
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(values.New("header", map[string]any{"Tenant": "acme"})))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := injector.CompilePlan(typ, specs...)
			if err != nil {
				t.Fatal(err)
			}
			if err = injector.Bind(context.Background(), input, bindly.WithPlan(plan)); err != nil {
				t.Fatal(err)
			}
			if reflect.ValueOf(input).Elem().FieldByName("Tenant").String() != "acme" {
				t.Fatalf("not bound: %+v", input)
			}
			if shadow, ok := input.(*promotedShadowInput); ok && shadow.PromotedInputFields.Tenant != "" {
				t.Fatal("shadowed embedded field was populated")
			}
			projection, err := plan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			value, found, err := projection.Value(input, "Tenant")
			if err != nil || !found || value != "acme" {
				t.Fatalf("projection=%v %v %v", value, found, err)
			}
		})
	}
	c := &spec.Component{Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "header", Name: "Tenant"}}}}
	if _, err := BuildBindingSpecs(c, reflect.TypeOf(promotedAmbiguousInput{}), nil); err == nil {
		t.Fatal("ambiguous promotion was accepted")
	}
}
