package selectorcompanion_test

import (
	"context"
	"github.com/viant/bindly/locator"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	compiler "github.com/viant/datly/runtime/handler/compiler"
	provider "github.com/viant/datly/runtime/handler/provider"
	"reflect"
	"testing"
)

func TestCarryPreparedRejectsForeignInvocationAndCopiesSelectors(t *testing.T) {
	plan, _ := runtimePlan(t)
	original := &fixture.Input{}
	prepared := &fixture.Input{}
	value := reflect.New(plan.Type())
	value.Elem().FieldByName("Fields").Set(reflect.ValueOf([]string{"id"}))
	value.Elem().FieldByName("Page").SetInt(2)
	binder := &delegatingBinder{}
	source, err := plan.Carry(binder, original, value.Interface())
	if err != nil {
		t.Fatal(err)
	}
	other, _ := runtimePlan(t)
	for name, run := range map[string]func() error{
		"missing carrier":  func() error { _, e := plan.CarryPrepared(binder, original, prepared, binder); return e },
		"foreign plan":     func() error { _, e := other.CarryPrepared(source, original, prepared, binder); return e },
		"foreign original": func() error { _, e := plan.CarryPrepared(source, &fixture.Input{}, prepared, binder); return e },
		"same pointer":     func() error { _, e := plan.CarryPrepared(source, original, original, binder); return e },
		"wrong type":       func() error { _, e := plan.CarryPrepared(source, original, new(int), binder); return e },
		"nil input":        func() error { _, e := plan.CarryPrepared(source, original, (*fixture.Input)(nil), binder); return e },
		"nil binder":       func() error { _, e := plan.CarryPrepared(source, original, prepared, nil); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if run() == nil {
				t.Fatal("invalid transfer admitted")
			}
		})
	}
	carried, err := plan.CarryPrepared(source, original, prepared, binder)
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.Value(carried, prepared)
	if err != nil {
		t.Fatal(err)
	}
	got.Elem().FieldByName("Fields").Index(0).SetString("changed")
	if value.Elem().FieldByName("Fields").Index(0).String() != "id" || got.Elem().FieldByName("Page").Int() != 2 {
		t.Fatal("selector state not preserved and detached")
	}
}

func TestDerivedPublicBindingRaisesOnlyCompanionStagePriority(t *testing.T) {
	for _, kind := range []string{"input", "param"} {
		t.Run(kind, func(t *testing.T) {
			c := runtimeComponent(t)
			c.Parameters[0].Source.Kind = kind
			c.Parameters[0].Source.Name = "Auth.ID"
			compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			plan := compiled.SelectorPlan
			if compiled.Bindings[0].Priority != 0 {
				t.Fatal("public binding priority mutated")
			}
			for _, b := range plan.Bindings() {
				if b.Priority != locator.PriorityDependent {
					t.Fatal("companion stage priority not represented")
				}
			}
			query := provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, nil })
			public := provider.Named(kind, func(context.Context, reflect.Type, string) (any, bool, error) { return 7, true, nil })
			if err := plan.ValidateProviders([]locator.Provider{priorityProvider{Provider: public, priority: 10}, query}); err != nil {
				t.Fatal(err)
			}
			if err := plan.ValidateProviders([]locator.Provider{priorityProvider{Provider: public, priority: 21}, query}); err == nil {
				t.Fatal("higher public priority admitted")
			}
		})
	}
}
