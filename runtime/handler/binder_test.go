package handler

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator/buildin"
	bindstate "github.com/viant/bindly/state"
	xhandler "github.com/viant/xdatly/handler"
)

func TestBinderOptionsPreserveCanonicalDefaultAndNativeOverrides(t *testing.T) {
	type source struct{ Value, Other string }
	type target struct {
		Value string `bind:"kind=input,in=Value"`
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(buildin.Struct("input", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	canonical := &source{Value: "canonical", Other: "planned"}
	var binder xhandler.Binder = NewBinder(injector, canonical)
	var shared bindly.Binder = binder
	var initial target
	if err = shared.Bind(context.Background(), &initial); err != nil || initial.Value != "canonical" {
		t.Fatalf("%+v %v", initial, err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(target{}), bindly.BindingSpec{Path: "Value", Name: "Value", Location: bindstate.Location{Kind: "input", In: "Other"}})
	if err != nil {
		t.Fatal(err)
	}
	var planned target
	if err = shared.Bind(context.Background(), &planned, bindly.WithPlan(plan)); err != nil || planned.Value != "planned" {
		t.Fatalf("%+v %v", planned, err)
	}
	var overridden target
	if err = shared.Bind(context.Background(), &overridden, bindly.WithSource(&source{Value: "explicit"})); err != nil || overridden.Value != "explicit" {
		t.Fatalf("%+v %v", overridden, err)
	}
	var after target
	if err = shared.Bind(context.Background(), &after); err != nil || after.Value != "canonical" {
		t.Fatalf("options leaked: %+v %v", after, err)
	}
}
