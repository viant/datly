package handler_test

import (
	"context"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
)

func TestHandlerFuncPreservesCanonicalInvocation(t *testing.T) {
	input := &struct{ ID int }{ID: 7}
	invocation := rhandler.Invocation{Input: input}
	called := false
	handler := rhandler.HandlerFunc(func(_ context.Context, actual rhandler.Invocation) (any, error) {
		called = true
		if actual.Input != input {
			t.Fatalf("invocation input = %v", actual.Input)
		}
		return actual.Input, nil
	})
	actual, err := handler.Execute(context.Background(), invocation)
	if err != nil || !called || actual != input {
		t.Fatalf("Execute() = (%v, %v), called=%v", actual, err, called)
	}
}

func TestBinderUsesScopedBindlyProviders(t *testing.T) {
	type address struct {
		City string
	}
	type inputType struct {
		ID      string
		Address address
	}
	input := &inputType{ID: "7", Address: address{City: "Warsaw"}}
	tenantKey := xhandler.ValueKey("tenant")
	injector, err := bindly.NewInjector(bindly.WithProviders(
		handlerprovider.Input(),
		handlerprovider.Static(tenantKey, "acme"),
	))
	if err != nil {
		t.Fatal(err)
	}
	binder := rhandler.NewBinder(injector, input)
	resolvedInput, found, err := binder.Lookup(context.Background(), xhandler.InputKey)
	if err != nil || !found || resolvedInput != input {
		t.Fatalf("input Lookup() = (%v, %v, %v)", resolvedInput, found, err)
	}
	tenant, found, err := binder.Lookup(context.Background(), tenantKey)
	if err != nil || !found || tenant != "acme" {
		t.Fatalf("tenant Lookup() = (%v, %v, %v)", tenant, found, err)
	}
	target := &struct {
		Input   *inputType `parameter:"Input,kind=input"`
		ID      int        `parameter:"ID,kind=input,in=ID"`
		City    string     `parameter:"City,kind=input,in=Address.City"`
		Missing string     `parameter:"Missing,kind=input,in=Missing,required=false"`
		Tenant  string     `parameter:"Tenant,kind=tenant"`
	}{}
	if err = binder.Bind(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if target.Input != input || target.ID != 7 || target.City != "Warsaw" || target.Missing != "" || target.Tenant != "acme" {
		t.Fatalf("bound target = %#v", target)
	}
}

func TestCapabilityKeysAreDistinct(t *testing.T) {
	keys := []xhandler.ValueKey{
		rhandler.LoggerCapabilityKey,
		rhandler.ValidatorCapabilityKey,
		rhandler.MessageBusCapabilityKey,
		rhandler.ConnectorCapabilityKey,
		xhandler.SequencerKey,
		xhandler.DMLKey,
		xhandler.FlusherKey,
		xhandler.DataKey,
	}
	seen := map[xhandler.ValueKey]bool{}
	for _, key := range keys {
		if key == "" || seen[key] {
			t.Fatalf("invalid capability key %q", key)
		}
		seen[key] = true
	}
}
