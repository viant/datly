package compiler

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type batchInputHooks struct{ rootEntityHooks }

func (*batchInputHooks) AfterQueueInput(context.Context, *hookInput, *hookOutput) error { return nil }

type batchWrongInputHooks struct{ rootEntityHooks }

func (*batchWrongInputHooks) AfterQueueInput(context.Context, *hookParent, *hookOutput) error {
	return nil
}

type batchWrongOutputHooks struct{ rootEntityHooks }

func (*batchWrongOutputHooks) AfterQueueInput(context.Context, *hookInput, *hookEntity) error {
	return nil
}

type batchVariadicHooks struct{ rootEntityHooks }

func (*batchVariadicHooks) AfterQueueInput(context.Context, *hookInput, ...*hookOutput) error {
	return nil
}

type batchWrongResultHooks struct{ rootEntityHooks }

func (*batchWrongResultHooks) AfterQueueInput(context.Context, *hookInput, *hookOutput) bool {
	return true
}

type batchPromotedHooks struct{ batchInputHooks }

func TestEntityHookCompilerAfterQueueInputContracts(t *testing.T) {
	location := reflect.TypeFor[hookEntity]().PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[batchInputHooks](), reflect.TypeFor[batchWrongInputHooks](), reflect.TypeFor[batchWrongOutputHooks](), reflect.TypeFor[batchVariadicHooks](), reflect.TypeFor[batchWrongResultHooks](), reflect.TypeFor[batchPromotedHooks]()} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, hook, parent, input string
		component, invalid        bool
	}{
		{name: "canonical", hook: "batchInputHooks"},
		{name: "promoted", hook: "batchPromotedHooks"},
		{name: "wrong input", hook: "batchWrongInputHooks", invalid: true},
		{name: "wrong output", hook: "batchWrongOutputHooks", invalid: true},
		{name: "variadic", hook: "batchVariadicHooks", invalid: true},
		{name: "wrong result", hook: "batchWrongResultHooks", invalid: true},
		{name: "descendant", hook: "batchInputHooks", parent: location + ".hookParent", invalid: true},
		{name: "auxiliary component", hook: "batchInputHooks", component: true, invalid: true},
		{name: "foreign same input name", hook: "batchInputHooks", input: "example.com/foreign.hookInput", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := location + ".hookInput"
			if tc.input != "" {
				input = tc.input
			}
			_, err := (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: tc.hook, Entity: location + ".hookEntity", Parent: tc.parent, Input: input, Output: location + ".hookOutput", Component: tc.component})
			if (err != nil) != tc.invalid {
				t.Fatalf("Compile error=%v, invalid=%v", err, tc.invalid)
			}
		})
	}
}
