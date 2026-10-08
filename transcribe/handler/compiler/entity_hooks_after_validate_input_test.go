package compiler

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type preparationInputHooks struct{ rootEntityHooks }

func (*preparationInputHooks) AfterValidateInput(context.Context, *hookInput, *hookOutput) error {
	return nil
}

type preparationWrongInputHooks struct{ rootEntityHooks }

func (*preparationWrongInputHooks) AfterValidateInput(context.Context, *hookParent, *hookOutput) error {
	return nil
}

type preparationWrongOutputHooks struct{ rootEntityHooks }

func (*preparationWrongOutputHooks) AfterValidateInput(context.Context, *hookInput, *hookEntity) error {
	return nil
}

type preparationVariadicHooks struct{ rootEntityHooks }

func (*preparationVariadicHooks) AfterValidateInput(context.Context, *hookInput, ...*hookOutput) error {
	return nil
}

type preparationWrongResultHooks struct{ rootEntityHooks }

func (*preparationWrongResultHooks) AfterValidateInput(context.Context, *hookInput, *hookOutput) bool {
	return true
}

type preparationPromotedHooks struct{ preparationInputHooks }

func TestEntityHookCompilerAfterValidateInputContracts(t *testing.T) {
	location := reflect.TypeFor[hookEntity]().PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[preparationInputHooks](), reflect.TypeFor[preparationWrongInputHooks](), reflect.TypeFor[preparationWrongOutputHooks](), reflect.TypeFor[preparationVariadicHooks](), reflect.TypeFor[preparationWrongResultHooks](), reflect.TypeFor[preparationPromotedHooks]()} {
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
		{name: "canonical", hook: "preparationInputHooks"},
		{name: "promoted", hook: "preparationPromotedHooks"},
		{name: "wrong input", hook: "preparationWrongInputHooks", invalid: true},
		{name: "wrong output", hook: "preparationWrongOutputHooks", invalid: true},
		{name: "variadic", hook: "preparationVariadicHooks", invalid: true},
		{name: "wrong result", hook: "preparationWrongResultHooks", invalid: true},
		{name: "descendant", hook: "preparationInputHooks", parent: location + ".hookParent", invalid: true},
		{name: "auxiliary component", hook: "preparationInputHooks", component: true, invalid: true},
		{name: "foreign same input name", hook: "preparationInputHooks", input: "example.com/foreign.hookInput", invalid: true},
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
