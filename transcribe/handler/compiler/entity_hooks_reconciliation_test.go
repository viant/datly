package compiler

import (
	"context"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"testing"
)

type finiteHooks struct{ rootEntityHooks }

func (*finiteHooks) ReconcileInput(context.Context, *hookInput, *hookOutput, writer.ReconciliationContext) (writer.ReconciliationPlan, error) {
	return writer.ReconciliationPlan{}, nil
}

type finiteWrongInputHooks struct{ rootEntityHooks }

func (*finiteWrongInputHooks) ReconcileInput(context.Context, *hookParent, *hookOutput, writer.ReconciliationContext) (writer.ReconciliationPlan, error) {
	return writer.ReconciliationPlan{}, nil
}

type finiteWrongContextHooks struct{ rootEntityHooks }

func (*finiteWrongContextHooks) ReconcileInput(context.Context, *hookInput, *hookOutput, *writer.ReconciliationContext) (writer.ReconciliationPlan, error) {
	return writer.ReconciliationPlan{}, nil
}

type finiteWrongResultHooks struct{ rootEntityHooks }

func (*finiteWrongResultHooks) ReconcileInput(context.Context, *hookInput, *hookOutput, writer.ReconciliationContext) error {
	return nil
}
func TestEntityHookCompilerFiniteReconciliationContracts(t *testing.T) {
	location := reflect.TypeFor[hookEntity]().PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[finiteHooks](), reflect.TypeFor[finiteWrongInputHooks](), reflect.TypeFor[finiteWrongContextHooks](), reflect.TypeFor[finiteWrongResultHooks]()} {
		if e := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); e != nil {
			t.Fatal(e)
		}
	}
	resolver, e := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name, hook, parent          string
		allowed, component, invalid bool
	}{
		{"canonical", "finiteHooks", "", true, false, false},
		{"absent opt-in", "finiteHooks", "", false, false, true},
		{"wrong input", "finiteWrongInputHooks", "", true, false, true},
		{"wrong context", "finiteWrongContextHooks", "", true, false, true},
		{"wrong result", "finiteWrongResultHooks", "", true, false, true},
		{"descendant", "finiteHooks", location + ".hookParent", true, false, true},
		{"auxiliary", "finiteHooks", "", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, e := (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: tc.hook, Entity: location + ".hookEntity", Parent: tc.parent, Input: location + ".hookInput", Output: location + ".hookOutput", ReconciliationAllowed: tc.allowed, Component: tc.component})
			if (e != nil) != tc.invalid {
				t.Fatalf("error=%v invalid=%v", e, tc.invalid)
			}
		})
	}
}
