package compiler

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
)

type aggregateHooks struct{}

func (*aggregateHooks) Init(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}
func (*aggregateHooks) ValidateInput(context.Context, *hookInput, *hookOutput, h.ValidationReport) error {
	return nil
}

type overlappingAggregateHooks struct{ rootEntityHooks }

func (*overlappingAggregateHooks) ValidateInput(context.Context, *hookInput, *hookOutput, h.ValidationReport) error {
	return nil
}

type wrongAggregateHooks struct{ aggregateHooks }

func (*wrongAggregateHooks) ValidateInput(context.Context, *hookEntity, *hookOutput, h.ValidationReport) error {
	return nil
}

func TestAggregateValidationCanonicalContract(t *testing.T) {
	for _, tc := range []struct {
		name            string
		value           any
		parent          string
		component, fail bool
	}{
		{"root", aggregateHooks{}, "", false, false},
		{"overlap", overlappingAggregateHooks{}, "", false, true},
		{"wrong input", wrongAggregateHooks{}, "", false, true},
		{"child", aggregateHooks{}, reflect.TypeFor[hookParent]().PkgPath() + ".hookParent", false, true},
		{"composing root", aggregateHooks{}, "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeOf(tc.value)
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: typ.PkgPath()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: typ.Name(), Entity: typ.PkgPath() + ".hookEntity", Input: typ.PkgPath() + ".hookInput", Output: typ.PkgPath() + ".hookOutput", Parent: tc.parent, Component: tc.component})
			if (err != nil) != tc.fail {
				t.Fatalf("compile error=%v, expected failure=%v", err, tc.fail)
			}
		})
	}
}
