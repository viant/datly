package compiler

import (
	"context"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type scalarEntityHooks int

func (*scalarEntityHooks) Init(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}
func (*scalarEntityHooks) Validate(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}

type sliceEntityHooks []string

func (*sliceEntityHooks) Init(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}
func (*sliceEntityHooks) Validate(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}

type injectedEntityHooks struct {
	Input any `bind:"kind=input"`
}

func (*injectedEntityHooks) Init(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}
func (*injectedEntityHooks) Validate(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}

func TestEntityHookContractsAcrossUnderlyingKinds(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	types := []reflect.Type{reflect.TypeOf(scalarEntityHooks(0)), reflect.TypeOf(sliceEntityHooks{}), reflect.TypeOf(rootEntityHooks{}), reflect.TypeOf(injectedEntityHooks{})}
	for _, typ := range types {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	location := reflect.TypeOf(hookEntity{}).PkgPath()
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if err != nil {
		t.Fatal(err)
	}
	compiler := EntityHookCompiler{Types: resolver}
	for _, typ := range types {
		t.Run(typ.Name(), func(t *testing.T) {
			_, err := compiler.Compile(EntityHookRequest{Hook: typ.Name(), Entity: location + ".hookEntity", Output: location + ".hookOutput"})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
