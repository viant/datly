package compiler

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
)

type auxiliaryInput struct{}
type auxiliaryHooks struct{}

func (*auxiliaryHooks) Init(context.Context, *auxiliaryInput, h.LifecycleContext[auxiliaryInput, h.NoParent, hookOutput]) error {
	return nil
}
func (*auxiliaryHooks) Validate(context.Context, *auxiliaryInput, h.LifecycleContext[auxiliaryInput, h.NoParent, hookOutput]) error {
	return nil
}

type auxiliarySequenceHooks struct{ auxiliaryHooks }

func (*auxiliarySequenceHooks) AfterSequence(context.Context, *auxiliaryInput, h.LifecycleContext[auxiliaryInput, h.NoParent, hookOutput]) error {
	return nil
}

type auxiliaryQueueHooks struct{ auxiliaryHooks }

func (*auxiliaryQueueHooks) AfterQueue(context.Context, *auxiliaryInput, h.LifecycleContext[auxiliaryInput, h.NoParent, hookOutput]) error {
	return nil
}

type auxiliaryRecoveryHooks struct{ auxiliaryHooks }

func (*auxiliaryRecoveryHooks) Recover() {}

func TestAuxiliaryHookCompilerRejectsRowPhases(t *testing.T) {
	location := reflect.TypeOf(auxiliaryInput{}).PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeOf(auxiliaryHooks{}), reflect.TypeOf(auxiliarySequenceHooks{}), reflect.TypeOf(auxiliaryQueueHooks{}), reflect.TypeOf(auxiliaryRecoveryHooks{}), reflect.TypeOf(rootEntityHooks{})} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name    string
		invalid bool
	}{{"auxiliaryHooks", false}, {"auxiliarySequenceHooks", true}, {"auxiliaryQueueHooks", true}, {"auxiliaryRecoveryHooks", true}, {"rootEntityHooks", true}} {
		t.Run(item.name, func(t *testing.T) {
			_, err := (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: item.name, Entity: location + ".auxiliaryInput", Input: location + ".auxiliaryInput", Output: location + ".hookOutput", Component: true})
			if (err != nil) != item.invalid {
				t.Fatalf("unexpected contract result: %v", err)
			}
		})
	}
}
