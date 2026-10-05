package compiler

import (
	"context"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type queueHooks struct{ rootEntityHooks }

func (*queueHooks) ObserveQueueAttempt(context.Context, h.QueueAttemptEvent) {}

type wrongAttemptHooks struct{ rootEntityHooks }

func (*wrongAttemptHooks) ObserveQueueAttempt(context.Context, h.PhaseEvent) {}

type controllingQueueHooks struct{ rootEntityHooks }

func (*controllingQueueHooks) ObserveQueueAttempt(context.Context, h.QueueAttemptEvent) error {
	return nil
}

type variadicQueueHooks struct{ rootEntityHooks }

func (*variadicQueueHooks) ObserveQueueAttempt(context.Context, ...h.QueueAttemptEvent) {}
func TestQueueAttemptCompilerCanonicalSignature(t *testing.T) {
	for _, tc := range []struct {
		value                         any
		parent                        string
		component, missingInput, fail bool
	}{{value: queueHooks{}}, {value: wrongAttemptHooks{}, fail: true}, {value: controllingQueueHooks{}, fail: true}, {value: variadicQueueHooks{}, fail: true}, {value: queueHooks{}, parent: reflect.TypeFor[hookParent]().PkgPath() + ".hookParent", fail: true}, {value: queueHooks{}, component: true, fail: true}, {value: queueHooks{}, missingInput: true, fail: true}} {
		typ := reflect.TypeOf(tc.value)
		catalog := typecatalog.NewCatalog()
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
		resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: typ.PkgPath()})
		if err != nil {
			t.Fatal(err)
		}
		input := typ.PkgPath() + ".hookInput"
		if tc.missingInput {
			input = ""
		}
		_, err = (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: typ.Name(), Entity: typ.PkgPath() + ".hookEntity", Input: input, Output: typ.PkgPath() + ".hookOutput", Parent: tc.parent, Component: tc.component})
		if (err != nil) != tc.fail {
			t.Fatalf("%s error=%v", typ.Name(), err)
		}
	}
}
