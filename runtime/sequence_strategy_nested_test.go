package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type sequenceStrategyRuntimeInput struct{}
type sequenceStrategyRuntimeOutput struct{}

func TestRuntimeOmittedRootNativeDefaultSQLite(t *testing.T) {
	h := sqlite.New(t)
	assertRuntimeSequenceStrategy(t, dml.Source{DB: h.DB}, "reservation", "transient")
}

func assertRuntimeSequenceStrategy(t *testing.T, source dml.Source, matching, differing string) {
	t.Helper()
	for _, testCase := range []struct {
		name     string
		strategy string
		wantErr  bool
	}{
		{name: "matching native default", strategy: matching},
		{name: "differing policy", strategy: differing, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parent := componentSpec("SequenceParent", http.MethodPost, "/sequence-parent", nil)
			child := componentSpec("SequenceChild", http.MethodPost, "/sequence-child", nil)
			child.Settings = &spec.Settings{SequenceStrategy: testCase.strategy}
			parentArtifact := componentArtifact(t, parent, reflect.TypeOf(sequenceStrategyRuntimeInput{}), reflect.TypeOf(sequenceStrategyRuntimeOutput{}))
			childArtifact := componentArtifact(t, child, reflect.TypeOf(sequenceStrategyRuntimeInput{}), reflect.TypeOf(sequenceStrategyRuntimeOutput{}))
			childTarget := dexec.ComponentTarget{Component: child.Key, Route: spec.RouteRef{Method: http.MethodPost, Path: "/sequence-child"}}

			childHandler := customhandler.New[sequenceStrategyRuntimeInput, sequenceStrategyRuntimeOutput](
				xhandler.ContractFunc[sequenceStrategyRuntimeInput, sequenceStrategyRuntimeOutput](func(ctx context.Context, session xhandler.Session, _ *sequenceStrategyRuntimeInput, _ *sequenceStrategyRuntimeOutput) error {
					return requireRuntimeData(ctx, session)
				}),
			)
			parentHandler := customhandler.New[sequenceStrategyRuntimeInput, sequenceStrategyRuntimeOutput](
				xhandler.ContractFunc[sequenceStrategyRuntimeInput, sequenceStrategyRuntimeOutput](func(ctx context.Context, session xhandler.Session, _ *sequenceStrategyRuntimeInput, _ *sequenceStrategyRuntimeOutput) error {
					if err := requireRuntimeData(ctx, session); err != nil {
						return err
					}
					value, found, err := session.Binder().Lookup(ctx, dexec.ComponentInvokerKey)
					if err != nil || !found {
						return fmt.Errorf("component invoker lookup: found=%v err=%w", found, err)
					}
					_, err = value.(dexec.ComponentInvoker).InvokeComponent(ctx, dexec.ComponentRequest{Target: childTarget})
					return err
				}),
			)
			runtime, err := NewRuntime([]*registry.RegisteredComponent{
				{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeOf(sequenceStrategyRuntimeOutput{}), Handler: parentHandler, DataSource: source},
				{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(sequenceStrategyRuntimeOutput{}), Handler: childHandler, DataSource: source},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, invokeErr := runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{
				Target: dexec.ComponentTarget{Component: parent.Key, Route: spec.RouteRef{Method: http.MethodPost, Path: "/sequence-parent"}},
			})
			if testCase.wantErr {
				if !errors.Is(invokeErr, handlerengine.ErrSequenceStrategyConflict) {
					t.Fatalf("differing child did not conflict: %v", invokeErr)
				}
				return
			}
			if invokeErr != nil {
				t.Fatalf("matching native default was rejected: %v", invokeErr)
			}
		})
	}
}

func requireRuntimeData(ctx context.Context, session xhandler.Session) error {
	_, found, err := session.Binder().Lookup(ctx, xhandler.DataKey)
	if err != nil || !found {
		return fmt.Errorf("Data lookup: found=%v err=%w", found, err)
	}
	return nil
}
