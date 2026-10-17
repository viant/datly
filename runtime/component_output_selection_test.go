package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/custom"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	xhandler "github.com/viant/xdatly/handler"
)

type publicSelectionInput struct {
	Fields  []string                  `parameter:"Fields,kind=query,in=fields"`
	Invoker xhandler.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required" json:"-"`
}
type publicSelectionOutput struct {
	Count  int
	Unused int
}
type publicSelectionParent publicSelectionOutput
type publicSelectionFilter string

func (f publicSelectionFilter) ExcludePath(_ []string, name string) bool { return name != string(f) }

func TestInjectedXdatlyOutputSelectionDelegation(t *testing.T) {
	for _, mode := range []string{"nested", "error", "mismatch", "no transport"} {
		t.Run(mode, func(t *testing.T) {
			leaf := componentArtifact(t, componentSpec("SelectionLeaf", "GET", "/selection-leaf", nil), reflect.TypeFor[publicSelectionInput](), reflect.TypeFor[publicSelectionOutput]())
			middle := componentArtifact(t, componentSpec("SelectionMiddle", "GET", "/selection-middle", nil), reflect.TypeFor[publicSelectionInput](), reflect.TypeFor[publicSelectionOutput]())
			root := componentArtifact(t, componentSpec("SelectionRoot", "GET", "/selection-root", nil), reflect.TypeFor[publicSelectionInput](), reflect.TypeFor[publicSelectionParent]())
			sibling := componentArtifact(t, componentSpec("SelectionSibling", "GET", "/selection-sibling", nil), reflect.TypeFor[publicSelectionInput](), reflect.TypeFor[publicSelectionParent]())
			leafEntry := &registry.RegisteredComponent{Component: leaf.Component, Input: leaf.Input, OutputType: reflect.TypeFor[publicSelectionOutput](), Handler: custom.NewFunc[publicSelectionInput, publicSelectionOutput](func(ctx context.Context, in *publicSelectionInput) (*publicSelectionOutput, error) {
				selection, ok := in.Invoker.(xhandler.OutputSelection)
				require.True(t, ok, "actual injected invoker must expose optional public seam")
				require.Equal(t, []string{"a,b", ""}, in.Fields, "public typed query provider binds without CSV re-decoding")
				out := &publicSelectionOutput{Unused: 99}
				selection.PublishOutputSelection(ctx, out, publicSelectionFilter("Count"))
				if mode == "error" {
					return out, errors.New("child failed")
				}
				return out, nil
			})}
			middleEntry := &registry.RegisteredComponent{Component: middle.Component, Input: middle.Input, OutputType: reflect.TypeFor[publicSelectionOutput](), Handler: custom.NewFunc[publicSelectionInput, publicSelectionOutput](func(ctx context.Context, in *publicSelectionInput) (*publicSelectionOutput, error) {
				selection := in.Invoker.(xhandler.OutputSelection)
				childCtx := selection.CaptureChildOutputSelection(ctx)
				value, err := in.Invoker.Invoke(childCtx, xhandler.ComponentRequest{Target: xhandler.ComponentTarget{Scope: leaf.Component.Key.Scope, Name: leaf.Component.Key.Name, Method: "GET", Path: "/selection-leaf"}})
				if err != nil {
					return nil, err
				}
				out := value.(*publicSelectionOutput)
				selection.PublishOutputSelection(ctx, out, selection.SelectedOutputFields(childCtx, value))
				return out, nil
			})}
			siblingEntry := &registry.RegisteredComponent{Component: sibling.Component, Input: sibling.Input, OutputType: reflect.TypeFor[publicSelectionParent](), Handler: custom.NewFunc[publicSelectionInput, publicSelectionParent](func(ctx context.Context, in *publicSelectionInput) (*publicSelectionParent, error) {
				out := &publicSelectionParent{}
				in.Invoker.(xhandler.OutputSelection).PublishOutputSelection(ctx, out, publicSelectionFilter("Unused"))
				return out, nil
			})}
			rootEntry := &registry.RegisteredComponent{Component: root.Component, Input: root.Input, OutputType: reflect.TypeFor[publicSelectionParent](), Handler: custom.NewFunc[publicSelectionInput, publicSelectionParent](func(ctx context.Context, in *publicSelectionInput) (*publicSelectionParent, error) {
				selection := in.Invoker.(xhandler.OutputSelection)
				childCtx := selection.CaptureChildOutputSelection(ctx)
				value, err := in.Invoker.Invoke(childCtx, xhandler.ComponentRequest{Target: xhandler.ComponentTarget{Scope: middle.Component.Key.Scope, Name: middle.Component.Key.Name, Method: "GET", Path: "/selection-middle"}})
				if err != nil {
					return nil, err
				}
				filter := selection.SelectedOutputFields(childCtx, value)
				if mode == "no transport" {
					require.Nil(t, filter)
				}
				out := (*publicSelectionParent)(value.(*publicSelectionOutput))
				if mode == "mismatch" {
					require.Nil(t, selection.SelectedOutputFields(childCtx, out), "defined parent output must not match child identity")
				}
				selection.PublishOutputSelection(ctx, out, filter)
				_, err = in.Invoker.Invoke(ctx, xhandler.ComponentRequest{Target: xhandler.ComponentTarget{Scope: sibling.Component.Key.Scope, Name: sibling.Component.Key.Name, Method: "GET", Path: "/selection-sibling"}})
				return out, err
			})}

			rt, err := NewRuntime([]*registry.RegisteredComponent{leafEntry, middleEntry, siblingEntry, rootEntry})
			require.NoError(t, err)
			defer rt.Shutdown(t.Context())
			ctx := context.Background()
			if mode != "no transport" {
				ctx = dexec.CaptureOutputSelection(ctx)
			}
			value, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component), Providers: []locator.Provider{hp.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				if name != "fields" {
					return nil, false, nil
				}
				return xhandler.TypedQueryValue{Value: []string{"a,b", ""}}, true, nil
			})}})
			if mode == "error" {
				require.Error(t, err)
				require.Nil(t, dexec.SelectedOutputFields(ctx, &publicSelectionOutput{}))
				return
			}
			require.NoError(t, err)
			filter := dexec.SelectedOutputFields(ctx, value)
			if mode == "no transport" {
				require.Nil(t, filter)
				return
			}
			require.NotNil(t, filter)
			require.False(t, filter.ExcludePath(nil, "Count"))
			require.True(t, filter.ExcludePath(nil, "Unused"))
		})
	}
}
