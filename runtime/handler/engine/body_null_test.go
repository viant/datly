package engine

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"reflect"
	"testing"
)

type nullBoundRow struct {
	ID   int
	Name string
	Has  *struct{ ID, Name bool } `json:"-" setMarker:"true"`
}
type nullBoundInput struct {
	View    *nullBoundRow
	Current string
	Has     *struct{ View, Current bool } `setMarker:"true"`
}

func TestBodyNullPolicyBoundInputsBeforeDependentRead(t *testing.T) {
	required := true
	bindings := []bindly.BindingSpec{{Path: "View", Location: bindstate.Location{Kind: "body"}, Required: &required, BodyNullPolicy: "empty-record"}, {Path: "Current", Location: bindstate.Location{Kind: "null-dependent"}}}
	contract := testRouteInput(t, reflect.TypeFor[nullBoundInput](), bindings...)
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "explicit-null"}[present], func(t *testing.T) {
			input := &nullBoundInput{Has: &struct{ View, Current bool }{View: present}}
			read, handled := false, false
			dependent := handlerprovider.Named("null-dependent", func(_ context.Context, _ reflect.Type, _ string) (any, bool, error) {
				read = true
				if input.View == nil || input.View.Has == nil || input.View.Has.ID || input.View.Has.Name {
					t.Fatalf("read saw unnormalized root: %+v", input.View)
				}
				return "scoped", true, nil
			})
			_, err := New().Execute(context.Background(), Request{Input: contract, BoundInput: input, Providers: []locator.Provider{dependent}, Handler: rhandler.HandlerFunc(func(_ context.Context, inv rhandler.Invocation) (any, error) { handled = true; return inv.Input, nil })})
			if present {
				if err != nil || !read || !handled || input.View == nil || !input.Has.View {
					t.Fatalf("err=%v read=%v handled=%v input=%+v", err, read, handled, input)
				}
			} else if err == nil || read || handled || input.View != nil {
				t.Fatalf("absent ran lifecycle: err=%v read=%v handled=%v", err, read, handled)
			}
		})
	}
}
func TestBodyNullPolicyBoundNilNeedsRealCompiledMarker(t *testing.T) {
	type markerless struct{ View *nullBoundRow }
	required := true
	for _, policy := range []string{"", "empty-record"} {
		contract := testRouteInput(t, reflect.TypeFor[markerless](), bindly.BindingSpec{Path: "View", Location: bindstate.Location{Kind: "body"}, Required: &required, BodyNullPolicy: policy})
		called := false
		input := &markerless{}
		_, err := New().Execute(context.Background(), Request{Input: contract, BoundInput: input, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })})
		if policy == "" {
			if err != nil || !called || input.View != nil {
				t.Fatalf("strict existing BoundInput changed: %v", err)
			}
		} else if err == nil || called || input.View != nil {
			t.Fatalf("markerless normalized: err=%v called=%v", err, called)
		}
	}
}
func TestBodyNullPolicyBoundPreparedAndOptional(t *testing.T) {
	required := false
	contract := testRouteInput(t, reflect.TypeFor[nullBoundInput](), bindly.BindingSpec{Path: "View", Location: bindstate.Location{Kind: "body"}, Required: &required, BodyNullPolicy: "empty-record"})
	prepared := &nullBoundRow{Name: "prepared"}
	for _, tc := range []struct {
		view    *nullBoundRow
		present bool
	}{{nil, false}, {nil, true}, {prepared, true}} {
		input := &nullBoundInput{View: tc.view, Has: &struct{ View, Current bool }{View: tc.present}}
		if err := normalizeBoundBodyNulls(contract, input); err != nil {
			t.Fatal(err)
		}
		if tc.view != nil && input.View != tc.view {
			t.Fatal("prepared body replaced")
		}
		if tc.view == nil && (input.View != nil) != tc.present {
			t.Fatalf("optional view=%+v present=%v", input.View, tc.present)
		}
	}
}
