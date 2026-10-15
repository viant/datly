package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
)

type markerlessSeedInput struct {
	ID           int
	Auth         *boundSeedIdentity
	Clock        time.Time
	Current      *boundSeedIdentity
	Derived      *boundSeedIdentity
	ScopedAuth   *boundSeedIdentity
	UncachedAuth *boundSeedIdentity
	Unknown      *boundSeedIdentity
}

func TestMarkerlessBoundSeedPreservesIdentityBeforeDependentReads(t *testing.T) {
	// A nonnil pointer to an otherwise zero object is still a supplied identity.
	seed, fresh := &boundSeedIdentity{}, &boundSeedIdentity{ID: 2}
	stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &markerlessSeedInput{Auth: seed, Clock: stamp, Current: seed, Derived: fresh, ScopedAuth: seed, UncachedAuth: seed, Unknown: seed}
	no := false
	bindings := []bindly.BindingSpec{
		{Path: "Auth", Location: bindstate.Location{Kind: "component", In: "GET:/auth"}},
		{Path: "Clock", Location: bindstate.Location{Kind: "generator", In: "clock"}},
		{Path: "Current", Location: bindstate.Location{Kind: "view", In: "current"}},
		{Path: "Derived", Location: bindstate.Location{Kind: "param", In: "Auth"}},
		{Path: "ScopedAuth", Scope: "private", Location: bindstate.Location{Kind: "component", In: "GET:/scoped"}},
		{Path: "UncachedAuth", Cacheable: &no, Location: bindstate.Location{Kind: "component", In: "GET:/uncached"}},
		{Path: "Unknown", Location: bindstate.Location{Kind: "fixture", In: "unknown"}},
	}
	calls := map[string]int{}
	value, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[markerlessSeedInput](), bindings...), BoundInput: input,
		Components: handlerprovider.Named("component", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
			calls[name]++
			return fresh, true, nil
		}),
		Providers: []locator.Provider{
			handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) {
				t.Fatal("canonical clock provider invoked")
				return nil, false, nil
			}),
			handlerprovider.Named("view", func(context.Context, reflect.Type, string) (any, bool, error) {
				if input.Auth != seed || input.Clock != stamp {
					t.Fatal("Current lost original seeds")
				}
				calls["current"]++
				return fresh, true, nil
			}),
			handlerprovider.Named("fixture", func(context.Context, reflect.Type, string) (any, bool, error) {
				calls["unknown"]++
				return fresh, true, nil
			}),
		}, Handler: rhandler.HandlerFunc(boundSeedReturn)})
	if err != nil {
		t.Fatal(err)
	}
	if value != input || input.Auth != seed || input.Clock != stamp || input.Current != fresh || input.Derived != seed || input.ScopedAuth != fresh || input.UncachedAuth != fresh || input.Unknown != fresh || calls["GET:/auth"] != 0 || calls["GET:/scoped"] != 1 || calls["GET:/uncached"] != 1 || calls["current"] != 1 || calls["unknown"] != 1 {
		t.Fatalf("input%+v calls%v", input, calls)
	}
}

func TestMarkerlessBoundSeedNilTypedNilAndZeroStillResolve(t *testing.T) {
	type input struct{ Value any }
	var typedNil *boundSeedIdentity
	for _, tc := range []struct {
		name  string
		value any
	}{{"nil", nil}, {"typed nil", typedNil}, {"zero int", 0}, {"false", false}, {"empty string", ""}} {
		for _, kind := range []string{"component", "generator"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				fresh := &boundSeedIdentity{ID: 2}
				supplied := &input{Value: tc.value}
				calls := 0
				location := "value"
				if kind == "component" {
					location = "GET:/value"
				}
				binding := bindly.BindingSpec{Path: "Value", Location: bindstate.Location{Kind: kind, In: location}}
				provider := handlerprovider.Named(kind, func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil })
				request := Request{Input: testRouteInput(t, reflect.TypeFor[input](), binding), BoundInput: supplied, Handler: rhandler.HandlerFunc(boundSeedReturn)}
				if kind == "component" {
					request.Components = provider
				} else {
					request.Providers = []locator.Provider{provider}
				}
				value, err := New().Execute(t.Context(), request)
				if err != nil || value != supplied || supplied.Value != fresh || calls != 1 {
					t.Fatalf("nil/zero seed suppressed normal resolution: %+v calls%d err%v", supplied, calls, err)
				}
			})
		}
	}
	// Ordinary zero-valued fields (not just interface-held zeros) also bind.
	type scalar struct {
		Clock   time.Time
		Count   int
		Enabled bool
		Text    string
	}
	supplied := &scalar{}
	calls := 0
	bindings := []bindly.BindingSpec{}
	for _, name := range []string{"Clock", "Count", "Enabled", "Text"} {
		bindings = append(bindings, bindly.BindingSpec{Path: name, Location: bindstate.Location{Kind: "generator", In: name}})
	}
	stamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[scalar](), bindings...), BoundInput: supplied, Providers: []locator.Provider{handlerprovider.Named("generator", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		calls++
		switch name {
		case "Clock":
			return stamp, true, nil
		case "Count":
			return 3, true, nil
		case "Enabled":
			return true, true, nil
		default:
			return "resolved", true, nil
		}
	})}, Handler: rhandler.HandlerFunc(boundSeedReturn)})
	if err != nil || calls != 4 || supplied.Clock != stamp || supplied.Count != 3 || !supplied.Enabled || supplied.Text != "resolved" {
		t.Fatalf("zero scalar did not resolve: %+v calls%d err%v", supplied, calls, err)
	}
}

func TestMarkerlessBoundSeedRequiredResolverErrorRetainsBindingOwner(t *testing.T) {
	type input struct{ Auth *boundSeedIdentity }
	sentinel := errors.New("required resolver cause")
	for _, kind := range []string{"component", "generator"} {
		for _, required := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/optional", true: "/required"}[required], func(t *testing.T) {
				location := "auth"
				if kind == "component" {
					location = "GET:/auth"
				}
				binding := bindly.BindingSpec{Path: "Auth", Name: "ownedAuth", Required: &required, ErrorCode: 403, ErrorMessage: "public binding error", Location: bindstate.Location{Kind: kind, In: location}}
				calls := 0
				entered := false
				provider := handlerprovider.Named(kind, func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return nil, false, sentinel })
				req := Request{Input: testRouteInput(t, reflect.TypeFor[input](), binding), BoundInput: &input{}, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { entered = true; return nil, nil })}
				if kind == "component" {
					req.Components = provider
				} else {
					req.Providers = []locator.Provider{provider}
				}
				_, err := New().Execute(t.Context(), req)
				var owned *bindly.BindingError
				if entered || calls != 1 || !errors.As(err, &owned) || !errors.Is(err, sentinel) || owned.Path != "Auth" || owned.Name != "ownedAuth" || owned.Kind != kind || owned.In != location || owned.Code != 403 || owned.Message != "public binding error" {
					t.Fatalf("binding owner lost: calls%d entered%t error%#v", calls, entered, err)
				}
			})
		}
	}
}

func TestMarkerlessBoundSeedPartialAndExplicitMarkerContractsDoNotInfer(t *testing.T) {
	type partial struct {
		Auth  *boundSeedIdentity
		Clock time.Time
		Has   *struct{ Clock bool } `setMarker:"true"`
	}
	seed, fresh := &boundSeedIdentity{ID: 1}, &boundSeedIdentity{ID: 2}
	stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, marked := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil partial Has", true: "true partial Has"}[marked], func(t *testing.T) {
			in := &partial{Auth: seed, Clock: stamp}
			if marked {
				in.Has = &struct{ Clock bool }{Clock: true}
			}
			calls := 0
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[partial](), boundSeedBindings()[3:5]...), BoundInput: in, Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil }), Handler: rhandler.HandlerFunc(boundSeedReturn)})
			if err != nil || calls != 1 || in.Auth != fresh || (marked && in.Clock != stamp) || (!marked && in.Clock == stamp) {
				t.Fatalf("partial marker inference %+v calls%d err%v", in, calls, err)
			}
		})
	}
	type explicit struct {
		Auth   *boundSeedIdentity
		Clock  time.Time
		Marker *struct{ Auth bool }
	}
	for _, marked := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil explicit marker", true: "true explicit marker"}[marked], func(t *testing.T) {
			in := &explicit{Auth: seed, Clock: stamp}
			if marked {
				in.Marker = &struct{ Auth bool }{Auth: true}
			}
			bindings := boundSeedBindings()[3:5]
			bindings[0].MarkerField = "Marker.Auth"
			calls := 0
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[explicit](), bindings...), BoundInput: in, Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil }), Handler: rhandler.HandlerFunc(boundSeedReturn)})
			want := fresh
			if marked {
				want = seed
			}
			if err != nil || in.Auth != want || in.Clock == stamp || calls != map[bool]int{false: 1, true: 0}[marked] {
				t.Fatalf("explicit marker inference %+v calls%d err%v", in, calls, err)
			}
		})
	}
}

func TestMarkerlessBoundSeedRequestOnlyRetryCloneRefreshes(t *testing.T) {
	seed, fresh := &boundSeedIdentity{ID: 1}, &boundSeedIdentity{ID: 2}
	stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &markerlessSeedInput{ID: 7, Auth: seed, Clock: stamp}
	bindings := []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "query", In: "id"}}, boundSeedBindings()[3], boundSeedBindings()[4]}
	contract := testRouteInput(t, reflect.TypeFor[markerlessSeedInput](), bindings...)
	value, err := captureMutationInput(contract, input)
	if err != nil {
		t.Fatal(err)
	}
	clone := value.(*markerlessSeedInput)
	if clone == input || clone.ID != 7 || clone.Auth != nil || !clone.Clock.IsZero() || input.Auth != seed || input.Clock != stamp {
		t.Fatal("request-only capture retained supplements or mutated original")
	}
	calls := 0
	_, err = New().Execute(t.Context(), Request{Input: contract, BoundInput: clone, Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil }), Handler: rhandler.HandlerFunc(boundSeedReturn)})
	if err != nil || calls != 1 || clone.Auth != fresh || clone.Clock.IsZero() || clone.Clock == stamp || input.Auth != seed || input.Clock != stamp {
		t.Fatalf("retry clone not refreshed %+v calls%d err%v", clone, calls, err)
	}
}

func TestMarkerlessBoundSeedPromotedPartialMarkerStillRefreshes(t *testing.T) {
	type Marker struct {
		Has *struct{ Clock bool } `setMarker:"true"`
	}
	type input struct {
		Marker
		Auth  *boundSeedIdentity
		Clock time.Time
	}
	seed, fresh := &boundSeedIdentity{ID: 1}, &boundSeedIdentity{ID: 2}
	in := &input{Auth: seed, Clock: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)}
	calls := 0
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[input](), boundSeedBindings()[3:5]...), BoundInput: in, Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil }), Handler: rhandler.HandlerFunc(boundSeedReturn)})
	if err != nil || calls != 1 || in.Auth != fresh || in.Clock.Year() == 1999 {
		t.Fatalf("promoted partial marker inferred values: %+v calls%d err%v", in, calls, err)
	}
}
func TestMarkerlessBoundSeedNilAncestorIsNotAllocatedBySeedInspection(t *testing.T) {
	type nested struct{ Auth *boundSeedIdentity }
	type input struct{ Nested *nested }
	in := &input{}
	contract := testRouteInput(t, reflect.TypeFor[input](), bindly.BindingSpec{Path: "Nested.Auth", Location: bindstate.Location{Kind: "component", In: "GET:/auth"}})
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := boundSupplementalPlan(injector, contract, in)
	if err != nil || plan == nil || in.Nested != nil {
		t.Fatalf("missing ancestor allocated/treated as seed: %+v plan%v err%v", in, plan, err)
	}
}
