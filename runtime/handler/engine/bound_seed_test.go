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
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type boundSeedIdentity struct{ ID int }
type boundSeedInput struct {
	Jwt          *string
	Enabled      bool
	Zero         int
	Auth         *boundSeedIdentity
	Clock        time.Time
	Current      *boundSeedIdentity
	Derived      *boundSeedIdentity
	ScopedAuth   *boundSeedIdentity
	UncachedAuth *boundSeedIdentity
	Unknown      *boundSeedIdentity
	Parent       *boundSeedInput
	Has          *boundSeedHas `setMarker:"true" json:"-"`
}
type boundSeedHas struct {
	Jwt, Enabled, Zero, Auth, Clock, Current, Derived, ScopedAuth, UncachedAuth, Unknown, Parent bool
}

func boundSeedBindings() []bindly.BindingSpec {
	yes, no := true, false
	return []bindly.BindingSpec{
		{Path: "Jwt", Required: &yes, ErrorCode: 401, Location: bindstate.Location{Kind: "header", In: "Authorization"}},
		{Path: "Enabled", Location: bindstate.Location{Kind: "query", In: "enabled"}},
		{Path: "Zero", Location: bindstate.Location{Kind: "query", In: "zero"}},
		{Path: "Auth", Location: bindstate.Location{Kind: "component", In: "GET:/auth"}},
		{Path: "Clock", Location: bindstate.Location{Kind: "generator", In: "current_time"}},
		{Path: "Current", Location: bindstate.Location{Kind: "view", In: "current"}},
		{Path: "Derived", Location: bindstate.Location{Kind: "param", In: "Auth"}},
		{Path: "ScopedAuth", Scope: "private", Location: bindstate.Location{Kind: "component", In: "GET:/scoped"}},
		{Path: "UncachedAuth", Cacheable: &no, Location: bindstate.Location{Kind: "component", In: "GET:/uncached"}},
		{Path: "Unknown", Location: bindstate.Location{Kind: "fixture", In: "unknown"}},
		{Path: "Parent", Location: bindstate.Location{Kind: "input"}},
	}
}
func boundSeedReturn(_ context.Context, v rhandler.Invocation) (any, error) { return v.Input, nil }

func TestBoundSeedPreservesCanonicalAndRefreshesOtherKinds(t *testing.T) {
	jwt := "verified"
	seed, fresh := &boundSeedIdentity{1}, &boundSeedIdentity{2}
	stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &boundSeedInput{Jwt: &jwt, Auth: seed, Clock: stamp, Current: seed, Derived: fresh, ScopedAuth: seed,
		UncachedAuth: seed, Unknown: seed, Parent: &boundSeedInput{}, Has: &boundSeedHas{Jwt: true, Enabled: true, Zero: true, Auth: true,
			Clock: true, Current: true, Derived: true, ScopedAuth: true, UncachedAuth: true, Unknown: true, Parent: true}}
	calls := map[string]int{}
	result, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[boundSeedInput](), boundSeedBindings()...), BoundInput: input,
		Components: handlerprovider.Named("component", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
			calls[name]++
			return fresh, true, nil
		}),
		Providers: []locator.Provider{
			handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) {
				calls["clock"]++
				return stamp.Add(time.Hour), true, nil
			}),
			handlerprovider.Named("view", func(context.Context, reflect.Type, string) (any, bool, error) {
				if input.Auth != seed || input.Clock != stamp || input.Jwt != &jwt {
					t.Fatal("dependent read lost canonical seed")
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
	if result != input || input.Auth != seed || input.Clock != stamp || input.Current != fresh || input.Derived != seed || input.ScopedAuth != fresh ||
		input.UncachedAuth != fresh || input.Unknown != fresh || input.Parent != input || calls["GET:/auth"] != 0 || calls["clock"] != 0 ||
		calls["current"] != 1 || calls["GET:/scoped"] != 1 || calls["GET:/uncached"] != 1 || calls["unknown"] != 1 {
		t.Fatalf("seed/refresh authority differs: input=%+v calls=%v", input, calls)
	}
}

func TestBoundSeedAbsentMarkersAndTransportStillBind(t *testing.T) {
	for _, mode := range []string{"nil Has", "false markers", "transport", "markerless"} {
		t.Run(mode, func(t *testing.T) {
			seed, fresh := &boundSeedIdentity{1}, &boundSeedIdentity{2}
			stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
			input := &boundSeedInput{Auth: seed, Clock: stamp}
			if mode == "false markers" {
				input.Has = &boundSeedHas{}
			}
			request := Request{Input: testRouteInput(t, reflect.TypeFor[boundSeedInput](), boundSeedBindings()[3:5]...), BoundInput: input, Handler: rhandler.HandlerFunc(boundSeedReturn)}
			if mode == "transport" {
				request.BoundInput = nil
			}
			type markerless struct {
				Auth  *boundSeedIdentity
				Clock time.Time
			}
			if mode == "markerless" {
				request.Input = testRouteInput(t, reflect.TypeFor[markerless](), boundSeedBindings()[3:5]...)
				request.BoundInput = &markerless{Auth: seed, Clock: stamp}
			}
			calls := 0
			request.Components = handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return fresh, true, nil })
			value, err := New().Execute(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			var actualAuth *boundSeedIdentity
			var actualClock time.Time
			if mode == "markerless" {
				actualAuth = value.(*markerless).Auth
				actualClock = value.(*markerless).Clock
			} else {
				actualAuth = value.(*boundSeedInput).Auth
				actualClock = value.(*boundSeedInput).Clock
			}
			if actualAuth != fresh || actualClock.IsZero() || actualClock == stamp || calls != 1 {
				t.Fatalf("absent seed suppressed binding: %v %v calls=%d", actualAuth, actualClock, calls)
			}
		})
	}
}

func TestBoundSeedRequiredAndOptionalNil(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			input := &boundSeedInput{Has: &boundSeedHas{Auth: true}}
			binding := boundSeedBindings()[3]
			binding.Required, binding.Name, binding.ErrorCode, binding.ErrorMessage = &required, "verifiedAuth", 403, "auth required"
			called, resolved := false, false
			_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[boundSeedInput](), binding), BoundInput: input,
				Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) {
					resolved = true
					return &boundSeedIdentity{2}, true, nil
				}),
				Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })})
			if resolved || input.Auth != nil || called == required || (err != nil) != required {
				t.Fatalf("required=%v called=%v resolved=%v err=%v", required, called, resolved, err)
			}
			if required {
				var public *bindly.BindingError
				if !errors.As(err, &public) || public.Path != "Auth" || public.Name != binding.Name || public.Kind != "component" || public.In != "GET:/auth" || public.Code != 403 || public.Message != "auth required" {
					t.Fatalf("binding metadata lost: %v", err)
				}
			}
		})
	}
}

func TestBoundSeedStillChecksRequiredRequest(t *testing.T) {
	input := &boundSeedInput{Auth: &boundSeedIdentity{1}, Clock: time.Now(), Has: &boundSeedHas{Auth: true, Clock: true}}
	called := false
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[boundSeedInput](), boundSeedBindings()[:5]...), BoundInput: input,
		Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })})
	var public *bindly.BindingError
	if called || !errors.As(err, &public) || public.Path != "Jwt" || public.Code != 401 {
		t.Fatalf("missing required request admitted: called=%v err=%v", called, err)
	}
}

func TestBoundSeedRequiredGeneratorNil(t *testing.T) {
	type input struct {
		Clock *time.Time
		Has   *struct{ Clock bool } `setMarker:"true"`
	}
	yes := true
	binding := bindly.BindingSpec{Path: "Clock", Required: &yes, Location: bindstate.Location{Kind: "generator", In: "current_time"}}
	called, generated := false, false
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[input](), binding),
		BoundInput: &input{Has: &struct{ Clock bool }{Clock: true}},
		Providers: []locator.Provider{handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) {
			generated = true
			return time.Now(), true, nil
		})}, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })})
	var public *bindly.BindingError
	if called || generated || !errors.As(err, &public) || public.Path != "Clock" || public.Kind != "generator" {
		t.Fatalf("required generator nil admitted/resolved: called=%v generated=%v err=%v", called, generated, err)
	}
}

type boundSeedUnexpectedTransform struct{ calls *int }

func (x boundSeedUnexpectedTransform) Transform(context.Context, locator.Resolver, any) (any, error) {
	*x.calls++
	return nil, errors.New("canonical seed transformed again")
}

func TestBoundSeedCanonicalValueDoesNotTransformAgain(t *testing.T) {
	seed := &boundSeedIdentity{1}
	input := &boundSeedInput{Auth: seed, Has: &boundSeedHas{Auth: true}}
	calls := 0
	binding := boundSeedBindings()[3]
	binding.SourceType, binding.Transformer = reflect.TypeFor[string](), boundSeedUnexpectedTransform{calls: &calls}
	_, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[boundSeedInput](), binding), BoundInput: input,
		Handler: rhandler.HandlerFunc(boundSeedReturn)})
	if err != nil || calls != 0 || input.Auth != seed {
		t.Fatalf("canonical value converted/transformed: calls=%d auth=%v err=%v", calls, input.Auth, err)
	}
}

func TestBoundSeedCaptureClearsEverySupplementalMarker(t *testing.T) {
	input := &boundSeedInput{Has: &boundSeedHas{Jwt: true, Enabled: true, Zero: true, Auth: true, Clock: true,
		Current: true, Derived: true, ScopedAuth: true, UncachedAuth: true, Unknown: true, Parent: true}}
	contract := testRouteInput(t, reflect.TypeFor[boundSeedInput](), boundSeedBindings()...)
	value, err := captureMutationInput(contract, input)
	if err != nil {
		t.Fatal(err)
	}
	clone := value.(*boundSeedInput)
	if clone.Has == input.Has || *clone.Has != (boundSeedHas{Jwt: true, Enabled: true, Zero: true}) || !input.Has.Current || !input.Has.Parent {
		t.Fatalf("request-only clone has stale supplemental markers or mutated caller: clone=%+v caller=%+v", clone.Has, input.Has)
	}
}

func TestBoundSeedRetryClearsSupplementalMarkersAndRefreshes(t *testing.T) {
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE bound_seed_retry(id INTEGER PRIMARY KEY)", "CREATE TRIGGER ignored BEFORE INSERT ON bound_seed_retry BEGIN SELECT RAISE(IGNORE); END"); err != nil {
		t.Fatal(err)
	}
	jwt := "verified"
	seed := &boundSeedIdentity{1}
	stamp := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	input := &boundSeedInput{Jwt: &jwt, Auth: seed, Clock: stamp, Has: &boundSeedHas{Jwt: true, Enabled: true, Zero: true, Auth: true, Clock: true}}
	contract := testRouteInput(t, reflect.TypeFor[boundSeedInput](), boundSeedBindings()[:5]...)
	captured, err := captureMutationInput(contract, input)
	if err != nil {
		t.Fatal(err)
	}
	clone := captured.(*boundSeedInput)
	if clone.Auth != nil || !clone.Clock.IsZero() || clone.Has == nil || clone.Has.Auth || clone.Has.Clock || !clone.Has.Jwt || !clone.Has.Enabled || !clone.Has.Zero || !input.Has.Auth || !input.Has.Clock {
		t.Fatalf("retry capture leaked supplemental presence: %+v original=%+v", clone, input)
	}
	probe := &recoveryProbe{}
	probe.finalize = func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error { return nil }
	var attempts []*boundSeedInput
	probe.execute = func(ctx context.Context, v rhandler.Invocation) (any, error) {
		actual := v.Input.(*boundSeedInput)
		attempts = append(attempts, actual)
		if actual.Has == nil || !actual.Has.Jwt || !actual.Has.Enabled || !actual.Has.Zero || actual.Enabled || actual.Zero != 0 {
			t.Fatal("retry lost explicit false/zero presence")
		}
		if len(attempts) == 2 {
			return actual, nil
		}
		value, _, err := v.Binder.Lookup(ctx, xhandler.DMLKey)
		if err != nil {
			return nil, err
		}
		return actual, value.(xhandler.DML).Insert("bound_seed_retry", &struct {
			ID int `sqlx:"id,primaryKey"`
		}{ID: 1})
	}
	probe.recover = func(_ context.Context, _ rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
		if outcome.Attempt != 0 || !outcome.CommitConfirmed() || outcome.Mutation.Affected != 0 {
			t.Fatal("invalid retry evidence", outcome)
		}
		return rhandler.RecoveryRetry, nil
	}
	authCalls := 0
	fresh := &boundSeedIdentity{2}
	_, err = New().Execute(t.Context(), Request{Input: contract, BoundInput: input, Handler: probe, DataSource: dml.Source{DB: db.DB},
		Components: handlerprovider.Named("component", func(context.Context, reflect.Type, string) (any, bool, error) { authCalls++; return fresh, true, nil })})
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0] != input || attempts[0].Auth != seed || attempts[0].Clock != stamp || attempts[1] == input || attempts[1].Auth != fresh ||
		attempts[1].Clock.IsZero() || attempts[1].Clock == stamp || authCalls != 1 || db.DB.Stats().InUse != 0 {
		t.Fatalf("retry did not freshly bind: attempts=%+v calls=%d", attempts, authCalls)
	}
}
