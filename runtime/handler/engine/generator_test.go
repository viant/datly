package engine

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"sync"
	"testing"
	"time"
)

type clockGeneratorInput struct {
	Clock       time.Time
	Pointer     *time.Time
	False       *bool
	Zero        *int
	Empty       *string
	Nil         *string
	Unknown     *string
	UUID        string
	UUIDAgain   string
	Has         *struct{ Clock, Pointer, False, Zero, Empty, Nil, Unknown, UUID, UUIDAgain bool } `setMarker:"true"`
	Initialized bool
}

func (i *clockGeneratorInput) Init(context.Context) error {
	if i.Clock.IsZero() || i.Pointer == nil || i.Pointer.IsZero() {
		return errors.New("clock missing before Init")
	}
	i.Initialized = true
	return nil
}

func clockGeneratorBindings() []bindly.BindingSpec {
	result := []bindly.BindingSpec{}
	for _, field := range []struct{ path, name string }{{"Clock", "current_time"}, {"Pointer", "NOW"}, {"False", "false"}, {"Zero", "zero"}, {"Empty", "empty"}, {"Nil", "nil"}, {"Unknown", "unknown"}, {"UUID", "uuid"}, {"UUIDAgain", "uuid"}} {
		result = append(result, bindly.BindingSpec{Path: field.path, Name: field.path, Location: bindstate.Location{Kind: "generator", In: field.name}})
	}
	return result
}
func TestEngineGeneratorBeforeInitTypesPresenceAndConcurrency(t *testing.T) {
	contract := testRouteInput(t, reflect.TypeOf(clockGeneratorInput{}), clockGeneratorBindings()...)
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	for j := 0; j < 16; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			actual, err := New().Execute(context.Background(), Request{Input: contract, Handler: rhandler.HandlerFunc(func(_ context.Context, v rhandler.Invocation) (any, error) { return v.Input, nil })})
			end := time.Now()
			if err != nil {
				t.Error(err)
				return
			}
			input := actual.(*clockGeneratorInput)
			if !input.Initialized || input.Clock.Before(start) || input.Clock.After(end) || input.False == nil || *input.False || input.Zero == nil || *input.Zero != 0 || input.Empty == nil || *input.Empty != "" || input.Nil != nil || input.Unknown != nil || input.Has == nil || !input.Has.Clock || !input.Has.False || !input.Has.Zero || !input.Has.Empty || !input.Has.Nil || input.Has.Unknown {
				t.Error("generated value/type/presence differs")
			}
			if input.UUID != input.UUIDAgain {
				t.Error("native binding cache identity changed")
			}
			ids <- input.UUID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Error("cross-invocation UUID cache")
		}
		seen[id] = true
	}
}

func TestEngineGeneratorExplicitProvidersAreAuthoritative(t *testing.T) {
	type input struct{ Clock *time.Time }
	contract := testRouteInput(t, reflect.TypeOf(input{}), bindly.BindingSpec{Path: "Clock", Location: bindstate.Location{Kind: "generator", In: "current_time"}})
	sentinel := errors.New("custom generator failure")
	stamp := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, mode := range []string{"value", "absent", "nil", "error"} {
		for _, layer := range []string{"component", "protocol", "root", "injector parent", "registry parent"} {
			t.Run(mode+"/"+layer, func(t *testing.T) {
				calls := 0
				custom := handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) {
					calls++
					switch mode {
					case "absent":
						return nil, false, nil
					case "nil":
						return nil, true, nil
					case "error":
						return nil, false, sentinel
					}
					return stamp, true, nil
				})
				req := Request{Input: contract, Handler: rhandler.HandlerFunc(func(_ context.Context, v rhandler.Invocation) (any, error) { return v.Input, nil })}
				var err error
				switch layer {
				case "component":
					req.Providers = []locator.Provider{custom}
				case "protocol":
					req.Scope = engineProviderScope{custom}
				case "root":
					req.Injector, err = bindly.NewInjector(bindly.WithProviders(custom))
				case "injector parent":
					var root *bindly.Injector
					root, err = bindly.NewInjector(bindly.WithProviders(custom))
					if err == nil {
						req.Injector, err = root.ForScope()
					}
				case "registry parent":
					registry := locator.NewRegistry()
					err = registry.Register(custom)
					if err == nil {
						req.Injector, err = bindly.NewInjector(bindly.WithLocators(registry.Child()))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				actual, err := New().Execute(context.Background(), req)
				if mode == "error" {
					if !errors.Is(err, sentinel) {
						t.Fatalf("custom cause lost: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					got := actual.(*input).Clock
					if mode == "value" {
						if got == nil || !got.Equal(stamp) {
							t.Fatal("default masked custom")
						}
					} else if got != nil {
						t.Fatal("default filled explicit absence/null")
					}
				}
				if calls != 1 {
					t.Fatalf("resolver was probed or repeated: %d", calls)
				}
			})
		}
	}
}

func TestEngineGeneratorRequiredNullUnknownAndTrustedInput(t *testing.T) {
	yes := true
	for _, name := range []string{"nil", "unknown"} {
		type input struct{ Value *string }
		contract := testRouteInput(t, reflect.TypeOf(input{}), bindly.BindingSpec{Path: "Value", Required: &yes, Location: bindstate.Location{Kind: "generator", In: name}})
		called := false
		_, err := New().Execute(context.Background(), Request{Input: contract, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })})
		if err == nil || called {
			t.Fatalf("required %q entered handler", name)
		}
	}
	contract := testRouteInput(t, reflect.TypeOf(clockGeneratorInput{}), clockGeneratorBindings()...)
	supplied := &clockGeneratorInput{Clock: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)}
	actual, err := New().Execute(context.Background(), Request{Input: contract, BoundInput: supplied, Handler: rhandler.HandlerFunc(func(_ context.Context, v rhandler.Invocation) (any, error) { return v.Input, nil })})
	if err != nil {
		t.Fatal(err)
	}
	if actual != supplied || !supplied.Initialized || supplied.Clock.Year() == 1999 {
		t.Fatal("trusted supplemental classification changed")
	}
}

func TestDefaultGeneratorPreservesCompositionValidationAndChildOverride(t *testing.T) {
	a := handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, nil })
	if _, err := (providerComposer{}).compose(providerComposition{component: []locator.Provider{a, a}}); err == nil {
		t.Fatal("duplicates accepted")
	}
	if _, err := (providerComposer{}).compose(providerComposition{protocol: []locator.Provider{handlerprovider.Parameter()}}); err == nil {
		t.Fatal("protected kind accepted")
	}
	composed, err := ComposeScope(engineProviderScope{a}, handlerprovider.Named("generator", func(context.Context, reflect.Type, string) (any, bool, error) { return "child", true, nil }))
	if err != nil {
		t.Fatal(err)
	}
	type input struct{ Value string }
	contract := testRouteInput(t, reflect.TypeOf(input{}), bindly.BindingSpec{Path: "Value", Location: bindstate.Location{Kind: "generator", In: "uuid"}})
	actual, err := New().Execute(context.Background(), Request{Input: contract, Scope: composed, Handler: rhandler.HandlerFunc(func(_ context.Context, v rhandler.Invocation) (any, error) { return v.Input, nil })})
	if err != nil || actual.(*input).Value != "child" {
		t.Fatal("child override changed", err)
	}
}

func TestEngineGeneratorRetryKeepsNativeFreshSupplementalPolicy(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport", true: "trusted"}[trusted], func(t *testing.T) {
			db := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE generator_retry(id INTEGER PRIMARY KEY)", "CREATE TRIGGER ignored BEFORE INSERT ON generator_retry BEGIN SELECT RAISE(IGNORE); END"); err != nil {
				t.Fatal(err)
			}
			contract := testRouteInput(t, reflect.TypeFor[clockGeneratorInput](), clockGeneratorBindings()...)
			probe := &recoveryProbe{}
			probe.finalize = func(context.Context, rhandler.Invocation, any, xhandler.Outcome) error { return nil }
			var attempts []*clockGeneratorInput
			probe.execute = func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				input := invocation.Input.(*clockGeneratorInput)
				if !input.Initialized || input.Clock.IsZero() || input.Has == nil || !input.Has.Clock {
					t.Fatal("retry entered business logic without generator/Init/presence")
				}
				attempts = append(attempts, input)
				if len(attempts) == 2 {
					return input, nil
				}
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
				if err != nil {
					return nil, err
				}
				return input, value.(xhandler.DML).Insert("generator_retry", &struct {
					ID int `sqlx:"id,primaryKey"`
				}{ID: 1})
			}
			probe.recover = func(_ context.Context, _ rhandler.Invocation, _ any, outcome rhandler.MutationOutcome) (rhandler.Recovery, error) {
				if outcome.Attempt != 0 || !outcome.CommitConfirmed() || outcome.Mutation.Affected != 0 {
					t.Fatal("invalid retry evidence", outcome)
				}
				return rhandler.RecoveryRetry, nil
			}
			request := Request{Input: contract, Handler: probe, DataSource: dml.Source{DB: db.DB}}
			if trusted {
				request.BoundInput = &clockGeneratorInput{Clock: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)}
			}
			_, err := New().Execute(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 2 || attempts[0] == attempts[1] || attempts[0].UUID == attempts[1].UUID || attempts[1].Clock.Before(attempts[0].Clock) {
				t.Fatal("retry reused generated values or input", attempts)
			}
			if db.DB.Stats().InUse != 0 {
				t.Fatal("retry leaked connection")
			}
		})
	}
}
