package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bodyprovider "github.com/viant/bindly/provider/body"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/bindly/xform"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	rcontract "github.com/viant/datly/runtime/contract"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/response"
)

type preparedExecutionStats struct {
	auth, policy, preparation, decode, business atomic.Int64
	unavailable, failPreparation                atomic.Bool
}

type preparedExecutionAuthInput struct {
	Role string `parameter:",kind=header,in=Authorization,required=true"`
}
type preparedExecutionAuth struct {
	Role  string
	Stats *preparedExecutionStats
}
type preparedExecutionBody struct {
	Secret string
	Value  int
	Has    *struct{ Secret, Value bool } `setMarker:"true" json:"-"`
}
type preparedExecutionInput struct {
	Auth    *preparedExecutionAuth `parameter:",kind=component,in=GET:/prepared-auth,required=true"`
	Variant string                 `parameter:",kind=header,in=X-Variant,required=true"`
	Body    *preparedExecutionBody `parameter:"Body,kind=body,in=,required=true" anonymous:"true"`
}

func (*preparedExecutionInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Auth    *preparedExecutionAuth `parameter:",kind=component,in=GET:/prepared-auth,required=true"`
		Variant string                 `parameter:",kind=header,in=X-Variant,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	state.Auth.Stats.policy.Add(1)
	if state.Variant == "nil" {
		return nil, nil
	}
	policy := &xcontract.Policy{Name: state.Auth.Role + "/" + state.Variant, NoCache: state.Variant == "nocache", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[preparedExecutionBody]() && field.Name == "Secret" {
			return field.Hide(), nil
		}
		return nil, nil
	}}
	if state.Variant == "unnamed" {
		policy.Name = ""
	}
	return policy, nil
}

var preparedExecutionUnavailable = errors.New("fixture authorization dependency unavailable")

// This transparent native-body adapter records every WithInputRepresentation
// compilation, including a fallback compilation outside the variant callback.
// The native body provider still owns decoding, filtering and set markers.
type preparedExecutionDecoder struct {
	stats    *preparedExecutionStats
	metadata *shape.Representation
}

func (d *preparedExecutionDecoder) WireSourceType() reflect.Type {
	return reflect.TypeFor[*preparedExecutionBody]()
}
func (d *preparedExecutionDecoder) WithRepresentation(metadata *shape.Representation) (xform.Transformer, error) {
	d.stats.preparation.Add(1)
	if d.stats.failPreparation.Load() {
		return nil, context.Canceled
	}
	return &preparedExecutionDecoder{stats: d.stats, metadata: metadata}, nil
}
func (d *preparedExecutionDecoder) Transform(ctx context.Context, _ locator.Resolver, value any) (any, error) {
	d.stats.decode.Add(1)
	if value == nil {
		return nil, nil
	}
	source, err := bodyprovider.New(value.(json.RawMessage), "application/json", nil, bodyprovider.WithRepresentation(d.metadata))
	if err != nil {
		return nil, err
	}
	result, _, err := source.Value(ctx, reflect.TypeFor[*preparedExecutionBody](), "")
	return result, err
}

type preparedExecutionFixture struct {
	rt      *Runtime
	entry   *RegisteredComponent
	stats   *preparedExecutionStats
	targets []dexec.ComponentTarget
}

func newPreparedExecutionFixture(t testing.TB, retained bool, capacity int, nativeBody ...bool) *preparedExecutionFixture {
	t.Helper()
	f := &preparedExecutionFixture{stats: new(preparedExecutionStats)}
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "PreparedAuth"}, Routes: []*spec.Route{{Method: "GET", Path: "/prepared-auth"}}}, InputType: reflect.TypeFor[preparedExecutionAuthInput](), OutputType: reflect.TypeFor[preparedExecutionAuth](), Handler: custom.NewFunc(func(_ context.Context, in *preparedExecutionAuthInput) (*preparedExecutionAuth, error) {
		f.stats.auth.Add(1)
		if f.stats.unavailable.Load() {
			return nil, &response.Error{Code: 503, Cause: preparedExecutionUnavailable}
		}
		return &preparedExecutionAuth{Role: in.Role, Stats: f.stats}, nil
	})})
	require.NoError(t, err)
	a, err := auth.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "PreparedExecution"}, Routes: []*spec.Route{{Method: "POST", Path: "/prepared-a"}, {Method: "PATCH", Path: "/prepared-b"}}}
	business, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[preparedExecutionInput](), OutputType: reflect.TypeFor[preparedExecutionBody](), Handler: custom.NewFunc(func(_ context.Context, in *preparedExecutionInput) (*preparedExecutionBody, error) {
		f.stats.business.Add(1)
		return in.Body, nil
	})})
	require.NoError(t, err)
	b, err := business.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	var routes []registry.RouteInput
	var projection *bindly.Projection
	in, err := bindly.NewInjector()
	require.NoError(t, err)
	for _, route := range component.Routes {
		ref := spec.RouteRef{Method: route.Method, Path: route.Path}
		original, ok := b.Input.ForRoute(ref)
		require.True(t, ok)
		var bindings []bindly.BindingSpec
		for _, field := range original.Fields() {
			binding := field.Binding()
			if binding.Location.Kind == "body" && (len(nativeBody) == 0 || !nativeBody[0]) {
				binding.SourceType = reflect.TypeFor[json.RawMessage]()
				binding.Transformer = &preparedExecutionDecoder{stats: f.stats}
			}
			bindings = append(bindings, binding)
		}
		plan, err := in.CompilePlan(b.Input.Type(), bindings...)
		require.NoError(t, err)
		projection, err = plan.Projection()
		require.NoError(t, err)
		routes = append(routes, registry.RouteInput{Route: ref, Plan: plan, Bindings: bindings, WireTypes: map[string]reflect.Type{"Body": reflect.TypeFor[*preparedExecutionBody]()}})
		f.targets = append(f.targets, dexec.ComponentTarget{Component: component.Key, Route: ref})
	}
	b.Input, err = registry.NewInputContract(b.Input.Type(), projection, routes...)
	require.NoError(t, err)
	f.rt, err = NewRuntime([]*registry.RegisteredComponent{a, b})
	require.NoError(t, err)
	b = f.rt.registered[component.Key.String()]
	f.entry = b
	t.Cleanup(func() { require.NoError(t, f.rt.Shutdown(context.Background())) })
	if !retained || capacity != 64 {
		// Baseline reproduces the previous native path: cache representation,
		// then compile WithInputRepresentation in component invocation each time.
		for _, target := range f.targets {
			plans, err := f.rt.contractPlansFor(b, target.Route.String())
			require.NoError(t, err)
			if retained {
				original, _ := b.Input.ForRoute(target.Route)
				plans.input, err = rcontract.NewInput(b.Input.Type(), capacity, prepareInputExecution(original))
			} else {
				plans.input, err = rcontract.New(b.Input.Type(), capacity)
			}
			require.NoError(t, err)
		}
	}
	return f
}

func (f *preparedExecutionFixture) invoke(ctx context.Context, target int, variant string) (*preparedExecutionBody, error) {
	scope, err := f.scope(target, variant)
	if err != nil {
		return nil, err
	}
	defer scope.Close()
	result, err := f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.targets[target], Providers: scope.Providers()})
	if err != nil {
		return nil, err
	}
	return result.(*preparedExecutionBody), nil
}

func (f *preparedExecutionFixture) scope(target int, variant string) (*requestprovider.Scope, error) {
	ref := f.targets[target]
	request := httptest.NewRequest(ref.Route.Method, ref.Route.Path, strings.NewReader(`{"Secret":"blocked","Value":0}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "reader")
	request.Header.Set("X-Variant", variant)
	return requestprovider.New(request)
}

func TestPreparedInputExecutionNativeWarmPaths(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprintf("retained=%v", retained), func(t *testing.T) {
			f := newPreparedExecutionFixture(t, retained, 64)
			for range 5 {
				body, err := f.invoke(t.Context(), 0, "named")
				require.NoError(t, err)
				require.Empty(t, body.Secret)
				require.NotNil(t, body.Has)
				require.True(t, body.Has.Value, "explicit zero presence reaches canonical business input")
				require.False(t, body.Has.Secret)
			}
			want := int64(5)
			if retained {
				want = 1
			}
			require.Equal(t, want, f.stats.preparation.Load(), "each count enters native WithInputRepresentation compilation")
			require.EqualValues(t, 5, f.stats.auth.Load())
			require.EqualValues(t, 5, f.stats.policy.Load())
			require.EqualValues(t, 5, f.stats.decode.Load())
			require.EqualValues(t, 5, f.stats.business.Load())
		})
	}
}

func TestPreparedInputExecutionNativeLifetime(t *testing.T) {
	f := newPreparedExecutionFixture(t, true, 2)
	for _, variant := range []string{"a", "a", "b", "c", "a", "nocache", "nocache", "unnamed", "unnamed"} {
		_, err := f.invoke(t.Context(), 0, variant)
		require.NoError(t, err)
	}
	require.EqualValues(t, 8, f.stats.preparation.Load(), "four bounded named misses plus four unretained policies")
	_, err := f.invoke(t.Context(), 1, "a")
	require.NoError(t, err)
	require.EqualValues(t, 9, f.stats.preparation.Load(), "route identity owns an independent artifact")
	plans, err := f.rt.contractPlansFor(f.entry, f.targets[0].Route.String())
	require.NoError(t, err)
	copy := *f.entry
	reloaded, err := f.rt.contractPlansFor(&copy, f.targets[0].Route.String())
	require.NoError(t, err)
	require.NotSame(t, plans.input, reloaded.input, "registration/config generation never reuses an old plan")
	_, err = f.invoke(t.Context(), 0, "nil")
	require.NoError(t, err)
	require.EqualValues(t, 9, f.stats.preparation.Load(), "original linked route needs no effective compilation")
}

func TestPreparedInputExecutionNativeOperationHandoff(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%v", changed), func(t *testing.T) {
			f := newPreparedExecutionFixture(t, true, 64)
			ctx := dexec.CaptureContracts(t.Context())
			before, err := f.scope(0, "before")
			require.NoError(t, err)
			defer before.Close()
			selected, err := f.rt.ResolveContract(ctx, f.targets[0], before)
			require.NoError(t, err)
			dexec.PublishContract(ctx, f.targets[0], selected)
			after := before
			if changed {
				after, err = f.scope(0, "after")
				require.NoError(t, err)
				defer after.Close()
			}
			_, err = f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.targets[0], Providers: after.Providers()})
			require.NoError(t, err)
			want := int64(1)
			if changed {
				want++
			}
			require.Equal(t, want, f.stats.preparation.Load())
			require.Equal(t, want, f.stats.policy.Load())
			require.EqualValues(t, 1, f.stats.auth.Load(), "same-operation dependency authority keeps its existing proof rules")
			_, err = f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.targets[0], Providers: after.Providers()})
			require.NoError(t, err)
			require.EqualValues(t, 2, f.stats.auth.Load(), "a completed operation handoff never supplies authorization")
			require.Equal(t, want, f.stats.preparation.Load(), "only immutable execution preparation survives operation completion")
		})
	}
}

func TestPreparedInputExecutionNativeConcurrentCold(t *testing.T) {
	f := newPreparedExecutionFixture(t, true, 64)
	const count = 24
	var wg sync.WaitGroup
	errors := make(chan error, count)
	start := make(chan struct{})
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.invoke(t.Context(), 0, "concurrent")
			errors <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, f.stats.preparation.Load())
	require.EqualValues(t, count, f.stats.auth.Load())
	require.EqualValues(t, count, f.stats.policy.Load())
	require.EqualValues(t, count, f.stats.business.Load())
}

func TestPreparedInputExecutionNativeFailuresRetry(t *testing.T) {
	f := newPreparedExecutionFixture(t, true, 64)
	f.stats.failPreparation.Store(true)
	_, err := f.invoke(t.Context(), 0, "named")
	require.ErrorIs(t, err, context.Canceled)
	f.stats.failPreparation.Store(false)
	_, err = f.invoke(t.Context(), 0, "named")
	require.NoError(t, err)
	require.EqualValues(t, 2, f.stats.preparation.Load(), "cancellation does not poison retained preparation")
	f.stats.unavailable.Store(true)
	_, err = f.invoke(t.Context(), 0, "named")
	require.ErrorIs(t, err, preparedExecutionUnavailable)
	var responseError *response.Error
	require.ErrorAs(t, err, &responseError)
	require.Equal(t, 503, responseError.Code)
	require.EqualValues(t, 1, f.stats.business.Load(), "warm metadata never substitutes for current authorization")
	f.stats.unavailable.Store(false)
	_, err = f.invoke(t.Context(), 0, "named")
	require.NoError(t, err)
	require.EqualValues(t, 2, f.stats.preparation.Load())
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = f.invoke(cancelled, 0, "named")
	require.ErrorIs(t, err, context.Canceled)
}

func TestPreparedInputExecutionNativeBodyRepresentation(t *testing.T) {
	f := newPreparedExecutionFixture(t, true, 64, true)
	plans, err := f.rt.contractPlansFor(f.entry, f.targets[0].Route.String())
	require.NoError(t, err)
	original, _ := f.entry.Input.ForRoute(f.targets[0].Route)
	prepare := prepareInputExecution(original)
	var captured *preparedInputExecutionPlan
	var count atomic.Int64
	plans.input, err = rcontract.NewInput(f.entry.Input.Type(), 64, func(metadata *shape.Representation) (any, error) {
		value, err := prepare(metadata)
		if err == nil {
			count.Add(1)
			captured = value.(*preparedInputExecutionPlan)
		}
		return value, err
	})
	require.NoError(t, err)
	for range 3 {
		body, err := f.invoke(t.Context(), 0, "named")
		require.NoError(t, err)
		require.Empty(t, body.Secret)
		require.NotNil(t, body.Has)
		require.True(t, body.Has.Value)
		require.False(t, body.Has.Secret)
	}
	require.EqualValues(t, 1, count.Load())
	require.NotNil(t, captured)
	for _, field := range captured.route.Fields() {
		binding := field.Binding()
		if binding.Location.Kind == "body" {
			require.Nil(t, binding.Transformer)
			require.NotNil(t, binding.BodyRepresentation, "actual Bindly CompilePlan owns the precomputed representation key")
		}
	}
	require.EqualValues(t, 3, f.stats.auth.Load())
	require.EqualValues(t, 3, f.stats.policy.Load())
	require.EqualValues(t, 3, f.stats.business.Load())
}

func BenchmarkPreparedInputExecutionNative(b *testing.B) {
	for _, retained := range []bool{false, true} {
		b.Run(fmt.Sprintf("retained=%v", retained), func(b *testing.B) {
			f := newPreparedExecutionFixture(b, retained, 64)
			_, err := f.invoke(b.Context(), 0, "named")
			require.NoError(b, err)
			before := f.stats.preparation.Load()
			beforeAuth, beforePolicy, beforeDecode := f.stats.auth.Load(), f.stats.policy.Load(), f.stats.decode.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := f.invoke(b.Context(), 0, "named")
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(f.stats.preparation.Load()-before)/float64(b.N), "compile/op")
			b.ReportMetric(float64(f.stats.auth.Load()-beforeAuth)/float64(b.N), "auth/op")
			b.ReportMetric(float64(f.stats.policy.Load()-beforePolicy)/float64(b.N), "policy/op")
			b.ReportMetric(float64(f.stats.decode.Load()-beforeDecode)/float64(b.N), "decode/op")
		})
	}
}

// Measure the default Bindly BodyRepresentation path as well as the transparent
// decoder fixture above. That native plan includes Signature/SHA preparation.
func BenchmarkPreparedInputExecutionNativeBody(b *testing.B) {
	for _, retained := range []bool{false, true} {
		b.Run(fmt.Sprintf("retained=%v", retained), func(b *testing.B) {
			f := newPreparedExecutionFixture(b, retained, 64, true)
			_, err := f.invoke(b.Context(), 0, "named")
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := f.invoke(b.Context(), 0, "named")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPreparedInputExecutionNativeBodyCold(b *testing.B) {
	for _, retained := range []bool{false, true} {
		b.Run(fmt.Sprintf("retained=%v", retained), func(b *testing.B) {
			f := newPreparedExecutionFixture(b, retained, 64, true)
			// Both paths start with the registration's hook/rewrite discovery
			// prepared; each measured named variant is a cold bounded-cache miss.
			_, err := f.rt.contractPlansFor(f.entry, f.targets[0].Route.String())
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				_, err := f.invoke(b.Context(), 0, fmt.Sprintf("cold-%d", i))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
