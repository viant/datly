package runtime

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx"
	xcodec "github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
)

type scopedSeedCodec struct {
	xcodec.Factory
	calls atomic.Int32
}

func (f *scopedSeedCodec) New(c *xcodec.Config, opts ...xcodec.Option) (xcodec.Instance, error) {
	i, e := f.Factory.New(c, opts...)
	if e != nil {
		return nil, e
	}
	return scopedSeedInstance{i, &f.calls}, nil
}

type scopedSeedInstance struct {
	xcodec.Instance
	calls *atomic.Int32
}

func (i scopedSeedInstance) Value(ctx context.Context, v any, opts ...xcodec.Option) (any, error) {
	i.calls.Add(1)
	return i.Instance.Value(ctx, v, opts...)
}

type scopedSeedForwardInput struct {
	Jwt *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
}
type scopedSeedForward struct {
	target xhandler.ComponentTarget
	seed   *scopedJWTInput
	log    *scopedNativeLog
}

func (h *scopedSeedForward) Exec(ctx context.Context, s xhandler.Session, i *scopedSeedForwardInput, o *scopedNativeOutput) error {
	h.log.Info("forward")
	v, found, e := s.Binder().Lookup(ctx, xhandler.ComponentInvokerKey)
	if e != nil || !found {
		return errors.New("native invoker missing")
	}
	h.seed = &scopedJWTInput{Jwt: i.Jwt, Child: &scopedNativeChildOutput{Value: "must not enter lazy scope"}, Has: &struct{ Jwt, Child bool }{true, true}}
	out, e := v.(xhandler.ComponentInvoker).Invoke(ctx, xhandler.ComponentRequest{Target: h.target, ExtraInput: xhandler.WithInput(h.seed)})
	if e == nil {
		*o = *out.(*scopedNativeOutput)
	}
	return e
}
func TestScopedSeedGenuineParentJWTDefaultPrebindingAndLazyChild(t *testing.T) {
	key := testharness.NewJWT(t)
	for _, tc := range []struct {
		name, subject string
		expiry        time.Duration
		want          string
		success       bool
	}{
		{"valid", "approved", time.Hour, "default,forward,begin,authorize,child,end", true},
		{"denied", "denied", time.Hour, "default,forward,begin", false},
		{"expired", "approved", -time.Hour, "default", false},
		{"missing", "", time.Hour, "default", false},
		{"malformed", "malformed", time.Hour, "default", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &scopedNativeLog{}
			service, e := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
			if e != nil {
				t.Fatal(e)
			}
			factory := &scopedSeedCodec{Factory: service}
			auth, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("SeedScoped", "GET", "/scope-jwt", nil), InputType: reflect.TypeFor[scopedJWTInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[scopedJWTInput, scopedNativeOutput](scopedJWTContract{}), CodecFactory: factory})
			if e != nil {
				t.Fatal(e)
			}
			forwarding := &scopedSeedForward{target: xhandler.ComponentTarget{Scope: auth.Component.Key.Scope, Name: auth.Component.Key.Name, Method: "GET", Path: "/scope-jwt"}, log: log}
			parent, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("SeedForward", "GET", "/seed-forward", nil), InputType: reflect.TypeFor[scopedSeedForwardInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[scopedSeedForwardInput, scopedNativeOutput](forwarding), CodecFactory: factory})
			if e != nil {
				t.Fatal(e)
			}
			child := componentArtifact(t, componentSpec("SeedLazy", "GET", "/scope-jwt-child", nil), reflect.TypeFor[scopedNativeChildInput](), reflect.TypeFor[scopedNativeChildOutput]())
			children := 0
			child.Handler = custom.NewFunc(func(context.Context, *scopedNativeChildInput) (*scopedNativeChildOutput, error) {
				children++
				log.Info("child")
				return &scopedNativeChildOutput{Value: "real lazy child"}, nil
			})
			var registrations []*registry.RegisteredComponent
			for _, a := range []*bootstrap.Artifact{parent, auth, child} {
				r, e := a.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: log}})
				if e != nil {
					t.Fatal(e)
				}
				registrations = append(registrations, r)
			}
			rt, e := NewRuntime(registrations)
			if e != nil {
				t.Fatal(e)
			}
			defer rt.Shutdown(context.Background())
			req := httptest.NewRequest("GET", "/seed-forward", nil)
			if tc.name == "malformed" {
				req.Header.Set("Authorization", "Bearer invalid")
			} else if tc.name != "missing" {
				req.Header.Set("Authorization", key.Sign(t, tc.subject, time.Now().Add(tc.expiry)))
			}
			scope, e := request.New(req)
			if e != nil {
				t.Fatal(e)
			}
			headers := 0
			provider := handlerprovider.Named("header", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				headers++
				log.Info("default")
				v := req.Header.Get(name)
				return v, v != "", nil
			})
			value, e := rt.invokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-forward"}}, Providers: []locator.Provider{provider}}, scope)
			if (e == nil) != tc.success || strings.Join(log.events, ",") != tc.want || headers != 1 {
				t.Fatalf("result %T err%v order%v headers%d", value, e, log.events, headers)
			}
			wantCodec := int32(1)
			if tc.name == "missing" {
				wantCodec = 0
			}
			if factory.calls.Load() != wantCodec {
				t.Fatalf("JWT verified %d times, want%d", factory.calls.Load(), wantCodec)
			}
			if tc.success {
				if children != 1 || value.(*scopedNativeOutput).Value != "real lazy child" {
					t.Fatal(value, children)
				}
			} else if children != 0 {
				t.Fatal("denied lazy effect")
			}
			if forwarding.seed != nil {
				if forwarding.seed.Jwt.Subject != tc.subject || forwarding.seed.Child.Value != "must not enter lazy scope" || !forwarding.seed.Has.Jwt || !forwarding.seed.Has.Child {
					t.Fatal("caller seed mutated")
				}
			}
		})
	}
}

func TestScopedSeedExplicitPresenceAndInvalidAdmission(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, e := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if e != nil {
		t.Fatal(e)
	}
	parent, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("SeedRequired", "GET", "/seed-required", nil), InputType: reflect.TypeFor[scopedJWTInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[scopedJWTInput, scopedNativeOutput](scopedJWTContract{}), CodecFactory: factory})
	if e != nil {
		t.Fatal(e)
	}
	r, e := parent.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: &scopedNativeLog{}}})
	if e != nil {
		t.Fatal(e)
	}
	rt, e := NewRuntime([]*registry.RegisteredComponent{r})
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(context.Background())
	target := dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-required"}}
	for _, tc := range []struct {
		name           string
		seed           any
		beforeProvider bool
		input          any
		replay         *bindly.ReplayBinding
		independent    bool
	}{
		{name: "wrong", seed: &struct{}{}, beforeProvider: true}, {name: "typednil", seed: (*scopedJWTInput)(nil), beforeProvider: true},
		{name: "input-plus-seed", seed: &scopedJWTInput{}, input: &scopedJWTInput{}, beforeProvider: true},
		{name: "replay", seed: &scopedJWTInput{}, replay: &bindly.ReplayBinding{}, beforeProvider: true},
		{name: "independent", seed: &scopedJWTInput{}, independent: true, beforeProvider: true},
		{name: "unmarked", seed: &scopedJWTInput{Jwt: &jwt.Claims{UserID: 999}}},
		{name: "false", seed: &scopedJWTInput{Jwt: &jwt.Claims{UserID: 999}, Has: &struct{ Jwt, Child bool }{}}},
		{name: "explicit-nil", seed: &scopedJWTInput{Has: &struct{ Jwt, Child bool }{Jwt: true}}, beforeProvider: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			_, e := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(tc.seed), Input: tc.input, Replay: tc.replay, IndependentChildTransactions: tc.independent, Providers: []locator.Provider{handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { calls++; return nil, false, nil })}})
			if e == nil {
				t.Fatal("admitted missing/invalid canonical credentials")
			}
			if tc.beforeProvider && calls != 0 || !tc.beforeProvider && calls != 1 {
				t.Fatal("provider count", calls, e)
			}
		})
	}
}

type scopedSeedDataInput struct {
	Credential string                                   `parameter:",kind=header,in=Authorization,required=true"`
	Live       string                                   `parameter:",kind=query,in=live,cacheable=false"`
	Logger     xhandler.Logger                          `parameter:",kind=logger,required=false"`
	Has        *struct{ Credential, Live, Logger bool } `setMarker:"true"`
}
type scopedSeedDataContract struct{ expected xhandler.Logger }

func (scopedSeedDataContract) BindsInputByScope() bool { return true }
func (h scopedSeedDataContract) Exec(_ context.Context, _ xhandler.Session, i *scopedSeedDataInput, o *scopedNativeOutput) error {
	if i.Credential != "seeded" || i.Live != "fresh" || i.Logger != h.expected || i.Has == nil || !i.Has.Credential {
		return errors.New("seed crossed cacheability or capability boundary")
	}
	o.Value = "eligible data only"
	return nil
}
func TestScopedSeedExcludesCapabilitiesAndNoncacheableData(t *testing.T) {
	configured, foreign := &scopedNativeLog{}, &scopedNativeLog{}
	a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("SeedData", "GET", "/seed-data", nil), InputType: reflect.TypeFor[scopedSeedDataInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[scopedSeedDataInput, scopedNativeOutput](scopedSeedDataContract{configured})})
	if e != nil {
		t.Fatal(e)
	}
	r, e := a.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: configured}})
	if e != nil {
		t.Fatal(e)
	}
	rt, e := NewRuntime([]*registry.RegisteredComponent{r})
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(context.Background())
	seed := &scopedSeedDataInput{Credential: "seeded", Live: "stale", Logger: foreign, Has: &struct{ Credential, Live, Logger bool }{true, true, true}}
	headers, queries := 0, 0
	value, e := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: a.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-data"}}, ExtraInput: dexec.WithInput(seed), Providers: []locator.Provider{
		handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { headers++; return "wrong", true, nil }),
		handlerprovider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { queries++; return "fresh", true, nil }),
	}})
	if e != nil || value.(*scopedNativeOutput).Value != "eligible data only" || headers != 0 || queries != 1 || seed.Live != "stale" || seed.Logger != foreign || !seed.Has.Logger {
		t.Fatal(value, e, headers, queries)
	}
}

type groupedSeedInput struct {
	Value string                `parameter:",kind=query,in=value"`
	Has   *struct{ Value bool } `setMarker:"true"`
}
type groupedSeedReader struct{ calls *int }

func (r groupedSeedReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	*r.calls++
	return &scopedNativeOutput{}, nil
}
func TestScopedSeedDoesNotBroadenGroupedReaderAdmission(t *testing.T) {
	a := componentArtifact(t, componentSpec("GroupSeed", "GET", "/group-seed", nil), reflect.TypeFor[groupedSeedInput](), reflect.TypeFor[scopedNativeOutput]())
	reads := 0
	r := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[scopedNativeOutput](), Reader: groupedSeedReader{&reads}}
	rt, e := NewRuntime([]*registry.RegisteredComponent{r})
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(context.Background())
	reg, err := rt.registeredComponent(t.Context(), a.Component.Key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("handler%T reader%T", reg.Handler, reg.Reader)
	inv := drainowner.NewInvocation()
	if e = drainowner.EnrollActivities(inv); e != nil {
		t.Fatal(e)
	}
	group, e := drainowner.OpenBindingGroup(inv, []drainowner.BindingGroupMember{{Path: "row", Target: (spec.RouteRef{Method: "GET", Path: "/group-seed"}).String()}})
	if e != nil {
		t.Fatal(e)
	}
	defer group.Close()
	ctx, e := group.Enter(t.Context(), "row")
	if e != nil {
		t.Fatal(e)
	}
	_, e = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: a.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/group-seed"}}, ExtraInput: dexec.WithInput(&struct{}{})})
	if !errors.Is(e, drainowner.ErrBindingGroup) || reads != 0 {
		t.Fatalf("grouped reader seed admission widened: %v reads%d", e, reads)
	}
}
