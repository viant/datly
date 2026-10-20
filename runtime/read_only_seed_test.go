package runtime

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

func TestReadOnlyDependencySeedRejectsMarksBeforeFiltering(t *testing.T) {
	type nested struct{ Child *int }
	type input struct {
		URL    string
		Parent *nested
		Child  *int
		Has    *struct{ URL, Parent, Child bool } `setMarker:"true"`
	}
	for _, tc := range []struct{ name, member, seedPath, kind string }{
		{name: "nil grouped output", member: "Child", seedPath: "Child", kind: "component"},
		{name: "filtered nil ancestor", member: "Parent.Child", seedPath: "Parent", kind: "state"},
		{name: "filtered capability ancestor", member: "Parent.Child", seedPath: "Parent", kind: "logger"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			specs := []bindly.BindingSpec{{Path: tc.member, Location: bindstate.Location{Kind: "component", In: "GET:/child"}, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "reads", Mode: bindly.ResolutionGroupDependencies}}}
			if tc.seedPath != tc.member {
				specs = append(specs, bindly.BindingSpec{Path: tc.seedPath, Location: bindstate.Location{Kind: tc.kind, In: "seed"}})
			}
			injector, err := bindly.NewInjector()
			require.NoError(t, err)
			plan, err := injector.CompilePlan(reflect.TypeFor[input](), specs...)
			require.NoError(t, err)
			projection, err := plan.Projection()
			require.NoError(t, err)
			ref := spec.RouteRef{Method: "GET", Path: "/seed-marks"}
			inputContract, err := registry.NewInputContract(reflect.TypeFor[input](), projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: specs})
			require.NoError(t, err)
			contract, ok := inputContract.ForRoute(ref)
			require.True(t, ok)
			seed := &input{Has: &struct{ URL, Parent, Child bool }{}}
			if tc.seedPath == "Parent" {
				seed.Has.Parent = true
			} else {
				seed.Has.Child = true
			}
			registered := &registry.RegisteredComponent{Input: inputContract, Reader: &seedReader{}}
			_, _, err = (&Runtime{}).prepareReaderSeed(t.Context(), registered, contract, dexec.WithInput(seed))
			require.ErrorContains(t, err, "overlaps resolution group binding")
		})
	}
}

type readOnlyAuthSeedInput struct {
	URL   string                          `parameter:",kind=const,in=snapshot,value=default"`
	Jwt   *jwt.Claims                     `parameter:",kind=header,in=Authorization,dataType=string,required=true" codec:"JwtClaim"`
	Child *bufferedCallOutput             `parameter:",kind=component,in=GET:/auth-seed-child,resolutionGroup=reads,resolutionMode=dependencies,resolutionAfter=Jwt"`
	Has   *struct{ URL, Jwt, Child bool } `setMarker:"true"`
}
type readOnlyAuthSeedReader struct{ calls atomic.Int32 }

func (r *readOnlyAuthSeedReader) Read(_ context.Context, input any, _ xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	r.calls.Add(1)
	i := input.(*readOnlyAuthSeedInput)
	return &readOnlyAuthSeedOutput{Subject: i.Jwt.Subject, URL: i.URL}, nil
}

type readOnlyAuthSeedOutput struct{ Subject, URL string }
type readOnlySeedCountingReader struct {
	dexec.Reader
	calls atomic.Int32
}

func (r *readOnlySeedCountingReader) Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	r.calls.Add(1)
	return r.Reader.Read(ctx, input, binder, resolver)
}

func TestReadOnlyDependencySeedAuthenticationStillRuns(t *testing.T) {
	key := testharness.NewJWT(t)
	service, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	require.NoError(t, err)
	child := readerFinalizerNative(t, readOnlyDatabase(t).DB, "AuthSeedChild", "/auth-seed-child", reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedCallOutput]())
	counted := &readOnlySeedCountingReader{Reader: child.Reader}
	child.Reader = counted
	component := componentSpec("AuthSeed", "GET", "/auth-seed", []*spec.Parameter{{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/auth-seed-child"}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "reads", Mode: "dependencies", After: []string{"Jwt"}}}})
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[readOnlyAuthSeedInput](), OutputType: reflect.TypeFor[readOnlyAuthSeedOutput](), CodecFactory: service})
	require.NoError(t, err)
	rootReader := &readOnlyAuthSeedReader{}
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[readOnlyAuthSeedOutput](), Reader: rootReader}, child})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	contract, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/auth-seed"})
	require.True(t, ok)
	verified := false
	for _, field := range contract.Fields() {
		verified = verified || field.VerifiesJWT()
	}
	require.True(t, verified, "must exercise the actual verifier, not a codec name")
	for _, tc := range []struct {
		name          string
		marked, valid bool
	}{
		{name: "marked forged claims", marked: true},
		{name: "marked nil claims", marked: true},
		{name: "unmarked forged claims with invalid token"},
		{name: "valid token", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := counted.calls.Load()
			seed := &readOnlyAuthSeedInput{URL: "selected-snapshot", Jwt: &jwt.Claims{}, Has: &struct{ URL, Jwt, Child bool }{URL: true, Jwt: tc.marked}}
			if tc.name == "marked nil claims" {
				seed.Jwt = nil
			}
			token := "Bearer invalid"
			if tc.valid {
				token = key.Sign(t, "approved", time.Now().Add(time.Hour))
			}
			value, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: contract.Route()}, ExtraInput: dexec.WithInput(seed), Providers: []locator.Provider{hp.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return token, true, nil })}})
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, &readOnlyAuthSeedOutput{Subject: "approved", URL: "selected-snapshot"}, value)
				require.Equal(t, before+1, counted.calls.Load())
			} else {
				require.Error(t, err)
				require.Equal(t, before, counted.calls.Load())
				if tc.marked {
					require.ErrorContains(t, err, "authorization binding Jwt")
				}
			}
			require.Equal(t, "selected-snapshot", seed.URL)
			require.Equal(t, tc.marked, seed.Has.Jwt)
		})
	}
	require.EqualValues(t, 1, rootReader.calls.Load())
}
