package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"net/http"
	"reflect"
	"sync"
	"testing"

	dexec "github.com/viant/datly/exec"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type readerSeedAuth struct {
	OutputLogger *sync.Mutex `parameter:"OutputLogger,kind=logger,required=false" json:"-"`
	Unsupported  any         `json:"-"`
	Status       string
	Context      map[string]int `json:"-"`
}
type readerSeedMarkers struct{ Jwt, Auth, Name, Live, Scoped, Request bool }
type readerSeedInput struct {
	Jwt     string             `parameter:"Jwt,kind=header,in=Authorization"`
	Auth    *readerSeedAuth    `parameter:"Auth,kind=component,in=GET:/seed-auth"`
	Name    string             `parameter:"Name,kind=query,in=name"`
	Live    string             `parameter:"Live,kind=query,in=live,cacheable=false"`
	Scoped  string             `parameter:"Scoped,kind=query,in=scoped,scope=request"`
	Request *http.Request      `parameter:"Request,kind=body,in=request,required=false"`
	Has     *readerSeedMarkers `setMarker:"true"`
}
type readerSeedResult struct{ Input *readerSeedInput }
type readerSeedNative struct{}

func (*readerSeedNative) Read(_ context.Context, input any, _ xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	return &readerSeedResult{Input: input.(*readerSeedInput)}, nil
}

func readerSeedFixture(t *testing.T) (*Runtime, dexec.ComponentTarget, *int) {
	t.Helper()
	child := componentArtifact(t, componentSpec("SeedAuth", "GET", "/seed-auth", nil), reflect.TypeFor[componentGuardInput](), reflect.TypeFor[readerSeedAuth]())
	reader := componentArtifact(t, componentSpec("SeedReader", "GET", "/seed-reader", nil), reflect.TypeFor[readerSeedInput](), reflect.TypeFor[readerSeedResult]())
	calls := new(int)
	rt, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: child.Component, Input: child.Input, OutputType: reflect.TypeFor[readerSeedAuth](), Handler: customhandler.NewFunc[componentGuardInput, readerSeedAuth](func(context.Context, *componentGuardInput) (*readerSeedAuth, error) {
			*calls++
			return &readerSeedAuth{Status: "ok", Context: map[string]int{"account": 20}}, nil
		})},
		{Component: reader.Component, Input: reader.Input, OutputType: reflect.TypeFor[readerSeedResult](), Reader: &readerSeedNative{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return rt, dexec.ComponentTarget{Component: reader.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-reader"}}, calls
}
func TestReaderSeedTrustedIdentityAndOrdinaryBindings(t *testing.T) {
	rt, target, calls := readerSeedFixture(t)
	auth := &readerSeedAuth{OutputLogger: &sync.Mutex{}, Status: "ok", Context: map[string]int{"account": 10}}
	extra := &readerSeedInput{Jwt: "verified-by-application", Auth: auth, Name: "ignored", Live: "stale", Scoped: "stale-scope", Request: &http.Request{Header: http.Header{"X-Private": []string{"parent-only"}}}, Has: &readerSeedMarkers{Jwt: true, Auth: true, Name: false, Live: true, Scoped: true, Request: true}}
	var providers = []locator.Provider{
		handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) {
			return "different-raw-header", true, nil
		}),
		handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
			return "fresh-" + name, true, nil
		}),
	}
	value, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(extra), Providers: providers})
	if err != nil {
		t.Fatal(err)
	}
	in := value.(*readerSeedResult).Input
	if *calls != 0 || in.Jwt != "verified-by-application" || in.Auth.Context["account"] != 10 || in.Name != "fresh-name" || in.Live != "fresh-live" || in.Scoped != "fresh-scoped" || in.Request != nil {
		t.Fatalf("seed/default binding mismatch: %+v calls=%d", in, *calls)
	}
	if in.Auth == auth || in.Auth.OutputLogger != nil || in.Has == extra.Has || !in.Has.Auth || !in.Has.Jwt {
		t.Fatal("capability/presence/ownership mismatch")
	}
	auth.Context["account"] = 99
	if in.Auth.Context["account"] != 10 {
		t.Fatal("authorization cache aliases parent")
	}
	value, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, Providers: providers})
	if err != nil {
		t.Fatal(err)
	}
	if *calls != 1 || value.(*readerSeedResult).Input.Auth.Context["account"] != 20 || value.(*readerSeedResult).Input.Jwt != "different-raw-header" {
		t.Fatal("ordinary no-option path changed")
	}
}
func TestReaderSeedRejectsInvalidTargetsAndCombinations(t *testing.T) {
	rt, target, _ := readerSeedFixture(t)
	for _, tc := range []struct {
		name         string
		seed         *dexec.InputSeed
		input        any
		replay       *bindly.ReplayBinding
		prepare, dry bool
	}{
		{name: "zero seed", seed: &dexec.InputSeed{}},
		{name: "nil payload", seed: dexec.WithInput(nil)},
		{name: "typed nil", seed: dexec.WithInput((*readerSeedInput)(nil))},
		{name: "wrong canonical type", seed: dexec.WithInput(&componentGuardInput{})},
		{name: "existing Input", seed: dexec.WithInput(&readerSeedInput{}), input: &readerSeedInput{}},
		{name: "replay", seed: dexec.WithInput(&readerSeedInput{}), replay: &bindly.ReplayBinding{}},
		{name: "prepare", seed: dexec.WithInput(&readerSeedInput{}), prepare: true},
		{name: "dry run", seed: dexec.WithInput(&readerSeedInput{}), dry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: tc.seed, Input: tc.input, Replay: tc.replay, PrepareQuery: tc.prepare, DryRun: tc.dry})
			if err == nil {
				t.Fatal("invalid seed admitted")
			}
		})
	}
	custom := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: target.Component.Scope, Name: "SeedAuth"}, Route: spec.RouteRef{Method: "GET", Path: "/seed-auth"}}
	if _, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: custom, ExtraInput: dexec.WithInput(&componentGuardInput{})}); err == nil {
		t.Fatal("custom GET handler admitted as native reader")
	}
}
func TestReaderSeedCancellationBeforeCapture(t *testing.T) {
	rt, target, _ := readerSeedFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&readerSeedInput{})}); err == nil {
		t.Fatal("canceled seed invocation continued")
	}
}

func TestReaderSeedConcurrentSiblingOwnership(t *testing.T) {
	rt, target, calls := readerSeedFixture(t)
	// Each parent is immutable during capture. Children mutate only detached data.
	const parents = 20
	var wg sync.WaitGroup
	errors := make(chan error, parents*2)
	for account := 1; account <= parents; account++ {
		extra := &readerSeedInput{Jwt: fmt.Sprintf("account-%d", account), Auth: &readerSeedAuth{Status: "ok", Context: map[string]int{"account": account}}, Has: &readerSeedMarkers{Jwt: true, Auth: true}}
		for sibling := 0; sibling < 2; sibling++ {
			wg.Add(1)
			go func(account int, parent *readerSeedInput) {
				defer wg.Done()
				value, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(parent)})
				if err != nil {
					errors <- err
					return
				}
				child := value.(*readerSeedResult).Input
				if child.Auth.Context["account"] != account || child.Jwt != parent.Jwt || child.Auth == parent.Auth {
					errors <- fmt.Errorf("identity/alias mismatch for account %d", account)
					return
				}
				child.Auth.Context["account"] = -account
				if parent.Auth.Context["account"] != account {
					errors <- fmt.Errorf("parent mutated for account %d", account)
				}
			}(account, extra)
		}
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if *calls != 0 {
		t.Fatalf("seeded Auth resolved %d times", *calls)
	}
}

func TestReaderSeedSerializationCannotConferAuthority(t *testing.T) {
	field, found := reflect.TypeFor[dexec.ComponentRequest]().FieldByName("ExtraInput")
	if !found || field.Tag.Get("json") != "-" {
		t.Fatal("trusted seed must be excluded from serialized requests")
	}
	for _, body := range []string{`{"ExtraInput":{"Jwt":"forged","Has":{"Jwt":true}}}`, `{"extraInput":{"Jwt":"forged"}}`} {
		var decoded dexec.ComponentRequest
		if err := json.Unmarshal([]byte(body), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.ExtraInput != nil {
			t.Fatal("serialized input acquired trusted seed authority")
		}
	}
}

func TestReaderSeedExplicitZeroAndMissingMarkers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		markers  *readerSeedMarkers
		expected string
	}{
		{"explicit zero", &readerSeedMarkers{Jwt: true}, ""},
		{"false marker", &readerSeedMarkers{}, "raw"},
		{"missing markers", nil, "raw"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, target, _ := readerSeedFixture(t)
			value, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&readerSeedInput{Has: tc.markers}), Providers: []locator.Provider{handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return "raw", true, nil })}})
			if err != nil {
				t.Fatal(err)
			}
			if got := value.(*readerSeedResult).Input.Jwt; got != tc.expected {
				t.Fatalf("Jwt=%q want %q", got, tc.expected)
			}
		})
	}
}

func TestReaderSeedRejectsUnsupportedNestedMutableGraph(t *testing.T) {
	rt, target, calls := readerSeedFixture(t)
	extra := &readerSeedInput{Auth: &readerSeedAuth{Context: map[string]int{"account": 10}, Unsupported: make(chan int)}, Has: &readerSeedMarkers{Auth: true}}
	_, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(extra)})
	if err == nil {
		t.Fatal("unsupported nested channel acquired trusted seed authority")
	}
	if *calls != 0 {
		t.Fatal("dependency executed after invalid seed graph")
	}
}
