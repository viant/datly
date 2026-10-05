package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type seedMarkers struct{ IDs, Missing, Scoped, Fresh, Capability, Opaque, Jwt bool }
type seedRecord struct {
	IDs        []int
	Missing    string
	Scoped     string
	Fresh      string
	Capability any
	Opaque     any
	Jwt        map[string]string
	Has        *seedMarkers `setMarker:"true"`
}
type seedReader struct{ calls int }

func (r *seedReader) Read(_ context.Context, input any, _ xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	r.calls++
	return input, nil
}

func seedContract(t *testing.T) (*registry.RegisteredComponent, *registry.RouteInputContract) {
	t.Helper()
	no := false
	specs := []bindly.BindingSpec{
		{Path: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}},
		{Path: "Missing", Location: bindstate.Location{Kind: "query", In: "missing"}},
		{Path: "Scoped", Location: bindstate.Location{Kind: "query", In: "scoped"}, Scope: "request"},
		{Path: "Fresh", Location: bindstate.Location{Kind: "query", In: "fresh"}, Cacheable: &no},
		{Path: "Capability", Location: bindstate.Location{Kind: "http_client", In: "client"}},
		{Path: "Opaque", Location: bindstate.Location{Kind: "body", In: "opaque"}},
		{Path: "Jwt", Location: bindstate.Location{Kind: "header", In: "authorization"}},
	}
	inj, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	typ := reflect.TypeFor[seedRecord]()
	plan, err := inj.CompilePlan(typ, specs...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "GET", Path: "/seed"}
	contract, err := registry.NewInputContract(typ, projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: specs})
	if err != nil {
		t.Fatal(err)
	}
	route, _ := contract.ForRoute(ref)
	return &registry.RegisteredComponent{Component: &spec.Component{}, Input: contract, Reader: &seedReader{}}, route
}

func TestSeedMarkedSelectionClonesAndExcludesCapabilities(t *testing.T) {
	registered, route := seedContract(t)
	input := &seedRecord{IDs: []int{1, 2}, Missing: "unmarked", Scoped: "scope", Fresh: "stale", Capability: make(chan int), Jwt: map[string]string{"sub": "trusted"}, Has: &seedMarkers{IDs: true, Scoped: true, Fresh: true, Capability: true, Jwt: true}}
	actual, paths, err := (&Runtime{}).prepareReaderSeed(context.Background(), registered, route, dexec.WithInput(input))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{"IDs", "Jwt"}) {
		t.Fatalf("paths=%v", paths)
	}
	detached := actual.(*seedRecord)
	if detached.Missing != "" || detached.Scoped != "" || detached.Fresh != "" || detached.Capability != nil || detached.Has != nil {
		t.Fatalf("excluded state copied: %#v", detached)
	}
	detached.IDs[0] = 9
	detached.Jwt["sub"] = "changed"
	if input.IDs[0] != 1 || input.Jwt["sub"] != "trusted" {
		t.Fatal("seed aliases caller")
	}
}

func TestSeedRejectsWrongTypeAndUnsupportedSelectedGraph(t *testing.T) {
	registered, route := seedContract(t)
	for _, input := range []any{nil, (*seedRecord)(nil), seedRecord{}, &struct{ IDs []int }{}, &seedRecord{Opaque: make(chan int), Has: &seedMarkers{Opaque: true}},
		&seedRecord{Opaque: &seedReader{}, Has: &seedMarkers{Opaque: true}},
		&seedRecord{Opaque: map[string]any{"nested": []any{func() {}}}, Has: &seedMarkers{Opaque: true}},
	} {
		if _, _, err := (&Runtime{}).prepareReaderSeed(context.Background(), registered, route, dexec.WithInput(input)); err == nil {
			t.Fatalf("accepted %T", input)
		}
	}
	empty := &seedRecord{IDs: []int{7}}
	actual, paths, err := (&Runtime{}).prepareReaderSeed(context.Background(), registered, route, dexec.WithInput(empty))
	if err != nil || len(paths) != 0 || len(actual.(*seedRecord).IDs) != 0 {
		t.Fatalf("unmarked input selected: %#v %v %v", actual, paths, err)
	}
}

func TestSeedAdmissionRejectsExecutionOverrides(t *testing.T) {
	registered, _ := seedContract(t)
	for name, request := range map[string]dexec.ComponentRequest{
		"input": {Input: &seedRecord{}}, "replay": {Replay: &bindly.ReplayBinding{}}, "warmup": {Warmup: &dexec.ReaderWarmupRequest{}}, "prepare": {PrepareQuery: true}, "dry": {DryRun: true}, "independent": {IndependentChildTransactions: true},
	} {
		t.Run(name, func(t *testing.T) {
			if validateSeedAdmission(request, registered) == nil {
				t.Fatal("override accepted")
			}
		})
	}
	registered.Handler = customhandler.NewFunc[seedRecord, seedRecord](nil)
	if validateSeedAdmission(dexec.ComponentRequest{}, registered) == nil {
		t.Fatal("custom handler accepted")
	}
	registered.Handler = nil
	registered.Component.Settings = &spec.Settings{Mutation: "insert"}
	if validateSeedAdmission(dexec.ComponentRequest{}, registered) == nil {
		t.Fatal("writer accepted")
	}
	encoded, _ := json.Marshal(dexec.ComponentRequest{ExtraInput: dexec.WithInput(&seedRecord{IDs: []int{1}})})
	if strings.Contains(string(encoded), "ExtraInput") || strings.Contains(string(encoded), "IDs") {
		t.Fatalf("seed exposed: %s", encoded)
	}
}

// Exercise canonical binding and the actual reader: only selected values skip
// transport resolution, while missing fields bind and presence is re-established.
func TestSeedRuntimeCanonicalReaderBinding(t *testing.T) {
	type markers struct{ Name, Other bool }
	type input struct {
		Name, Other string
		Has         *markers `setMarker:"true"`
	}
	component := componentSpec("SeedReader", http.MethodGet, "/seed-reader", []*spec.Parameter{
		{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string", Required: boolSeed(true)},
		{Name: "Other", Source: spec.BindSource{Kind: "query", Name: "other"}, TypeExpr: "string"},
	})
	artifact := componentArtifact(t, component, reflect.TypeFor[input](), reflect.TypeFor[input]())
	reader := &seedReader{}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[input](), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	request := testharness.NewRequest("GET", "/seed-reader").WithQuery(url.Values{"name": {"transport"}, "other": {"bound"}})
	scope, err := request.Scope()
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	source := &input{Name: "trusted", Has: &markers{Name: true}}
	actual, err := runtime.invokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-reader"}}, ExtraInput: dexec.WithInput(source)}, scope)
	if err != nil {
		t.Fatal(err)
	}
	got := actual.(*input)
	if got.Name != "trusted" || got.Other != "bound" || got.Has == nil || !got.Has.Name || !got.Has.Other || reader.calls != 1 {
		t.Fatalf("canonical binding not preserved: %#v calls%d", got, reader.calls)
	}
	if source.Other != "" || source.Has.Other {
		t.Fatal("binding mutated caller")
	}
	blankScope, err := testharness.NewRequest("GET", "/seed-reader").Scope()
	if err != nil {
		t.Fatal(err)
	}
	defer blankScope.Close()
	_, err = runtime.invokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/seed-reader"}}, ExtraInput: dexec.WithInput(&input{})}, blankScope)
	if err == nil || reader.calls != 1 {
		t.Fatalf("missing required value reached reader: %v calls%d", err, reader.calls)
	}

}

func boolSeed(value bool) *bool { return &value }

func TestSeedCanonicalValidationAndConditions(t *testing.T) {
	type marker struct{ Rows bool }
	type input struct {
		Rows    []int
		Enabled bool
		Has     *marker `setMarker:"true"`
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	maximum := 1
	bindings := []bindly.BindingSpec{{Path: "Rows", Location: bindstate.Location{Kind: "query", In: "rows"}, When: "Enabled", MaxAllowedRecords: &maximum}}
	plan, err := injector.CompilePlan(reflect.TypeFor[input](), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "GET", Path: "/validation"}
	contract, err := registry.NewInputContract(reflect.TypeFor[input](), projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	route, _ := contract.ForRoute(ref)
	registered := &registry.RegisteredComponent{Component: &spec.Component{}, Input: contract, Reader: &seedReader{}}
	seed, paths, err := (&Runtime{}).prepareReaderSeed(context.Background(), registered, route, dexec.WithInput(&input{Rows: []int{1, 2}, Has: &marker{Rows: true}}))
	if err != nil {
		t.Fatal(err)
	}
	target := &input{Enabled: true}
	err = injector.Bind(context.Background(), target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolvedInput(plan, seed, paths...))
	if err == nil {
		t.Fatal("seed bypassed maximum record validation")
	}
	target = &input{Enabled: false}
	if err = injector.Bind(context.Background(), target, bindly.WithPlan(plan), bindly.WithSource(target), bindly.WithResolvedInput(plan, seed, paths...)); err != nil {
		t.Fatal(err)
	}
	if len(target.Rows) != 0 || target.Has != nil && target.Has.Rows {
		t.Fatal("seed bypassed condition or forged presence")
	}
}
