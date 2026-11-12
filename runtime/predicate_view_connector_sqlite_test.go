package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xhandler "github.com/viant/xdatly/handler"
	xpredicate "github.com/viant/xdatly/predicate"
)

type lookupScopeRow struct {
	Tenant int `sqlx:"tenant_id"`
}
type lookupScopePredicate struct {
	Binder xhandler.Binder `bind:"kind=binder,required"`
}

func (p *lookupScopePredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	canonical, present, err := p.Binder.Lookup(ctx, xhandler.InputKey)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, fmt.Errorf("canonical predicate input unavailable")
	}
	var dependency struct {
		Input any `bind:"kind=input,required"`
	}
	if err := p.Binder.Bind(ctx, &dependency); err != nil {
		return nil, err
	}
	if dependency.Input != canonical {
		return nil, fmt.Errorf("injected binder lost canonical input")
	}
	rows, ok := value.([]lookupScopeRow)
	if !ok {
		return nil, fmt.Errorf("typed lookup rows required")
	}
	if len(rows) == 0 {
		return &xpredicate.Criteria{Expression: "1=0"}, nil
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("ambiguous lookup")
	}
	return &xpredicate.Criteria{Expression: "r.tenant_id = ?", Placeholders: []any{rows[0].Tenant}}, nil
}

type lazyLookupInput struct {
	Actor int                 `parameter:"Actor,kind=query,in=actor,required" predicate:"handler,applyWhenAbsent=true,example.LazyLookupScope"`
	Scope []lookupScopeRow    `parameter:"Scope,kind=view,in=Scope,scope=authorization"`
	Has   *lazyLookupInputHas `setMarker:"true"`
}
type lazyLookupInputHas struct{ Actor, Scope bool }
type lazyLookupScopePredicate struct {
	Binder xhandler.Binder `bind:"kind=binder,required"`
}

func (p *lazyLookupScopePredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	canonical, found, err := p.Binder.Lookup(ctx, xhandler.InputKey)
	if err != nil || !found {
		return nil, fmt.Errorf("canonical input lookup: %w", err)
	}
	input, ok := canonical.(*lazyLookupInput)
	if !ok {
		return nil, fmt.Errorf("unexpected canonical input %T", canonical)
	}
	if input.Actor == 0 {
		return &xpredicate.Criteria{Expression: "1=0"}, nil
	}
	binder, ok := p.Binder.(xhandler.ScopedBinder)
	if !ok {
		return nil, fmt.Errorf("named scope binder unavailable")
	}
	if err := binder.BindScope(ctx, input, "authorization"); err != nil {
		return nil, err
	}
	return (&lookupScopePredicate{Binder: p.Binder}).Compute(ctx, input.Scope)
}

// This fixture proves native view binding and connector selection, not a
// production identity policy. A real scope lookup requires verified identity.
func TestRegularReaderPredicateUsesDedicatedConnectorView(t *testing.T) {
	t.Run("eager", func(t *testing.T) { testReaderPredicateView(t, false) })
	t.Run("lazy", func(t *testing.T) { testReaderPredicateView(t, true) })
}
func testReaderPredicateView(t *testing.T, lazy bool) {
	ctx := context.Background()
	facts, directory := testharness.NewSQLiteHarness(t), testharness.NewSQLiteHarness(t)
	if err := facts.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES(1,7),(2,7),(3,8)"); err != nil {
		t.Fatal(err)
	}
	if err := directory.ExecStatements(ctx, "CREATE TABLE scope_rows(actor INTEGER, tenant_id INTEGER)", "INSERT INTO scope_rows VALUES(10,7),(20,8)"); err != nil {
		t.Fatal(err)
	}
	type input struct {
		Actor int              `parameter:"Actor,kind=query,in=actor,required"`
		Scope []lookupScopeRow `parameter:"Scope,kind=view,in=Scope" predicate:"handler,applyWhenAbsent=true,example.LookupScope"`
	}
	inputType := reflect.TypeFor[input]()
	if lazy {
		inputType = reflect.TypeFor[lazyLookupInput]()
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	component := componentSpec("ScopedRecords", "GET", "/scoped-records", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	component.RootView = &spec.View{Source: &spec.ViewSource{SQL: `SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")} ORDER BY r.id`, Bindings: &spec.ViewBindings{Connector: "facts"}}}
	component.Views = []*spec.View{{Name: "Scope", Source: &spec.ViewSource{SQL: "SELECT tenant_id FROM scope_rows WHERE actor = :Actor", Bindings: &spec.ViewBindings{Connector: "directory"}}}}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[lookupScopePredicate](), PkgPath: "example", Name: "LookupScope"}); err != nil {
		t.Fatal(err)
	}
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[lazyLookupScopePredicate](), PkgPath: "example", Name: "LazyLookupScope"}); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[output](), DirectViewField: "Data", Types: types})
	if err != nil {
		t.Fatal(err)
	}
	sources := &dsql.SQLComponent{}
	if err = sources.RegisterConnector("facts", facts.DB); err != nil {
		t.Fatal(err)
	}
	if err = sources.RegisterConnector("directory", directory.DB); err != nil {
		t.Fatal(err)
	}
	views, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: sources})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: inputType, OutputType: reflect.TypeFor[output](), Plan: artifact.Reader, SQL: sources})
	if err != nil {
		t.Fatal(err)
	}
	var scopeReads, factReads atomic.Int32
	service, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Reader: reader, Providers: []locator.Provider{views}}}, WithObservability(ObservabilityConfig{ReadingData: func(_ string, _ time.Duration, query string, _ int, _ []any, _ error) {
		if strings.Contains(query, "scope_rows") {
			scopeReads.Add(1)
		} else if strings.Contains(query, "records") {
			factReads.Add(1)
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Shutdown(ctx) })
	for _, tc := range []struct {
		actor string
		want  []row
	}{{"10", []row{{1}, {2}}}, {"20", []row{{3}}}, {"30", []row{}}, {"0", []row{}}} {
		t.Run(tc.actor, func(t *testing.T) {
			beforeScope, beforeFacts := scopeReads.Load(), factReads.Load()
			actual, err := executeTestRoute(t, service, ctx, testharness.NewRequest(http.MethodGet, "/scoped-records").WithQuery(url.Values{"actor": {tc.actor}}))
			if err != nil {
				t.Fatal(err)
			}
			got := actual.(*output).Data
			if len(got) != len(tc.want) || len(got) > 0 && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			wantScope := int32(1)
			if lazy && tc.actor == "0" {
				wantScope = 0
			}
			if scopeReads.Load()-beforeScope != wantScope || factReads.Load()-beforeFacts != 1 {
				t.Fatalf("expected one scope and one fact read; got %d, %d", scopeReads.Load()-beforeScope, factReads.Load()-beforeFacts)
			}
		})
	}
	if lazy {
		for _, actor := range []int{10, 20, 30, 0} {
			beforeScope, beforeFacts := scopeReads.Load(), factReads.Load()
			seed := &lazyLookupInput{Actor: actor, Scope: []lookupScopeRow{{Tenant: 999}}, Has: &lazyLookupInputHas{Actor: true, Scope: true}}
			actual, err := service.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/scoped-records"}}, Input: seed})
			if err != nil {
				t.Fatalf("typed actor %d: %v", actor, err)
			}
			want := []row{}
			if actor == 10 {
				want = []row{{1}, {2}}
			} else if actor == 20 {
				want = []row{{3}}
			}
			got := actual.(*output).Data
			if len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
				t.Fatalf("typed actor %d returned %v, want %v", actor, got, want)
			}
			wantScope := int32(1)
			if actor == 0 {
				wantScope = 0
			}
			if scopeReads.Load()-beforeScope != wantScope || factReads.Load()-beforeFacts != 1 {
				t.Fatalf("typed actor %d repeated or eagerly bound scope: scope=%d facts=%d", actor, scopeReads.Load()-beforeScope, factReads.Load()-beforeFacts)
			}
		}
	}
	if err := directory.ExecStatements(ctx, "DROP TABLE scope_rows"); err != nil {
		t.Fatal(err)
	}
	beforeFacts := factReads.Load()
	if _, err := executeTestRoute(t, service, ctx, testharness.NewRequest(http.MethodGet, "/scoped-records").WithQuery(url.Values{"actor": {"10"}})); err == nil {
		t.Fatal("scope lookup failure must abort the protected read")
	}
	if factReads.Load() != beforeFacts {
		t.Fatal("fact query ran after scope lookup failed")
	}
}
