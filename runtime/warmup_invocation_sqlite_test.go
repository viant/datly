package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	handlerexec "github.com/viant/xdatly/handler/exec"
	xpredicate "github.com/viant/xdatly/predicate"
)

type warmupAuthorizationPredicate struct {
	Invocation *handlerexec.InvocationInfo `bind:"kind=invocation,required"`
}

var warmupAuthorizationTrace struct {
	sync.Mutex
	phases []handlerexec.WarmupPhase
}

func (p *warmupAuthorizationPredicate) Compute(ctx context.Context, _ any) (*xpredicate.Criteria, error) {
	fromContext := handlerexec.InvocationFromContext(ctx)
	if p.Invocation == nil || p.Invocation != fromContext {
		return nil, fmt.Errorf("injected invocation does not match context invocation")
	}
	if !p.Invocation.MayBypassRowAuthorization() {
		return nil, fmt.Errorf("row authorization required")
	}
	warmupAuthorizationTrace.Lock()
	warmupAuthorizationTrace.phases = append(warmupAuthorizationTrace.phases, p.Invocation.WarmupPhase())
	warmupAuthorizationTrace.Unlock()
	return nil, nil
}

func TestRuntimeWarmupExposesInvocationToAuthorizationPredicate(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE secured_records(id INTEGER,tenant INTEGER)", "INSERT INTO secured_records VALUES(11,1)"); err != nil {
		t.Fatal(err)
	}
	type input struct {
		Tenant int
		Access string
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	required := true
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Name: "WarmupAuthorization"},
		Routes:   []*spec.Route{{Method: "GET", Path: "/secured-records"}},
		Settings: &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "secured-records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"1"}}}}}}}},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}},
			{Name: "Access", Source: spec.BindSource{Kind: "query", Name: "access"}, Predicates: []*spec.Predicate{{Name: "handler", Args: []string{"example.WarmupAuthorization"}, ApplyWhenAbsent: true}}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "secured-records", Source: &spec.ViewSource{SQL: `SELECT id FROM secured_records WHERE tenant=:Tenant ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")} ORDER BY id`}},
	}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(warmupAuthorizationPredicate{}), PkgPath: "example", Name: "WarmupAuthorization"}); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows", Types: types})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	warmupAuthorizationTrace.Lock()
	warmupAuthorizationTrace.phases = nil
	warmupAuthorizationTrace.Unlock()
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/secured-records"}}
	operation, err := runtime.NewWarmup(target)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := operation.Prepare(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = prepared.Run(ctx); err != nil {
		t.Fatal(err)
	}
	warmupAuthorizationTrace.Lock()
	phases := append([]handlerexec.WarmupPhase(nil), warmupAuthorizationTrace.phases...)
	warmupAuthorizationTrace.Unlock()
	if !reflect.DeepEqual(phases, []handlerexec.WarmupPhase{handlerexec.WarmupPhasePrepare, handlerexec.WarmupPhaseFill}) {
		t.Fatalf("predicate warmup phases = %v", phases)
	}
}
