package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestRuntimeWarmupUsesDedicatedConnectorSQLite(t *testing.T) {
	type input struct{ Tenant int }
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	ctx := context.Background()
	regular, reserved := sqlite.New(t), sqlite.New(t)
	if err := regular.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES(11,1),(22,2)"); err != nil {
		t.Fatal(err)
	}
	if err := reserved.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES(101,1),(202,2)"); err != nil {
		t.Fatal(err)
	}
	required := true
	component := &spec.Component{
		Key:        spec.Key{Kind: spec.KindComponent, Name: "ReservedWarmup"},
		Routes:     []*spec.Route{{Method: "GET", Path: "/reserved-records"}},
		Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{Connector: "reservation", Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"1"}}}}}}}},
		Parameters: []*spec.Parameter{{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
		RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{Bindings: &spec.ViewBindings{Connector: "on_demand"}, SQL: "SELECT id FROM records WHERE tenant=:Tenant ORDER BY id"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	sources := &dsql.SQLComponent{DB: regular.DB}
	if err := sources.RegisterConnector("on_demand", regular.DB); err != nil {
		t.Fatal(err)
	}
	if err := sources.RegisterConnector("reservation", reserved.DB); err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: sources})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/reserved-records"}}
	if count, err := runtime.Warmup(ctx, target); err != nil || count != 1 {
		t.Fatalf("Warmup = %d, %v", count, err)
	}
	// Removing the reservation table makes any unintended fallback observable.
	if err := reserved.ExecStatements(ctx, "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	assertRows := func(tenant, want int) {
		t.Helper()
		actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: []locator.Provider{values.New("query", map[string]any{"tenant": tenant})}})
		if err != nil {
			t.Fatal(err)
		}
		if got := actual.(*output).Rows; !reflect.DeepEqual(got, []row{{want}}) {
			t.Fatalf("tenant %d: rows = %v, want %d", tenant, got, want)
		}
	}
	assertRows(1, 101) // Warmup selected the reservation source, not regular row 11.
	assertRows(2, 22)  // Unwarmed key selected the regular source, not reserved row 202.
	if err := regular.ExecStatements(ctx, "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	assertRows(1, 101)
	assertRows(2, 22) // Lazy fill remains available alongside the warmup entry.
}
