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

func TestRuntimeAuthoredWarmupSQLite(t *testing.T) {
	type input struct{ Tenant int }
	type row struct {
		ID int `sqlx:"id"`
	}
	type summary struct {
		Total int `sqlx:"total"`
	}
	type output struct {
		Rows    []row
		Summary *summary
	}
	for _, tc := range []struct {
		name    string
		max     int
		values  []string
		fail    bool
		summary bool
	}{{"all_cases", 0, []string{"1", "2"}, false, false}, {"budget", 1, []string{"1", "2"}, false, false}, {"invalid_source", 0, []string{"invalid"}, true, false}, {"summary_cases", 0, []string{"1", "2"}, false, true}, {"summary_budget", 1, []string{"1", "2"}, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant INTEGER)", "INSERT INTO records VALUES(11,1),(22,2)"); err != nil {
				t.Fatal(err)
			}
			required := true
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{MaxCases: &tc.max, Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: tc.values}}}}}}},
				Parameters: []*spec.Parameter{{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
				RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:Tenant ORDER BY id"}},
			}
			if tc.summary {
				component.Settings.Cache.Warmup.IndexMeta = true
				component.RootView.Relations = []*spec.Relation{{Name: "summary", Holder: "Summary", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent"}}}}
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
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
			target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}
			count, err := runtime.Warmup(ctx, target)
			if tc.fail {
				if err == nil {
					t.Fatal("invalid authored value was accepted")
				}
				db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records ORDER BY id"}, []row{{11}, {22}})
				return
			}
			wantCount := 2
			cost := 1
			if tc.summary {
				cost = 2
			}
			if tc.max > 0 {
				wantCount = tc.max / cost
			}
			if err != nil || count != wantCount*cost {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			for tenant := 1; tenant <= 2; tenant++ {
				actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: []locator.Provider{values.New("query", map[string]any{"tenant": tenant})}})
				if tenant > wantCount {
					if err == nil {
						t.Fatal("unwarmed case unexpectedly replayed")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(actual.(*output).Rows, []row{{tenant * 11}}) {
					t.Fatalf("rows=%+v", actual)
				}
				if tc.summary && (actual.(*output).Summary == nil || actual.(*output).Summary.Total != 1) {
					t.Fatalf("summary=%+v", actual)
				}
			}
		})
	}
}
