package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestRuntimeIndexedWarmupSQLite(t *testing.T) {
	type input struct{ ID int }
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	for _, tc := range []struct {
		name string
		id   int
		want []row
	}{{"first", 11, []row{{11}}}, {"second", 22, []row{{22}}}, {"absent", 33, []row{}}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(11),(22)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
				Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}}},
				Parameters: []*spec.Parameter{{Name: "ID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
				RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE (:ID=0 OR id=:ID) ORDER BY id"}},
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
			if count, err := runtime.Warmup(ctx, target); err != nil || count != 2 {
				t.Fatalf("Warmup=%d,%v", count, err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			bound := &input{ID: tc.id}
			actual, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: bound})
			if err != nil {
				t.Fatal(err)
			}
			rows := actual.(*output).Rows
			if len(rows) != 0 || len(tc.want) != 0 {
				if !reflect.DeepEqual(rows, tc.want) {
					t.Fatalf("rows=%v,want %v", rows, tc.want)
				}
			}
			if bound.ID != tc.id {
				t.Fatal("cache identity preparation mutated canonical input")
			}
		})
	}
}
