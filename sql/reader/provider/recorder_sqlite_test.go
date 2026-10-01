package provider

import (
	"context"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/compiler"
	"reflect"
	"testing"
	"time"
)

func TestDetachedViewRecorderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first'),(2,NULL)"); err != nil {
		t.Fatal(err)
	}
	bindings := []bindly.BindingSpec{{Path: "Rows", Name: "Rows", Location: bindstate.Location{Kind: "view", In: "Current"}}}
	seed, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	inputType := reflect.TypeFor[providerMetadataInput]()
	plan, err := seed.CompilePlan(inputType, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	nullable := true
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Current"}}}, Views: []*spec.View{{Name: "Current", AllowNulls: &nullable, Source: &spec.ViewSource{SQL: "SELECT id,name FROM records ORDER BY id"}}}}
	deps, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	source, err := New(Config{Dependencies: deps, Input: projection, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	counts := [2]int{}
	callback := func(index int) observability.ReadingData {
		return func(_ string, _ time.Duration, query string, rows int, args []any, readErr error) {
			if readErr != nil || rows != 2 || query == "" || len(args) != 0 {
				t.Errorf("query observation rows%d err%v", rows, readErr)
			}
			counts[index]++
		}
	}
	a := source.(*viewProvider).WithRecorder(observability.NewRecorder(nil, observability.WithReadingData(callback(0))))
	b := source.(*viewProvider).WithRecorder(observability.NewRecorder(nil, observability.WithReadingData(callback(1))))
	for _, provided := range []*viewProvider{a.(*viewProvider), b.(*viewProvider), source.(*viewProvider), a.(*viewProvider)} {
		scope, err := seed.ForScope(provided)
		if err != nil {
			t.Fatal(err)
		}
		input := new(providerMetadataInput)
		if err := scope.Bind(ctx, input, bindly.WithPlan(plan)); err != nil {
			t.Fatal(err)
		}
		if len(input.Rows) != 2 || input.Rows[0].ID != 1 || input.Rows[1].Name != nil {
			t.Fatalf("typed/null rows changed: %+v", input.Rows)
		}
	}
	if counts != [2]int{2, 1} {
		t.Fatalf("recorders contaminated each other or source: %v", counts)
	}
}
