package provider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/compiler"
)

// A writer's independent Current graph shares its component identity, but its
// relation holders belong to the Current row, not the public response contract.
func TestIndependentViewClearsSameOwnerSelectorPresentation(t *testing.T) {
	for _, transport := range []string{"REST", "MCP"} {
		t.Run(transport, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT)", "INSERT INTO parents VALUES(1)", "INSERT INTO children VALUES(11,1,'Current child'),(12,1,NULL)"); err != nil {
				t.Fatal(err)
			}
			current := &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT id,parent_id,name FROM children WHERE $COLUMN_IN"}}}}}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "writer"}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Current"}}}, Views: []*spec.View{current}}
			bindings := []bindly.BindingSpec{{Path: "Rows", Name: "Rows", Location: bindstate.Location{Kind: "view", In: "Current"}}}
			seed, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			inputType := reflect.TypeFor[relationMetadataInput]()
			plan, err := seed.CompilePlan(inputType, bindings...)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := plan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			dependencies, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
			if err != nil {
				t.Fatal(err)
			}
			views, err := New(Config{Dependencies: dependencies, Input: projection, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			injector, err := seed.ForScope(views)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			if transport == "MCP" {
				target := exec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/writer"}}
				presentation := exec.NewSelectorPresentation(target, func(_ reflect.Type, path string) ([]exec.SelectorFieldName, error) {
					calls++
					if path == "Children" {
						return nil, fmt.Errorf("Current.Children does not belong to writer response")
					}
					return nil, nil
				})
				ctx, err = exec.EnterSelectorPresentation(ctx, target, presentation)
				if err != nil {
					t.Fatal(err)
				}
			}
			input := &relationMetadataInput{}
			if err := injector.Bind(ctx, input, bindly.WithPlan(plan), bindly.WithSource(input)); err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatalf("Current read consulted public output presentation %d times", calls)
			}
			if transport == "MCP" {
				// ValueInScope derives a private context; the public response
				// retains the exact original presentation and its error.
				_, err := exec.PresentedSelectorFields(ctx, component.Key, reflect.TypeFor[relationMetadataChild](), "Children")
				if err == nil || err.Error() != "Current.Children does not belong to writer response" || calls != 1 {
					t.Fatalf("parent output presentation changed: calls=%d err=%v", calls, err)
				}
			}

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			fresh, err := seed.ForScope(views)
			if err != nil {
				t.Fatal(err)
			}
			candidate := &relationMetadataInput{}
			err = fresh.Bind(cancelled, candidate, bindly.WithPlan(plan), bindly.WithSource(candidate))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Current read lost cancellation identity: %v", err)
			}

			if len(input.Rows) != 1 || len(input.Rows[0].Children) != 2 || !input.Rows[0].Complete {
				t.Fatalf("Current graph did not load its complete relations: %+v", input.Rows)
			}
		})
	}
}
