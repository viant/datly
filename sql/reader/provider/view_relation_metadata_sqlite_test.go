package provider

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/compiler"
	xhandler "github.com/viant/xdatly/handler"
)

type relationMetadataChild struct {
	ID       int     `sqlx:"id"`
	ParentID int     `sqlx:"parent_id"`
	Name     *string `sqlx:"name"`
	Unloaded string  `sqlx:"-"`
}
type relationMetadataParent struct {
	ID       int
	Children []*relationMetadataChild `sqlx:"-"`
	Complete bool                     `sqlx:"-"`
}

func (p *relationMetadataParent) OnRelation(context.Context) {
	p.Complete = len(p.Children) == 2
	if p.Complete {
		p.Children[0], p.Children[1] = p.Children[1], p.Children[0]
	}
}

type relationMetadataInput struct{ Rows []*relationMetadataParent }

func TestViewProviderRelationProjectionObservesCompletedBatchesSQLite(t *testing.T) {
	for _, includeName := range []bool{true, false} {
		t.Run(map[bool]string{true: "loaded NULL", false: "omitted Name"}[includeName], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT)", "INSERT INTO parents VALUES(1),(2)", "INSERT INTO children VALUES(11,1,'first'),(12,1,NULL),(21,2,'third'),(22,2,NULL)"); err != nil {
				t.Fatal(err)
			}
			selection := "id,parent_id"
			if includeName {
				selection += ",name"
			}
			allowNulls := true
			current := &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: "SELECT id FROM parents ORDER BY id"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: &spec.View{Name: "children", AllowNulls: &allowNulls, BatchSize: 1, BatchConcurrency: 2, Source: &spec.ViewSource{SQL: "SELECT " + selection + " FROM children WHERE $COLUMN_IN ORDER BY parent_id,id"}}}}}
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "view", Name: "Current"}}}, Views: []*spec.View{current}}
			bindings := []bindly.BindingSpec{{Path: "Rows", Name: "Rows", Location: bindstate.Location{Kind: "view", In: "Current"}}}
			seed, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			inputType := reflect.TypeOf(relationMetadataInput{})
			inputPlan, err := seed.CompilePlan(inputType, bindings...)
			if err != nil {
				t.Fatal(err)
			}
			inputProjection, err := inputPlan.Projection()
			if err != nil {
				t.Fatal(err)
			}
			dependencies, err := compiler.CompileViewDependencies(compiler.Input{Component: component, InputType: inputType, Bindings: bindings})
			if err != nil {
				t.Fatal(err)
			}
			views, err := New(Config{Dependencies: dependencies, Input: inputProjection, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			injector, err := seed.ForScope(views)
			if err != nil {
				t.Fatal(err)
			}
			input := &relationMetadataInput{}
			observations := 0
			err = injector.Bind(ctx, input, bindly.WithPlan(inputPlan), bindly.WithBindingObserver(func(_ context.Context, event bindly.BindingEvent) error {
				if event.Target != input || event.Path != "Rows" {
					return fmt.Errorf("unexpected observation target")
				}
				observations++
				projection, ok := event.Metadata.(xhandler.ReadProjection)
				if !ok {
					return fmt.Errorf("missing real read projection")
				}
				rows, ok := event.Value.([]*relationMetadataParent)
				if !ok || len(rows) != 2 {
					return fmt.Errorf("unexpected typed parents %T", event.Value)
				}
				for rootOrdinal, parent := range rows {
					if !parent.Complete || len(parent.Children) != 2 || parent.Children[0].ID != parent.ID*10+2 {
						return fmt.Errorf("observer ran before complete relation hooks")
					}
					if _, err := projection.Fields(rootOrdinal); err == nil {
						return fmt.Errorf("opaque value-root storage was treated as stable identity")
					}
					for childOrdinal, child := range parent.Children {
						fields, err := projection.Fields(rootOrdinal, xhandler.ReadStep{Holder: "Children", Index: childOrdinal})
						if err != nil {
							return err
						}
						if !fields.Has("ID") || !fields.Has("ParentID") || fields.Has("Name") != includeName || fields.Has("Unloaded") {
							return fmt.Errorf("relation projection does not describe actual SQL columns")
						}
						if child.ParentID != parent.ID || childOrdinal == 0 && child.Name != nil {
							return fmt.Errorf("typed child/NULL result differs")
						}
					}
				}
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if observations != 1 {
				t.Fatalf("observations=%d", observations)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM children"}, []struct{ Count int }{{4}})
		})
	}
}
