package reader

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/collector"
	xreader "github.com/viant/xdatly/reader"
)

type budgetChild struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
	TenantID int `sqlx:"tenant_id"`
}
type budgetParent struct {
	ID, TenantID int
	Children     []*budgetChild
}

type budgetPartitions struct{}

func (budgetPartitions) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return []xreader.Partition{{Expression: "parent_id <= ?", Args: []any{2}}, {Expression: "parent_id > ?", Args: []any{2}}}, nil
}

type budgetReducingPartitions struct {
	budgetPartitions
	calls *int
}

func (p *budgetReducingPartitions) Reducer(context.Context) xreader.Reducer { return p }
func (p *budgetReducingPartitions) Reduce(_ context.Context, rows any) (any, error) {
	*p.calls++
	return rows, nil
}

func TestRelationBatchesRespectBoundParameterBudgetSQLite(t *testing.T) {
	for _, test := range []struct {
		name                       string
		size, concurrency, maximum int
		composite, fail            bool
		partition, reduced         bool
	}{
		{"scalar_fixed_args", 0, 0, 3, false, false, false, false},
		{"composite_fixed_args", 0, 0, 3, true, false, false, false},
		{"configured_concurrent", 3, 2, 3, true, false, false, false},
		{"cannot_fit_tuple", 0, 0, 2, true, true, false, false},
		{"partition_fixed_args", 0, 0, 0, true, false, true, false},
		{"reducer_adaptive", 0, 0, 0, true, false, true, true},
		{"reducer_configured_batches", 1, 2, 0, true, false, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE children(id INTEGER,parent_id INTEGER,tenant_id INTEGER,enabled INTEGER)", "INSERT INTO children VALUES(11,1,1,1),(12,2,1,1),(13,3,1,1),(21,1,2,1),(22,2,2,1),(23,3,2,1)"); err != nil {
				t.Fatal(err)
			}
			childView := &data.View{Spec: spec.View{Name: "Children", BatchSize: test.size, BatchConcurrency: test.concurrency, Source: &spec.ViewSource{SQL: "SELECT id,parent_id,tenant_id FROM children WHERE $COLUMN_IN AND enabled=:Enabled ORDER BY id"}}}
			parentView := &data.View{Spec: spec.View{Name: "Parents"}}
			relation := &data.Relation{Holder: "Children", Cardinality: spec.CardinalityMany, On: data.Links{{Column: "id", Field: "ID"}}, Of: &data.RelationRef{View: childView, On: data.Links{{Column: "parent_id", Field: "ParentID"}}, MatchStrategy: data.MatchSequential}}
			if test.composite {
				relation.On = append(relation.On, &data.Link{Column: "tenant_id", Field: "TenantID"})
				relation.Of.On = append(relation.Of.On, &data.Link{Column: "tenant_id", Field: "TenantID"})
			}
			parentView.Relations = []*data.Relation{relation}
			graph, err := collector.Compile(parentView, map[*data.View]reflect.Type{parentView: reflect.TypeFor[*budgetParent](), childView: reflect.TypeFor[*budgetChild]()})
			if err != nil {
				t.Fatal(err)
			}
			parents := []*budgetParent{}
			root := collector.NewCollector(graph.Root, &parents, false)
			visitor := root.Visitor(ctx)
			var scalar []interface{}
			var tuples [][]interface{}
			for id := 1; id <= 3; id++ {
				parent := root.NewItem()().(*budgetParent)
				parent.ID = id
				parent.TenantID = 1
				if err := visitor(parent); err != nil {
					t.Fatal(err)
				}
				scalar = append(scalar, id)
				tuples = append(tuples, []interface{}{id, 1})
			}
			root.Fetched()
			child := root.Relations(nil)[0]
			sqlComponent := &dsql.SQLComponent{DB: db.DB}
			connection, err := sqlComponent.Resolve(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			dialect := *connection.Dialect
			if test.maximum > 0 {
				dialect.MaxPlaceholders = test.maximum
			}
			connection.Dialect = &dialect
			var enabled any = 1
			plan := &ViewPlan{View: childView, Collector: graph.View(childView)}
			reductions := 0
			if test.partition {
				values := make([]int, dialect.MaxPlaceholderCount()-4)
				for index := range values {
					values[index] = 1
				}
				enabled = values
				childView.Spec.Source.SQL = "SELECT id,parent_id,tenant_id FROM children WHERE $COLUMN_IN AND enabled IN (:Enabled) ORDER BY id"
				childView.Spec.Partitioning = &spec.Partitioning{Type: "budget", Concurrency: 2}
				plan.Partitioner = budgetPartitions{}
				if test.reduced {
					plan.Partitioner = &budgetReducingPartitions{calls: &reductions}
				}
			}
			reader := &relationRead{service: NewService(), ctx: ctx, input: reflect.ValueOf(&struct{}{}), child: child, plan: plan, connection: connection,
				session: &Session{SQL: sqlComponent, Parameters: func(name string) (any, bool, error) { return enabled, name == "Enabled", nil }},
			}
			if test.composite {
				err = reader.Read(nil, tuples, []string{"parent_id", "tenant_id"})
			} else {
				err = reader.Read(scalar, nil, nil)
			}
			if test.fail {
				if reductions != 0 {
					t.Fatal("oversized batch invoked reducer")
				}
				var limit *parameterLimitError
				if !errors.As(err, &limit) {
					t.Fatalf("budget error=%v", err)
				}
				for _, parent := range parents {
					if len(parent.Children) != 0 {
						t.Fatal("failed budget published children")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.reduced && reductions != 1 {
				t.Fatalf("reducer calls=%d want1", reductions)
			}
			for _, parent := range parents {
				want := 2
				if test.composite {
					want = 1
				}
				if len(parent.Children) != want {
					t.Fatalf("parent%d children=%+v want%d", parent.ID, parent.Children, want)
				}
				for _, item := range parent.Children {
					if item.ParentID != parent.ID || test.composite && item.TenantID != 1 {
						t.Fatal("tuple matching crossed identities")
					}
				}
			}
		})
	}
}

func TestParameterBudgetUsesDialectLimit(t *testing.T) {
	budget := parameterBudget{}
	maximum := budget.dialect.MaxPlaceholderCount()
	if err := budget.check(maximum); err != nil {
		t.Fatal(err)
	}
	if err := budget.check(maximum + 1); err == nil {
		t.Fatal(fmt.Sprintf("accepted %d parameters", maximum+1))
	}
}
