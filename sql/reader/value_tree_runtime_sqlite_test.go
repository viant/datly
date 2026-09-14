package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

type valueGraphNode struct {
	ID          int              `sqlx:"id"`
	ParentID    *int             `sqlx:"parent_id"`
	Children    []valueGraphNode `sqlx:"-" self:"child=id,parent=parent_id"`
	Related     bool             `sqlx:"-"`
	OrderBroken bool             `sqlx:"-"`
}

func (n *valueGraphNode) OnRelation(context.Context) {
	if n.Related {
		n.OrderBroken = true
	}
	for _, child := range n.Children {
		if !child.Related {
			n.OrderBroken = true
		}
	}
	n.Related = true
}

type valueSelfGraphOutput struct {
	Rows  []valueGraphNode `parameter:"Rows,kind=output,in=view" view:"Nodes,limit=5000" sql:"SELECT id,parent_id FROM nodes ORDER BY id"`
	Count *graphCount      `parameter:"Count,kind=output,in=summary" view:"Count" sql:"SELECT COUNT(*) AS total FROM ($View.Nodes.NonWindowSQL) source"`
}

func TestValueSelfTreeLifecycleThroughRuntimeSQLite(t *testing.T) {
	for _, depth := range []int{1, 128, 1024} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE nodes(id INTEGER,parent_id INTEGER)", fmt.Sprintf("WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<%d) INSERT INTO nodes SELECT i,CASE WHEN i=1 THEN NULL ELSE i-1 END FROM n", depth)); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ValueTree"}, Name: "ValueTree", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}
			actual := (typedGraphFixture{db: db, component: component, output: reflect.TypeOf(valueSelfGraphOutput{})}).invoke(t, ctx).(*valueSelfGraphOutput)
			if actual.Count == nil || actual.Count.Total != depth {
				t.Fatalf("flat row summary %+v", actual.Count)
			}
			rows := actual.Rows
			for level := 1; level <= depth; level++ {
				if len(rows) != 1 || rows[0].ID != level || !rows[0].Related || rows[0].OrderBroken {
					t.Fatalf("value tree level%d %+v", level, rows)
				}
				rows = rows[0].Children
			}
			if len(rows) != 0 {
				t.Fatal("unexpected tree tail")
			}
		})
	}
}
