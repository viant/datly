package builder

import (
	"context"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	sqltemplate "github.com/viant/datly/sql/template"
	"reflect"
	"testing"
)

func TestRelationTemplateBindingsExecuteInSourceOrder(t *testing.T) {
	type input struct {
		Minimum int
		Enabled bool
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE relation_rows(id INTEGER,parent_id INTEGER,amount INTEGER)", "INSERT INTO relation_rows VALUES(1,7,10),(2,7,20),(3,9,30),(4,10,40)"); err != nil {
		t.Fatal(err)
	}
	evaluator, err := (sqltemplate.Compiler{Source: `#set($min = $Minimum) #set($enabled = $Enabled) SELECT id,parent_id FROM relation_rows WHERE amount >= $min AND $enabled`, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "ParentID", Column: "parent_id"}}}}
	for _, tc := range []struct {
		desc      string
		input     input
		expect    []int
		composite bool
	}{
		{"enabled scoped rows", input{20, true}, []int{2, 3}, false},
		{"disabled child", input{20, false}, nil, false},
		{"composite scoped rows", input{20, true}, []int{2, 3}, true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			options := []BuilderOption{WithBuilderSQL("unused"), WithBuilderInput(reflect.ValueOf(tc.input)), WithBuilderTemplate(evaluator)}
			if tc.composite {
				options = append(options, WithBuilderRelation(&data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "ParentID", Column: "parent_id"}, {Field: "ID", Column: "id"}}}}), WithBuilderCompositeArgs([]string{"parent_id", "id"}, [][]interface{}{{7, 2}, {9, 3}}))
			} else {
				options = append(options, WithBuilderRelation(relation), WithBuilderPositionalArgs([]any{7, 9}))
			}
			query, err := NewBuilder().Build(ctx, options...)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := h.DB.QueryContext(ctx, query.SQL, query.Args...)
			if err != nil {
				t.Fatalf("%s args=%v: %v", query.SQL, query.Args, err)
			}
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var r row
				var parent int
				if err := rows.Scan(&r.ID, &parent); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ids, tc.expect) {
				t.Fatalf("IDs=%v expected=%v SQL=%s args=%v", ids, tc.expect, query.SQL, query.Args)
			}
		})
	}
}
