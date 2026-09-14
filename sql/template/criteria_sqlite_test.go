package template

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xpredicate "github.com/viant/xdatly/predicate"
)

func TestCriteriaFragmentsExecuteSQLite(t *testing.T) {
	type ids struct{ Values []int }
	type input struct {
		IDs    *ids
		Names  []string
		Filter *xpredicate.IntFilter
		Flag   bool
		Zero   int
		Empty  string
	}
	type row struct{ ID int }
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT,active BOOLEAN)", "INSERT INTO records VALUES(1,'alpha',0),(2,'beta',1),(3,'alpha beta',0)"); err != nil {
		t.Fatal(err)
	}
	value := input{IDs: &ids{Values: []int{1, 3}}, Names: []string{"alpha", "beta"}, Filter: &xpredicate.IntFilter{Include: []int{1, 3}, Exclude: []int{2}}}
	for _, test := range []struct {
		name, where string
		input       input
		want        []row
		args        []any
	}{
		{"nested identifiers", `$criteria.In("id",$IDs.Values)`, value, []row{{1}, {3}}, []any{1, 3}},
		{"excluded identifiers", `$criteria.NotIn("id",$IDs.Values)`, value, []row{{2}}, []any{1, 3}},
		{"filter include", `$criteria.In("id",$Filter)`, value, []row{{1}, {3}}, []any{1, 3}},
		{"filter exclude", `$criteria.NotIn("id",$Filter)`, value, []row{{1}, {3}}, []any{2}},
		{"nil filter", `$criteria.In("id",$Filter)`, input{}, nil, nil},
		{"nil exclude", `$criteria.NotIn("id",$Filter)`, input{}, []row{{1}, {2}, {3}}, nil},
		{"empty include", `$criteria.In("id",$IDs.Values)`, input{IDs: &ids{}}, nil, nil},
		{"nil nested identifiers", `$criteria.In("id",$IDs.Values)`, input{}, nil, nil},
		{"like", `$criteria.Like("name",$Names)`, value, []row{{1}, {2}}, []any{"alpha", "beta"}},
		{"contains", `$criteria.Contains("name",$Names)`, value, []row{{1}, {2}, {3}}, []any{"%alpha%", "%beta%"}},
		{"not like", `$criteria.NotLike("name",$Names)`, value, []row{{3}}, []any{"alpha", "beta"}},
		{"not contains", `$criteria.NotContains("name",$Names)`, value, nil, []any{"%alpha%", "%beta%"}},
		{"false expression", `$criteria.Expression("active = ?",$Flag)`, value, []row{{1}, {3}}, []any{false}},
		{"zero expression", `$criteria.Expression("active = ?",$Zero)`, value, []row{{1}, {3}}, []any{0}},
		{"empty expression", `1=1 $criteria.Expression("AND name = ?",$Empty)`, value, []row{{1}, {2}, {3}}, nil},
		{"mixed loop fragments", `#foreach($id in $IDs.Values) #if($foreach.Index > 0) OR #end ($criteria.Expression("id = ?",$id) AND $criteria.Expression("active = ?",$Flag)) #end`, value, []row{{1}, {3}}, []any{1, false, 3, false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			program, err := (Compiler{Source: "SELECT id FROM records WHERE " + test.where + " ORDER BY id", InputType: reflect.TypeOf(input{})}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			if program == nil {
				t.Fatal("criteria-only template was not detected")
			}
			result, err := program.Evaluate(ctx, Invocation{Input: reflect.ValueOf(test.input)})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Args, test.args) {
				t.Fatalf("args=%#v want=%#v SQL=%s", result.Args, test.args, result.SQL)
			}
			db.AssertQuery(t, ctx, sqlite.Query{SQL: result.SQL, Args: result.Args}, test.want)
		})
	}
}

func TestCriteriaProtectedSQLAndUnknownMethod(t *testing.T) {
	for _, text := range []string{`SELECT '$criteria.In("id",$IDs)'`, "SELECT 1 -- $criteria.In(\"id\",$IDs)\n", `SELECT $$${criteria.In("id",$IDs)}$$`} {
		program, err := (Compiler{Source: text, InputType: reflect.TypeOf(struct{}{})}).Compile()
		if err != nil || program != nil {
			t.Fatalf("protected SQL=%s program=%v error=%v", text, program, err)
		}
	}
	program, err := (Compiler{Source: `#if(true) SELECT '$criteria.In("id",$IDs)' AS literal WHERE $criteria.In("id",$IDs) #end`, InputType: reflect.TypeOf(struct{ IDs []int }{})}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(struct{ IDs []int }{[]int{1}})})
	if err != nil || !strings.Contains(result.SQL, `'$criteria.In("id",$IDs)'`) || !reflect.DeepEqual(result.Args, []any{1}) {
		t.Fatalf("protected mixed result=%+v err=%v", result, err)
	}
	if _, err := (Compiler{Source: `SELECT $criteria.Unknown("id")`, InputType: reflect.TypeOf(struct{}{})}).Compile(); err == nil {
		t.Fatal("unknown criteria method accepted")
	}
}
