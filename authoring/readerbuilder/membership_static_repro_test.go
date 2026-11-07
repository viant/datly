package readerbuilder_test

import (
	"context"
	"database/sql"
	"github.com/viant/datly/sql/template"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/authoring/readerbuilder"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestMembershipRuntimeAndStaticContract(t *testing.T) {
	source := `#package('example.com/membership')
#setting($_ = $internal(true))
#setting($_ = $route('/membership','GET'))
#define($_ = $Members<?>(view/Members).Optional().Cardinality('Many').WithConnector('fixture').WithLimit(5).WithColumnType('Name','*string') /* SELECT name AS Name FROM facts GROUP BY name LIMIT 5 */)
#set($_ = $Data<?>(output/view))
SELECT result.label,CAST(result.label AS *string)
FROM (SELECT CASE WHEN (#if($Members)#foreach($member in $Members)(f.name = $member.Name OR (f.name IS NULL AND $member.Name IS NULL))#if($foreach.HasNext) OR #end#end#else FALSE#end) THEN f.name ELSE 'Other' END AS label FROM facts f) result`
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE facts(name TEXT)"); err != nil {
		t.Fatal(err)
	}
	_, err = transcribe.NewCompiler().RuntimeContractsInModule(context.Background(), "example.com", &transcribe.Source{Name: "Membership", Scope: "example.com/membership", Text: source, Connector: "fixture", Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"fixture": db})})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("runtime compiler passed")
	result := readerbuilder.New(readerbuilder.Config{Name: "Membership", Types: typecatalog.NewCatalog()}).Apply(context.Background(), readerbuilder.Request{DQL: source, Operation: readerbuilder.Operation{Type: readerbuilder.OperationInspect}})
	if len(result.Diagnostics) > 0 {
		for _, d := range result.Diagnostics {
			t.Logf("%s: %s", d.Code, d.Message)
		}
		t.Fatal("static inspection rejected same runtime-valid membership SQL")
	}
}

func TestMembershipPredicateRuntimeValues(t *testing.T) {
	type member struct{ Name *string }
	type input struct {
		Members      []member
		Alternates   []member
		UseAlternate bool
	}
	const predicate = `SELECT CASE WHEN (#if($Members)#foreach($member in $Members)(f.name = $member.Name OR (f.name IS NULL AND $member.Name IS NULL))#if($foreach.HasNext) OR #end#end#else FALSE#end) THEN 'Member' ELSE 'Other' END AS label FROM (SELECT 'alpha' AS name) f`
	program, err := (template.Compiler{Source: predicate, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alpha, beta := "alpha", "beta"
	for _, tc := range []struct {
		name    string
		members []member
		want    string
	}{
		{"nil", nil, "Other"}, {"empty", []member{}, "Other"}, {"one match", []member{{&alpha}}, "Member"}, {"one miss", []member{{&beta}}, "Other"}, {"list", []member{{&beta}, {nil}, {&alpha}}, "Member"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := program.Evaluate(context.Background(), template.Invocation{Input: reflect.ValueOf(input{Members: tc.members})})
			if err != nil {
				t.Fatal(err)
			}
			var actual string
			if err = db.QueryRow(result.SQL, result.Args...).Scan(&actual); err != nil {
				t.Fatalf("%s: %v", result.SQL, err)
			}
			if actual != tc.want {
				t.Fatalf("got %s, want %s", actual, tc.want)
			}
		})
	}
	t.Run("branch bindings", func(t *testing.T) {
		source := `#if($UseAlternate)#set($selected=$Alternates)#else#set($selected=$Members)#end` + strings.ReplaceAll(predicate, "$Members", "$selected")
		branchProgram, err := (template.Compiler{Source: source, InputType: reflect.TypeFor[input]()}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		for _, useAlternate := range []bool{false, true} {
			result, err := branchProgram.Evaluate(context.Background(), template.Invocation{Input: reflect.ValueOf(input{Members: []member{{&beta}}, Alternates: []member{{&alpha}}, UseAlternate: useAlternate})})
			if err != nil {
				t.Fatal(err)
			}
			var actual string
			if err = db.QueryRow(result.SQL, result.Args...).Scan(&actual); err != nil {
				t.Fatal(err)
			}
			want := "Other"
			if useAlternate {
				want = "Member"
			}
			if actual != want {
				t.Fatalf("alternate %t: got %s, want %s", useAlternate, actual, want)
			}
		}
	})

}
