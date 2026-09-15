package template

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/constant"
)

func TestInstanceConstantIdentifiers(t *testing.T) {
	type input struct {
		ID      int
		Project string
	}
	values, err := constant.New(map[string]string{"project": "shop-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"SELECT * FROM $project.ds.table WHERE id=$ID",
		"SELECT * FROM ${project}.ds.table WHERE id=$ID",
		"SELECT * FROM `$project.ds.table` WHERE id=$ID",
		"SELECT * FROM `${project}.ds.table` WHERE id=$ID",
		"SELECT * FROM [$project.ds.table] WHERE id=$ID",
		"SELECT * FROM [${project}.ds.table] WHERE id=$ID",
		"#if($ID > 0) SELECT * FROM `$project.ds.table` WHERE id=$ID #end",
	} {
		t.Run(sql, func(t *testing.T) {
			program, err := (Compiler{Source: sql, Const: values, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "ID", FieldIndex: []int{0}}, {Name: "project", FieldIndex: []int{1}}}}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{ID: 7, Project: "request-data"})})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(result.SQL, "shop-e2e.ds.table") || strings.Contains(result.SQL, "request-data") || !strings.Contains(result.SQL, ":ID") {
				t.Fatalf("SQL=%q args=%v", result.SQL, result.Args)
			}
		})
	}
}

func TestInstanceConstantsPreserveLiteralsAndTemplateDecisions(t *testing.T) {
	values, _ := constant.New(map[string]string{"project": "shop-e2e"})
	sql := "SELECT '$project.ds.table', \"$project.ds.table\" FROM `$project.ds.table` -- $project.ds.table\n WHERE id=:ID"
	got, err := (ConstantRenderer{Values: values}).render(sql)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT '$project.ds.table', \"$project.ds.table\" FROM `shop-e2e.ds.table` -- $project.ds.table\n WHERE id=:ID"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	for _, sql := range []string{"SELECT $project", "#if($project.Length > 0) SELECT :ID #end", "SELECT $Unsafe.Table", "SELECT $predicate.Filter($project)", "SELECT $View.NonWindowSQL", "#set($project = $ID) SELECT $project.ds.table"} {
		got, err := (ConstantRenderer{Values: values}).render(sql)
		if err != nil || got != sql {
			t.Fatalf("altered %q: %q %v", sql, got, err)
		}
	}
}

func TestInstanceIdentifierFailuresAreRegistrationErrors(t *testing.T) {
	for _, value := range []string{"", "x` WHERE 1=1 --", "x.ds", "x y", "$Other", "x;DELETE"} {
		values, _ := constant.New(map[string]string{"project": value})
		if _, err := (Compiler{Const: values, Source: "SELECT * FROM `$project.ds.records`"}).Compile(); err == nil {
			t.Fatalf("accepted identifier value %q", value)
		}
	}
}

func TestInstanceLiteralEvaluationKeepsAuthoredPlaceholders(t *testing.T) {
	type input struct{ ID int }
	values, _ := constant.New(map[string]string{"project": "e2e"})
	source := "SELECT '$project.ds.records' AS literal FROM `$project.ds.records` WHERE id=$ID -- $project.ds.records"
	compiler := Compiler{Const: values, Source: source, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "ID", FieldIndex: []int{0}}}}
	program, err := compiler.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{ID: 7})})
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT '$project.ds.records' AS literal FROM `e2e.ds.records` WHERE id=:ID -- $project.ds.records"
	if result.SQL != want {
		t.Fatalf("got %q want %q", result.SQL, want)
	}
	if compiler.Source != source {
		t.Fatal("compiler overwrote authored source")
	}
}

func TestMissingIdentifierConstantFailsBeforeEvaluation(t *testing.T) {
	values, _ := constant.New(map[string]string{})
	if _, err := (Compiler{Const: values, Source: "SELECT * FROM `$Missing.ds.records`"}).Compile(); err == nil {
		t.Fatal("missing identifier accepted")
	}
}

func TestInstanceDoesNotExpandRequestEmittedUnsafeText(t *testing.T) {
	type input struct{ Table string }
	values, _ := constant.New(map[string]string{"project": "e2e"})
	program, err := (Compiler{Const: values, Source: "SELECT * FROM $Unsafe.Table", InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "Table", FieldIndex: []int{0}}}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Table: "`$project.ds.records`"})})
	if err != nil {
		t.Fatal(err)
	}
	if result.SQL != "SELECT * FROM `$project.ds.records`" {
		t.Fatalf("request text was expanded: %q", result.SQL)
	}
}
