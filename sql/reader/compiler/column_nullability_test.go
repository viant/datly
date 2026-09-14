package compiler

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
	"github.com/viant/datly/sql/reader"
)

func TestReconcileColumnsPreservesPointerNulls(t *testing.T) {
	type Number int
	type Fields struct {
		Pointer *Number `sqlx:"pointer"`
	}
	type row struct {
		Fields
		Scalar Number `sqlx:"scalar"`
	}
	typed, err := columnsFromType(reflect.TypeOf(row{}))
	if err != nil {
		t.Fatal(err)
	}
	canonical := []*data.Column{
		{Name: "Pointer", Column: "pointer", Nullable: true, NullFallback: "0", Groupable: true},
		{Name: "Scalar", Column: "scalar", Nullable: true, NullFallback: "0"},
	}
	for pass := 0; pass < 2; pass++ {
		typed = reconcileColumns(typed, canonical)
		if !typed[0].Nullable || typed[0].NullFallback != "" || !typed[0].Groupable {
			t.Fatalf("pointer = %+v", typed[0])
		}
		if !typed[1].Nullable || typed[1].NullFallback != "0" {
			t.Fatalf("scalar = %+v", typed[1])
		}
	}
	if canonical[0].NullFallback != "0" {
		t.Fatal("canonical metadata was mutated")
	}
}

func TestColumnPointerNullProjectionSQLite(t *testing.T) {
	type row struct {
		ID     int      `sqlx:"id"`
		Number *int     `sqlx:"number"`
		Text   *string  `sqlx:"text"`
		Flag   *bool    `sqlx:"flag"`
		Amount *float64 `sqlx:"amount"`
		Scalar int      `sqlx:"scalar"`
	}
	type output struct{ Data []*row }
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		"CREATE TABLE records(id INTEGER,number INTEGER,text TEXT,flag BOOLEAN,amount REAL,scalar INTEGER)",
		"INSERT INTO records VALUES(1,1,'text',true,1.5,1),(2,NULL,NULL,NULL,NULL,NULL),(3,0,'',false,0,0),(4,NULL,NULL,NULL,NULL,NULL)",
	); err != nil {
		t.Fatal(err)
	}
	one, zero, text, empty, yes, no, amount, zeroAmount := 1, 0, "text", "", true, false, 1.5, 0.0
	want := []*row{{1, &one, &text, &yes, &amount, 1}, {ID: 2}, {3, &zero, &empty, &no, &zeroAmount, 0}, {ID: 4}}
	for _, source := range []struct {
		name  string
		value *spec.ViewSource
	}{
		{"table", &spec.ViewSource{Table: "records"}},
		{"query", &spec.ViewSource{SQL: "SELECT id,number,text,flag,amount,scalar FROM records ORDER BY id"}},
	} {
		t.Run(source.name, func(t *testing.T) {
			component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "Records", Source: source.value, Columns: []*spec.Column{
				{Name: "ID", Source: "id", Type: spec.TypeRef{Name: "int"}},
				{Name: "Number", Source: "number", Type: spec.TypeRef{Name: "int"}, Nullable: true},
				{Name: "Text", Source: "text", Type: spec.TypeRef{Name: "string"}, Nullable: true},
				{Name: "Flag", Source: "flag", Type: spec.TypeRef{Name: "bool"}, Nullable: true},
				{Name: "Amount", Source: "amount", Type: spec.TypeRef{Name: "float64"}, Nullable: true},
				{Name: "Scalar", Source: "scalar", Type: spec.TypeRef{Name: "int"}, Nullable: true, ExplicitType: true},
			}}}
			plan, err := Compile(Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Data"})
			if err != nil {
				t.Fatal(err)
			}
			query, err := builder.NewBuilder().Build(ctx, builder.WithBuilderView(plan.Root.View), builder.WithBuilderInput(reflect.ValueOf(struct{}{})), builder.WithBuilderSQL(source.value.SQL))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(query.SQL, "COALESCE") != 1 || !strings.Contains(strings.ToLower(query.SQL), "coalesce(scalar, 0)") {
				t.Fatalf("pointer/scalar SQL policy: %s", query.SQL)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, want)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				result, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(result.(*output).Data, want) {
					t.Fatalf("pass %d: got %+v want %+v", pass, result.(*output).Data, want)
				}
			}
		})
	}
}
