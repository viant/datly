package transcribe

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly/provider/body"
	"github.com/viant/datly/internal/testfixture/castmodel"
	"github.com/viant/datly/internal/testharness/sqlite"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/reader"
	rcompile "github.com/viant/datly/sql/reader/compiler"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
)

func TestScalarPlaceholderReaderWriterSQLite(t *testing.T) {
	for _, present := range []bool{true, false} {
		t.Run(map[bool]string{true: "present", false: "omitted"}[present], func(t *testing.T) {
			t.Run("string default", func(t *testing.T) { checkScalarPlaceholder[string](t, "''", "", "string", present) })
			t.Run("int default", func(t *testing.T) { checkScalarPlaceholder[int](t, "0", "", "int", present) })
			t.Run("int CAST", func(t *testing.T) { checkScalarPlaceholder[int](t, "0", ",CAST(r.value AS int)", "int", present) })
			t.Run("computed CAST", func(t *testing.T) {
				checkScalarPlaceholder[int](t, "coalesce(o.cap,0)", ",CAST(r.value AS int)", "int", present)
			})
			t.Run("pointer CAST", func(t *testing.T) { checkScalarPlaceholder[*int](t, "0", ",CAST(r.value AS *int)", "*int", present) })
		})
	}
}

func checkScalarPlaceholder[T castmodel.ScalarValue](t *testing.T, literal, cast, wantType string, present bool) {
	t.Helper()
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,unit TEXT,cap INTEGER)`, `INSERT INTO records VALUES(1,'days',7)`); err != nil {
		t.Fatal(err)
	}
	text := `#setting($_ = $route('/records','GET'))
SELECT r.*` + cast + `,tag(r.value,'sqlx:"-"') FROM (SELECT o.*, ` + literal + ` AS value FROM records o) r`
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: text})
	if err != nil {
		t.Fatal(err)
	}
	generated, err := generate.New(generate.Input{Component: compiled.Component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := generated.Views[0].Field("Value")
	if !ok || field.Type != wantType {
		t.Fatalf("field=%+v", field)
	}
	type output struct{ Rows []*castmodel.Scalar[T] }
	plan, err := rcompile.Compile(rcompile.Input{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.Read(ctx, &struct{}{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := result.(*output).Rows
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	switch v := any(rows[0].Value).(type) {
	case int:
		if v != 7 {
			t.Fatal(v)
		}
	case *int:
		if v == nil || *v != 7 {
			t.Fatal(v)
		}
	case string:
		if v != "days" {
			t.Fatal(v)
		}
	}
	request := `{"id":1}`
	unit, cap := "days", 7
	if present {
		if wantType == "string" {
			request = `{"id":1,"value":""}`
			unit = ""
		} else {
			request = `{"id":1,"value":0}`
			cap = 0
		}
	}
	provider, err := body.New([]byte(request), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := provider.Value(ctx, reflect.TypeOf(castmodel.Scalar[T]{}), "")
	if err != nil || !found {
		t.Fatalf("body: %v", err)
	}
	row := value.(castmodel.Scalar[T])
	if err := row.Init(ctx); err != nil {
		t.Fatal(err)
	}
	data := dml.NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer data.Complete(ctx, context.Canceled)
	if row.Has.Cap || row.Has.Unit {
		if err := data.Update("records", &row); err != nil {
			t.Fatal(err)
		}
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT unit,cap FROM records WHERE id=1"}, []struct {
		Unit string
		Cap  int
	}{{unit, cap}})
}

func TestLiteralDefaultsMappedReaderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER)`, `INSERT INTO records VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	const text = `#setting($_ = $route('/records','GET'))
SELECT r.* FROM (SELECT id,'' AS message,0 AS number FROM records) r`
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: text})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID      int    `sqlx:"id"`
		Message string `sqlx:"message"`
		Number  int    `sqlx:"number"`
	}
	type output struct{ Rows []*row }
	plan, err := rcompile.Compile(rcompile.Input{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.Read(ctx, &struct{}{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := result.(*output).Rows
	if len(rows) != 1 || rows[0].ID != 1 || rows[0].Message != "" || rows[0].Number != 0 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestOpaqueCTECASTReaderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	const inner = `WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<2) SELECT sum(n) AS value FROM numbers`
	text := `#setting($_ = $route('/numbers','GET'))
SELECT r.*,CAST(r.value AS int) FROM (` + inner + `) r`
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Numbers", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: text})
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		Value int `sqlx:"value"`
	}
	type output struct{ Rows []*row }
	plan, err := rcompile.Compile(rcompile.Input{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: compiled.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.Read(ctx, &struct{}{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := result.(*output).Rows
	if len(rows) != 1 || rows[0].Value != 3 {
		t.Fatalf("rows=%+v", rows)
	}
}
