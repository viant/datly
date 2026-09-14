package criteria

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestTypedCompilerSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE typed(id INTEGER, name TEXT, active BOOLEAN, amount REAL, stamp TIMESTAMP)", "INSERT INTO typed VALUES(1,'ALICE',true,1.5,'2026-09-12 00:00:00+00:00')"); err != nil {
		t.Fatal(err)
	}
	c := Compiler{
		Columns: map[string]Column{
			"id":     {Expression: "id", Type: reflect.TypeOf(int8(0))},
			"name":   {Expression: "name", Type: reflect.TypeOf("")},
			"active": {Expression: "active", Type: reflect.TypeOf(false)},
			"amount": {Expression: "amount", Type: reflect.TypeOf(float32(0))},
			"stamp":  {Expression: "stamp", Type: reflect.TypeOf(time.Time{}), TimeLayout: "2006-01-02"},
		},
		Methods: map[string]Method{"upper": {Name: "upper", Args: []reflect.Type{reflect.TypeOf("")}}, "abs": {Name: "abs", Args: []reflect.Type{reflect.TypeOf(int8(0))}}},
	}
	for _, tt := range []struct {
		name, source string
		args         []any
		reject       bool
	}{
		{"integer", "id=1", []any{int8(1)}, false},
		{"float", "amount=1.5", []any{float32(1.5)}, false},
		{"bool", "active=true", []any{true}, false},
		{"time", "stamp='2026-09-12'", []any{time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)}, false},
		{"method", "name=upper('alice')", []any{"alice"}, false},
		{"numeric method", "id=abs(1)", []any{int8(1)}, false},
		{"overflow", "id=128", nil, true},
		{"wrong literal", "id='1'", nil, true},
		{"wrong operator", "active> false", nil, true},
		{"column type mismatch", "id=name", nil, true},
		{"bad time", "stamp='invalid'", nil, true},
		{"unknown method", "name=lower('ALICE')", nil, true},
		{"wrong arity", "name=upper('alice','bob')", nil, true},
		{"method wrong type", "name=upper(1)", nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := c.Compile(tt.source, nil)
			if tt.reject {
				if err == nil {
					t.Fatalf("expected rejection: %s", sql)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(args, tt.args) {
				t.Fatalf("args %#v expected %#v", args, tt.args)
			}
			type row struct {
				ID int `sqlx:"id"`
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM typed WHERE " + sql, Args: args}, []row{{ID: 1}})
		})
	}
}

func TestMethodValidation(t *testing.T) {
	for _, tt := range []struct {
		name  string
		valid bool
	}{{"upper", true}, {"schema.upper", true}, {" upper ", true}, {"upper) OR 1=1 --", false}, {"upper('x')", false}, {"upper(); SELECT 1", false}, {"", false}} {
		t.Run(tt.name, func(t *testing.T) {
			method := Method{Name: tt.name}
			err := method.Validate()
			if (err == nil) != tt.valid {
				t.Fatalf("Validate() %v valid %v", err, tt.valid)
			}
		})
	}
}
