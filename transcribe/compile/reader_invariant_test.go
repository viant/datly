package compile

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReaderInvariantDeclarations(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		columns   []*spec.Column
		want      string
	}{
		{name: "shared group", sql: `SELECT r.*,invariant(r.start,'Window'),invariant(r.end,'Window') FROM (SELECT o.* FROM records o) r`},
		{name: "case and whitespace", sql: `SELECT r.*,INVARIANT(r.start,' Window ') FROM (SELECT o.* FROM records o) r`},
		{name: "source alias", sql: `SELECT r.*,invariant(r.start,'Window'),invariant(r.begin,'Window') FROM (SELECT o.* FROM records o) r`, columns: []*spec.Column{{Name: "begin", Source: "start", Tag: `json:"begin"`}}},
		{name: "tag then invariant", sql: `SELECT r.*,tag(r.start,'invariant:"Window"'),invariant(r.start,'Window') FROM (SELECT o.* FROM records o) r`},
		{name: "invariant then tag", sql: `SELECT r.*,invariant(r.start,'Window'),tag(r.start,'invariant:"Window"') FROM (SELECT o.* FROM records o) r`},
		{name: "missing args", sql: `SELECT r.*,invariant() FROM (SELECT o.* FROM records o) r`, want: "requires a column"},
		{name: "one arg", sql: `SELECT r.*,invariant(r.start) FROM (SELECT o.* FROM records o) r`, want: "requires a column"},
		{name: "too many", sql: `SELECT r.*,invariant(r.start,'Window','Other') FROM (SELECT o.* FROM records o) r`, want: "requires a column"},
		{name: "empty", sql: `SELECT r.*,invariant(r.start,'') FROM (SELECT o.* FROM records o) r`, want: "non-empty string literal"},
		{name: "blank", sql: `SELECT r.*,invariant(r.start,'  ') FROM (SELECT o.* FROM records o) r`, want: "non-empty string literal"},
		{name: "identifier group", sql: `SELECT r.*,invariant(r.start,Window) FROM (SELECT o.* FROM records o) r`, want: "string literal"},
		{name: "numeric group", sql: `SELECT r.*,invariant(r.start,1) FROM (SELECT o.* FROM records o) r`, want: "string literal"},
		{name: "comma groups", sql: `SELECT r.*,invariant(r.start,'Window,Other') FROM (SELECT o.* FROM records o) r`, want: "multiple groups"},
		{name: "space groups", sql: `SELECT r.*,invariant(r.start,'Window Other') FROM (SELECT o.* FROM records o) r`, want: "multiple groups"},
		{name: "unqualified", sql: `SELECT r.*,invariant(start,'Window') FROM (SELECT o.* FROM records o) r`, want: "qualified view column"},
		{name: "literal target", sql: `SELECT r.*,invariant('r.start','Window') FROM (SELECT o.* FROM records o) r`, want: "qualified view column"},
		{name: "expression target", sql: `SELECT r.*,invariant(r.start+1,'Window') FROM (SELECT o.* FROM records o) r`, want: "qualified view column"},
		{name: "star target", sql: `SELECT r.*,invariant(r.*,'Window') FROM (SELECT o.* FROM records o) r`, want: "qualified view column"},
		{name: "inner table alias", sql: `SELECT r.*,invariant(o.start,'Window') FROM (SELECT o.* FROM records o) r`, want: "no canonical view"},
		{name: "unknown view", sql: `SELECT r.*,invariant(other.start,'Window') FROM (SELECT o.* FROM records o) r`, want: "no canonical view"},
		{name: "metadata name is not namespace", sql: `SELECT r.*,invariant(Records.start,'Window') FROM (SELECT o.* FROM records o) r`, want: "no canonical view"},
		{name: "ambiguous source", sql: `SELECT r.*,invariant(r.start,'Window') FROM (SELECT o.* FROM records o) r`, columns: []*spec.Column{{Name: "a", Source: "start"}, {Name: "b", Source: "start"}}, want: "multiple canonical columns"},
		{name: "conflict", sql: `SELECT r.*,invariant(r.start,'Window'),invariant(r.start,'Other') FROM (SELECT o.* FROM records o) r`, want: "conflicting invariant values"},
		{name: "tag conflict", sql: `SELECT r.*,tag(r.start,'invariant:"Other"'),invariant(r.start,'Window') FROM (SELECT o.* FROM records o) r`, want: "conflicting invariant values"},
		{name: "existing conflict", sql: `SELECT r.*,invariant(r.start,'Window') FROM (SELECT o.* FROM records o) r`, columns: []*spec.Column{{Name: "start", Tag: `invariant:"Other"`}}, want: "conflicting invariant values"},
		{name: "annotation alias", sql: `SELECT r.*,invariant(r.start,'Window') AS start FROM (SELECT o.* FROM records o) r`, want: "without an alias"},
		{name: "entire projection", sql: `SELECT invariant(r.start,'Window') FROM (SELECT o.* FROM records o) r`, want: "entire SELECT projection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tc.sql}, Columns: tc.columns}
			before := input.Clone()
			got, err := NewReader().Compile(ReadInput{View: input, SQL: tc.sql})
			if !reflect.DeepEqual(input, before) {
				t.Fatal("declaration mutated authored metadata")
			}
			if tc.want != "" {
				var compileErr *Error
				if !errors.As(err, &compileErr) || compileErr.Code != CodeViewDirective || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error=%v; want %s: %s", err, CodeViewDirective, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(strings.ToLower(got.Source.SQL), "invariant(") {
				t.Fatalf("annotation leaked: %s", got.Source.SQL)
			}
			if len(got.Columns) == 0 {
				t.Fatal("missing annotated columns")
			}
			for _, column := range got.Columns {
				if reflect.StructTag(column.Tag).Get("invariant") != "Window" {
					t.Fatalf("column=%+v", column)
				}
			}
			if tc.name == "shared group" && len(got.Columns) != 2 {
				t.Fatalf("columns=%+v", got.Columns)
			}
			if tc.name == "source alias" && (len(got.Columns) != 1 || reflect.StructTag(got.Columns[0].Tag).Get("json") != "begin") {
				t.Fatalf("alias metadata=%+v", got.Columns)
			}
		})
	}
}

func TestReaderInvariantRelatedView(t *testing.T) {
	const SQL = `SELECT r.*,c.*,invariant(c.start,'Window'),invariant(c.end,'Window') FROM (SELECT o.* FROM records o) r JOIN (SELECT c.* FROM children c) c ON c.parent_id=r.id`
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	child := got.Relations[0].View
	if len(got.Columns) != 0 || len(child.Columns) != 2 {
		t.Fatalf("wrong target: root=%+v child=%+v", got.Columns, child.Columns)
	}
	for _, column := range child.Columns {
		if reflect.StructTag(column.Tag).Get("invariant") != "Window" {
			t.Fatalf("column=%+v", column)
		}
	}
	for _, view := range []*spec.View{got, child} {
		if strings.Contains(strings.ToLower(view.Source.SQL), "invariant(") {
			t.Fatalf("annotation leaked: %s", view.Source.SQL)
		}
	}
}

func TestReaderInvariantNamedDerivedViews(t *testing.T) {
	const SQL = `SELECT orders.*, vendor.*, invariant(orders.WINDOW_START, 'DeliveryWindow'), invariant(orders.WINDOW_END, 'DeliveryWindow') FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT v.* FROM (VENDOR) v) vendor ON vendor.ID=orders.VENDOR_ID`
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 2 || len(got.Relations) != 1 || got.Namespace != "orders" {
		t.Fatalf("view=%+v", got)
	}
	for i, name := range []string{"WINDOW_START", "WINDOW_END"} {
		if column := got.Columns[i]; column.Name != name || reflect.StructTag(column.Tag).Get("invariant") != "DeliveryWindow" {
			t.Fatalf("column=%+v", column)
		}
	}
	vendor := got.Relations[0].View
	if len(vendor.Columns) != 0 || !strings.Contains(vendor.Source.SQL, "(VENDOR)") {
		t.Fatalf("vendor source/metadata changed: %+v / %+v", vendor.Source, vendor.Columns)
	}
	for _, view := range []*spec.View{got, vendor} {
		if strings.Contains(strings.ToLower(view.Source.SQL), "invariant(") {
			t.Fatalf("annotation leaked: %s", view.Source.SQL)
		}
	}
}

func TestReaderInvariantSQLBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		valid     bool
	}{
		{"projected SQL alias", `SELECT orders.*,invariant(orders.START_AT,'DeliveryWindow') FROM (SELECT CAST(o.WINDOW_START AS INTEGER) AS START_AT FROM ORDERS o) orders`, true},
		{"database cast", `SELECT orders.*,invariant(orders.WINDOW_START,'DeliveryWindow') FROM (SELECT CAST(o.WINDOW_START AS INTEGER) AS WINDOW_START FROM ORDERS o) orders`, true},
		{"nested annotation", `SELECT orders.* FROM (SELECT o.*,invariant(o.WINDOW_START,'DeliveryWindow') FROM ORDERS o) orders`, false},
		{"nested outer target", `SELECT orders.* FROM (SELECT o.*,invariant(orders.WINDOW_START,'DeliveryWindow') FROM ORDERS o) orders`, false},
		{"nested view control and invariant", `SELECT orders.* FROM (SELECT o.*,set_limit(o,10),invariant(o.WINDOW_START,'DeliveryWindow') FROM ORDERS o) orders`, false},
		{"deep nested annotation", `SELECT orders.* FROM (SELECT nested.* FROM (SELECT o.*,invariant(o.WINDOW_START,'DeliveryWindow') FROM ORDERS o) nested) orders`, false},
		{"cte annotation", `WITH source AS (SELECT o.*,invariant(o.WINDOW_START,'DeliveryWindow') FROM ORDERS o) SELECT orders.* FROM source orders`, false},
		{"expression annotation", `SELECT orders.*,coalesce(invariant(orders.WINDOW_START,'DeliveryWindow'),0) FROM (SELECT o.* FROM ORDERS o) orders`, false},
		{"where annotation", `SELECT orders.* FROM (SELECT o.* FROM ORDERS o) orders WHERE invariant(orders.WINDOW_START,'DeliveryWindow')=1`, false},
		{"inner where annotation", `SELECT orders.* FROM (SELECT o.* FROM ORDERS o WHERE invariant(o.WINDOW_START,'DeliveryWindow')=1) orders`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: tc.sql}}, SQL: tc.sql})
			if !tc.valid {
				var compileErr *Error
				if !errors.As(err, &compileErr) || compileErr.Code != CodeViewDirective || !strings.Contains(err.Error(), "invariant") {
					t.Fatalf("expected annotation boundary error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got.Source.SQL, "CAST(o.WINDOW_START AS INTEGER)") {
				t.Fatalf("database CAST lost: %s", got.Source.SQL)
			}
			if strings.Contains(strings.ToLower(got.Source.SQL), "invariant(") {
				t.Fatalf("annotation leaked: %s", got.Source.SQL)
			}
		})
	}
}

// These cases require native AST fidelity; do not add a Datly text parser to
// compensate for tokens or CASE expressions missing from sqlparser's AST.
func TestReaderInvariantNativeParserRegressions(t *testing.T) {
	for _, tc := range []struct{ name, SQL, blocker string }{
		{"trailing comma", `SELECT orders.*,invariant(orders.WINDOW_START,'DeliveryWindow',) FROM (SELECT o.* FROM ORDERS o) orders`, "sqlparser list.go:57 parseCallArgs accepts an absent operand after comma"},
		{"trailing argument text", `SELECT orders.*,invariant(orders.WINDOW_START,'DeliveryWindow' extra) FROM (SELECT o.* FROM ORDERS o) orders`, "sqlparser operand.go:177 parseCallArguments does not require full consumption"},
		{"multiple string tokens", `SELECT orders.*,invariant(orders.WINDOW_START,'DeliveryWindow' 'Other') FROM (SELECT o.* FROM ORDERS o) orders`, "sqlparser operand.go:177 parseCallArguments drops trailing string tokens"},
		{"CASE annotation", `SELECT orders.*,CASE WHEN orders.ID=1 THEN invariant(orders.WINDOW_START,'DeliveryWindow') ELSE 0 END AS invalid FROM (SELECT o.* FROM ORDERS o) orders`, "sqlparser operand.go:89 emits CASE as expr.Switch.Raw without Cases"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: tc.SQL}}, SQL: tc.SQL})
			if err == nil {
				t.Fatal("malformed or misplaced invariant was accepted")
			}
		})
	}
}

func TestReaderInvariantRequiresProjectedOutput(t *testing.T) {
	for _, SQL := range []string{
		`SELECT orders.*, invariant(orders.MISSING,'Window') FROM (SELECT o.ID FROM ORDERS o) orders`,
		`SELECT orders.*, invariant(orders.WINDOW_START,'Window') FROM (SELECT o.WINDOW_START AS START_AT FROM ORDERS o) orders`,
		`SELECT orders.ID, invariant(orders.WINDOW_START,'Window') FROM (SELECT o.* FROM ORDERS o) orders`,
		`SELECT orders.*, invariant(vendor.ID,'Window') FROM (SELECT o.* FROM ORDERS o) orders JOIN (SELECT v.* FROM (VENDOR) v) vendor ON vendor.ID=orders.VENDOR_ID`,
		`WITH projected AS (SELECT o.ID FROM ORDERS o) SELECT orders.*, invariant(orders.MISSING,'Window') FROM projected orders`,
	} {
		t.Run(SQL, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: SQL}}, SQL: SQL})
			var compileErr *Error
			if !errors.As(err, &compileErr) || compileErr.Code != CodeViewDirective || !strings.Contains(err.Error(), "absent from the SQL projection") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
