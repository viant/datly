package compile

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestInternalTransientMatchesExplicitTags(t *testing.T) {
	compile := func(annotation string) *spec.View {
		t.Helper()
		sql := "SELECT r.*, " + annotation + " FROM records r"
		view := &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "caller", Source: "caller", Type: spec.TypeRef{Name: "string"}}}}
		got, err := NewReader().Compile(ReadInput{View: view, SQL: sql})
		if err != nil {
			t.Fatal(err)
		}
		if view.Columns[0].Tag != "" {
			t.Fatal("source mutated")
		}
		return got
	}
	short := compile(`internal_transient(r.caller),tag(r.caller,'validate:"omitempty"')`)
	long := compile(`tag(r.caller,'sqlx:"-" diff:"-" internal:"true" json:"-" validate:"omitempty"')`)
	if !reflect.DeepEqual(short, long) {
		t.Fatalf("shorthand differs: %+v / %+v", short.Columns, long.Columns)
	}
	if strings.Contains(short.Source.SQL, "internal_transient") {
		t.Fatal("annotation leaked into SQL")
	}
	if got := compile(`internal_transient(r.caller),internal_transient(r.caller)`); got.Columns[0].Tag != `sqlx:"-" diff:"-" internal:"true" json:"-"` {
		t.Fatal(got.Columns[0].Tag)
	}
	public := compile(`tag(r.caller,'sqlx:"-"')`)
	if public.Columns[0].Tag != `sqlx:"-"` {
		t.Fatal("public logical field hidden")
	}
}

func TestInternalTransientRejectsConflictsBothOrders(t *testing.T) {
	for _, conflict := range []string{`sqlx:"caller"`, `diff:"caller"`, `internal:"false"`, `json:"caller"`} {
		for _, reverse := range []bool{false, true} {
			a, b := "internal_transient(r.caller)", fmt.Sprintf("tag(r.caller,'%s')", conflict)
			if reverse {
				a, b = b, a
			}
			sql := "SELECT r.*, " + a + ", " + b + " FROM records r"
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: sql})
			if err == nil || !strings.Contains(err.Error(), "conflict") {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
}

func TestInternalTransientRejectsInvalidPlacementAndTargets(t *testing.T) {
	for _, sql := range []string{
		`SELECT r.*,internal_transient() FROM records r`,
		`SELECT r.*,internal_transient(r.caller,r.id) FROM records r`,
		`SELECT r.*,internal_transient(caller) FROM records r`,
		`SELECT r.*,internal_transient(missing.caller) FROM records r`,
		`SELECT r.*,internal_transient(r.caller) AS caller FROM records r`,
		`SELECT r.id,internal_transient(r.caller) FROM records r`,
		`SELECT r.*,1+internal_transient(r.caller) FROM records r`,
		`SELECT r.* FROM records r WHERE internal_transient(r.caller)=1`,
		`SELECT r.*,CASE WHEN r.id=1 THEN internal_transient(r.caller) ELSE 0 END AS v FROM records r`,
		`SELECT r.* FROM (SELECT o.*,internal_transient(o.caller) FROM records o) r`,
		`WITH c AS (SELECT o.*,internal_transient(o.caller) FROM records o) SELECT r.* FROM c r`,
		`SELECT r.* FROM records r UNION SELECT o.*,internal_transient(o.caller) FROM records o`,
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records"}, SQL: sql})
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
	view := &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "a", Source: "caller"}, {Name: "b", Source: "caller"}}}
	_, err := NewReader().Compile(ReadInput{View: view, SQL: `SELECT r.*,internal_transient(r.caller) FROM records r`})
	if err == nil || !strings.Contains(err.Error(), "multiple canonical columns") {
		t.Fatal(err)
	}
}
