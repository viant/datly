package compile

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestOuterMutationMarkers(t *testing.T) {
	for _, name := range []string{"delete_marker", "concurrency_token"} {
		for _, tc := range []struct {
			label, projection, inner string
			fail                     bool
		}{
			{"qualified", "r.*," + name + "(r.flag)", "SELECT id, '' AS flag FROM records", false},
			{"qualified cast", "r.*,CAST(r.flag AS bool)," + name + "(r.flag)", "SELECT id, '' AS flag FROM records", false},
			{"missing column", "r.*," + name + "(r.missing)", "SELECT id, '' AS flag FROM records", true},
			{"cast cannot invent", "r.*,CAST(r.missing AS bool)," + name + "(r.missing)", "SELECT id, '' AS flag FROM records", true},
			{"duplicate", "r.*," + name + "(r.flag)," + name + "(r.id)", "SELECT id, '' AS flag FROM records", true},
			{"unqualified", "r.*," + name + "(flag)", "SELECT id, '' AS flag FROM records", true},
			{"wrong scope", "r.*," + name + "(records.flag)", "SELECT id, '' AS flag FROM records", true},
			{"literal", "r.*," + name + "('r.flag')", "SELECT id, '' AS flag FROM records", true},
			{"expression", "r.*," + name + "(r.flag+1)", "SELECT id, '' AS flag FROM records", true},
			{"no args", "r.*," + name + "()", "SELECT id, '' AS flag FROM records", true},
			{"extra args", "r.*," + name + "(r.flag,1)", "SELECT id, '' AS flag FROM records", true},
			{"aliased", "r.*," + name + "(r.flag) AS f", "SELECT id, '' AS flag FROM records", true},
			{"nested call", "r.*,COALESCE(" + name + "(r.flag),false)", "SELECT id, '' AS flag FROM records", true},
			{"inner SQL", "r.*", "SELECT id," + name + "(flag) FROM records", true},
		} {
			t.Run(name+"/"+tc.label, func(t *testing.T) {
				sql := "SELECT " + tc.projection + " FROM (" + tc.inner + ") r"
				input := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}
				before := input.Clone()
				got, err := NewReader().Compile(ReadInput{View: input, SQL: sql})
				if !reflect.DeepEqual(before, input) {
					t.Fatal("source metadata mutated")
				}
				if (err != nil) != tc.fail {
					t.Fatalf("error=%v", err)
				}
				if err != nil {
					return
				}
				found := false
				for _, column := range got.Columns {
					found = found || name == "delete_marker" && column.DeleteMarker || name == "concurrency_token" && column.ConcurrencyToken
				}
				if !found || strings.Contains(strings.ToLower(got.Source.SQL), name+"(") {
					t.Fatal("canonical annotation missing or SQL leak")
				}
			})
		}
	}
}

func TestColumnAnnotationDiagnosticNames(t *testing.T) {
	for _, annotation := range []string{"invariant", "delete_marker", "concurrency_token"} {
		call := annotation + "(r.flag)"
		if annotation == "invariant" {
			call = annotation + "(r.flag,'Group')"
		}
		innerMessage := annotation + " belongs on the outer view projection, not inside database SQL"
		if annotation == "invariant" {
			innerMessage = "Datly view controls, tag and invariant annotations belong on the outer view projection, not inside database SQL"
		}
		for _, tc := range []struct{ name, projection, want string }{
			{"alias", "r.*," + call + " AS f", annotation + " must be a standalone SELECT annotation without an alias"},
			{"nested", "r.*,COALESCE(" + call + ",false)", annotation + " must be a standalone outer SELECT annotation"},
			{"inner SQL", "", innerMessage},
		} {
			t.Run(annotation+"/"+tc.name, func(t *testing.T) {
				sql := "SELECT " + tc.projection + " FROM (SELECT id, '' AS flag FROM records) r"
				if tc.name == "inner SQL" {
					sql = "SELECT r.* FROM (SELECT id, " + call + " FROM records r) r"
				}
				_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
				if err == nil || err.Error() != CodeViewDirective+": "+tc.want {
					t.Fatalf("diagnostic=%v, want %s", err, tc.want)
				}
			})
		}
	}
}
