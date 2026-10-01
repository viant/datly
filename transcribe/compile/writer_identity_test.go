package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestWriterIdentityDirective(t *testing.T) {
	for _, tc := range []struct {
		expr  string
		valid bool
	}{{"r.*,writer_identity(r,'assigned-update')", true}, {"r.*,writer_identity(r,'unknown')", false}, {"r.*,writer_identity(other,'assigned-update')", false}, {"r.*,writer_identity(r,'assigned-update') AS value", false}, {"r.*,writer_identity(r,'assigned-update'),writer_identity(r,'assigned-update')", false}} {
		sql := "SELECT " + tc.expr + " FROM (SELECT id FROM records) r"
		got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s err%v", tc.expr, err)
		}
		if err == nil && (got.WriterIdentityPolicy != "assigned-update" || strings.Contains(got.Source.SQL, "writer_identity")) {
			t.Fatal("writer policy retained in business SQL")
		}
	}
}
