package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestInsertValidationPresenceDirective(t *testing.T) {
	for _, tc := range []struct {
		expr      string
		valid, on bool
	}{{"r.*,insert_validation_presence(r,true)", true, true}, {"r.*,insert_validation_presence(r,false)", true, false}, {"r.*,insert_validation_presence(other,true)", false, false}, {"r.*,insert_validation_presence(r,wrong)", false, false}, {"r.*,insert_validation_presence(r,true) AS value", false, false}, {"r.*,insert_validation_presence(r,true),insert_validation_presence(r,true)", false, false}} {
		sql := "SELECT " + tc.expr + " FROM (SELECT id FROM records) r"
		got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s err%v", tc.expr, err)
		}
		if err == nil && (got.InsertValidationPresence != tc.on || strings.Contains(got.Source.SQL, "insert_validation_presence")) {
			t.Fatal("presence flag lost or retained in SQL")
		}
	}
}
