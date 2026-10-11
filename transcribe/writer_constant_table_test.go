package transcribe

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestWriterRetainsOnlyAuthoredConstantTarget(t *testing.T) {
	for _, tc := range []struct{ name, sql, table, want string }{
		{"constant target", "SELECT r.id FROM $Unsafe.Table r", "default_records", "$Unsafe.Table"},
		{"literal table", "SELECT r.id FROM default_records r", "default_records", "default_records"},
		{"explicit different target", "SELECT r.id FROM $Unsafe.Table r", "other_records", "other_records"},
		{"union source", "SELECT r.id FROM (SELECT id FROM $Unsafe.Table UNION SELECT id FROM other_records) r", "default_records", "default_records"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Source: &spec.ViewSource{Table: tc.table, SQL: tc.sql}}
			component := &spec.Component{RootView: view, Settings: &spec.Settings{Const: map[string]string{"Table": "default_records"}}}
			retainWriterConstantTables(component)
			if view.Source.Table != tc.want || view.Source.SQL != tc.sql {
				t.Fatalf("source target changed incorrectly: %+v", view.Source)
			}
		})
	}
}
