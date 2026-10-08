package compile

import (
	"github.com/viant/datly/spec"
	"testing"
	"testing/fstest"
)

func TestDocumentationMetadataPreservesOriginAndAmbiguity(t *testing.T) {
	for _, tc := range []struct{ sql, output, table, column string }{
		{"SELECT u.id AS user_id FROM users u", "user_id", "users", "id"},
		{"SELECT o.id AS user_id FROM users u JOIN orders o ON o.user_id=u.id", "user_id", "orders", "id"},
		{"SELECT u.*,o.id AS id FROM users u JOIN orders o ON o.user_id=u.id", "id", "", ""},
		{"SELECT COUNT(*) AS user_id FROM users u", "user_id", "", ""},
	} {
		c := &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: tc.sql}, Columns: []*spec.Column{{Name: "ID", Source: tc.output}}}}
		if err := BackfillDocumentationMetadata(c); err != nil {
			t.Fatal(err)
		}
		origin := c.RootView.Columns[0].DocumentationOrigin
		if origin == nil || origin.Table != tc.table || origin.Column != tc.column {
			t.Fatalf("%s: %+v", tc.sql, origin)
		}
		clone := c.Clone()
		clone.RootView.Columns[0].DocumentationOrigin.Table = "changed"
		if origin.Table == "changed" {
			t.Fatal("origin clone shares mutable metadata")
		}
	}
}
func TestDocumentationMetadataDefersUnboundResources(t *testing.T) {
	c := &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{URI: "rows.sql"}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	if c.RootView.DocumentationTable != "" || c.RootView.Columns[0].DocumentationOrigin != nil {
		t.Fatal("unresolved source marked complete")
	}
	if err := BackfillDocumentationMetadata(c, fstest.MapFS{"rows.sql": {Data: []byte("SELECT id FROM users")}}); err != nil {
		t.Fatal(err)
	}
	if c.RootView.DocumentationTable != "users" || c.RootView.Columns[0].DocumentationOrigin.Table != "users" {
		t.Fatal("origin not resolved")
	}
	if c.RootView.Source.SQL != "" {
		t.Fatal("authoring SQL overwritten")
	}
}
