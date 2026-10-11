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
		table, column, known := c.RootView.ProjectionOrigin("ID")
		if !known || table != tc.table || column != tc.column {
			t.Fatalf("%s: %s.%s (%v)", tc.sql, table, column, known)
		}
		clone := c.Clone()
		if _, _, known := clone.RootView.ProjectionOrigin("ID"); known {
			t.Fatal("source rebuild retained compiled attribution")
		}

	}
}
func TestDocumentationMetadataDefersUnboundResources(t *testing.T) {
	c := &spec.Component{RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{URI: "rows.sql"}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	if _, _, known := c.RootView.ProjectionOrigin("ID"); known {
		t.Fatal("unresolved source marked complete")
	}
	if err := BackfillDocumentationMetadata(c, fstest.MapFS{"rows.sql": {Data: []byte("SELECT id FROM users")}}); err != nil {
		t.Fatal(err)
	}
	if table, _, known := c.RootView.ProjectionOrigin("ID"); !known || table != "users" {
		t.Fatal("origin not resolved")
	}
	if c.RootView.Source.SQL != "" {
		t.Fatal("authoring SQL overwritten")
	}
}

func TestDocumentationMetadataRebuildAndOpaque(t *testing.T) {
	c := &spec.Component{RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id FROM users", Table: "canonical"}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	c.RootView.Source.SQL = "SELECT id FROM orders"
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	if table, _, known := c.RootView.ProjectionOrigin("ID"); !known || table != "orders" {
		t.Fatal("stale SQL attribution")
	}
	c.RootView.Source.SQL = "opaque vendor SQL"
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	if _, _, known := c.RootView.ProjectionOrigin("ID"); known {
		t.Fatal("opaque SQL retained stale attribution")
	}
	if c.RootView.ProjectionTable() != "canonical" {
		t.Fatal("canonical table lost")
	}
}

func TestDocumentationMetadataUnresolvedRebuild(t *testing.T) {
	c := &spec.Component{RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT id FROM users"}, Columns: []*spec.Column{{Name: "ID", Source: "id"}}}}
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	c.RootView.Source = &spec.ViewSource{URI: "unbound.sql"}
	if err := BackfillDocumentationMetadata(c); err != nil {
		t.Fatal(err)
	}
	if _, _, known := c.RootView.ProjectionOrigin("ID"); known {
		t.Fatal("unresolved rebuild retained old attribution")
	}
}
