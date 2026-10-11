package generate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestLinkedSQLResourceDoesNotDiscardDQLBehavior(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "parents", "queries")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	const original = "SELECT c.id,c.parent_id FROM children c ORDER BY c.id\n"
	if err := os.WriteFile(filepath.Join(dir, "children.sql"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := &planResolver{input: Input{ProjectRoot: root}}
	wrapper := "SELECT * FROM (" + original + ") children"
	for _, sql := range []string{original, wrapper} {
		got, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", sql)
		if err != nil || got != original {
			t.Fatalf("original bytes lost: %q %v", got, err)
		}
	}
	for _, sql := range []string{
		wrapper + " WHERE parent_id=1",
		wrapper + " ORDER BY id DESC",
		wrapper + " LIMIT 1",
		"SELECT DISTINCT * FROM (" + original + ") children",
		"SELECT id FROM (" + original + ") children",
		"SELECT * EXCEPT(parent_id) FROM (" + original + ") children",
		"SELECT * FROM (SELECT c.id,c.parent_id FROM children c WHERE c.id>1 ORDER BY c.id) children",
		wrapper + " UNION SELECT id,parent_id FROM children",
	} {
		if _, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", sql); err == nil {
			t.Fatalf("DQL behavior silently discarded: %s", sql)
		}
	}
}

func TestLinkedSQLResourceRetainsLineCommentsInDerivedSource(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "parents", "queries")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	const original = "SELECT id, '-- protected' AS label FROM children\n-- original annotation\nWHERE id > 0\n"
	if err := os.WriteFile(filepath.Join(dir, "children.sql"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := &planResolver{input: Input{ProjectRoot: root}}
	wrapper := "SELECT * FROM (" + original + ") children"
	got, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", wrapper)
	if err != nil || got != original {
		t.Fatalf("original comment bytes lost: %q %v", got, err)
	}
	for _, changed := range []string{wrapper + " WHERE id=1", "SELECT * FROM (SELECT id, '-- changed' AS label FROM children\n-- original annotation\nWHERE id > 0\n) children"} {
		if _, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", changed); err == nil {
			t.Fatal("changed SQL behavior accepted")
		}
	}
}

func TestLinkedSQLResourceRetainsOriginalQueryBindingForm(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "parents", "queries")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	const original = "SELECT id FROM children WHERE stamp=$criteria.AppendBinding($Unsafe.At)\n"
	if err := os.WriteFile(filepath.Join(dir, "children.sql"), []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := &planResolver{input: Input{ProjectRoot: root, Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "At", Source: spec.BindSource{Kind: "query", Name: "at"}}}}}}
	const authored = "SELECT id FROM children WHERE stamp=$At\n"
	for _, SQL := range []string{authored, "SELECT * FROM (" + authored + ") children"} {
		got, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", SQL)
		if err != nil || got != original {
			t.Fatalf("original binding bytes changed: %q %v", got, err)
		}
	}
	for _, SQL := range []string{"SELECT id FROM children WHERE stamp=$Other", "SELECT id FROM children WHERE stamp='$criteria.AppendBinding($Unsafe.At)'", "SELECT * FROM (" + authored + ") children WHERE id=1"} {
		if _, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", SQL); err == nil {
			t.Fatal("different query accepted")
		}
	}
	resolver.input.Component.Parameters = nil
	if _, err := resolver.linkedSQLResource("example.com/generated/parents", "queries/children.sql", authored); err == nil {
		t.Fatal("undeclared query binding accepted")
	}
}

func TestQueryBindingComparisonPreservesTemplateDecisionsAndLocals(t *testing.T) {
	resolver := &planResolver{input: Input{Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "At", Source: spec.BindSource{Kind: "query"}}}}}}
	for _, source := range []string{
		"#if($criteria.AppendBinding($Unsafe.At)) SELECT 1 #end",
		"#set($At = 1) SELECT $criteria.AppendBinding($Unsafe.At)",
		"SELECT '$criteria.AppendBinding($Unsafe.At)'",
	} {
		if got := resolver.queryBindingComparison(source); got != source {
			t.Fatalf("non-emitted query authority changed: %q", got)
		}
	}
	const source = "#if($At) SELECT $criteria.AppendBinding($Unsafe.At) #end"
	if got := resolver.queryBindingComparison(source); got != "#if($At) SELECT $At #end" {
		t.Fatalf("emitted query body not recognized: %q", got)
	}
}
