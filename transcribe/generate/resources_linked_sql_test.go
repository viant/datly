package generate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
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
