package main

import (
	"bytes"
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStockCurrentCanonicalTranscribeCLI(t *testing.T) {
	for _, name := range []string{"nested", "sibling"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			schema, err := os.ReadFile("../../transcribe/stock_current_resources/schema.sql")
			if err != nil {
				t.Fatal(err)
			}
			for _, stmt := range strings.Split(string(schema), ";") {
				if strings.TrimSpace(stmt) != "" {
					if err = db.ExecStatements(ctx, stmt); err != nil {
						t.Fatal(err)
					}
				}
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/stockcurrentfixture"}).Write(t, root)
			dir := filepath.Join(root, "source")
			if err = os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			dql, err := os.ReadFile("../../transcribe/stock_current_resources/" + name + ".dql")
			if err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(dir, "Probe.dql")
			if err = os.WriteFile(source, dql, 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "github.com/viant/datly/stockcurrentfixture/source"}
			var out, diag bytes.Buffer
			code := run(ctx, args, &out, &diag)
			if code != 0 || !strings.Contains(out.String(), "Generated go patch") {
				t.Fatalf("canonical %s code%d %s %s", name, code, out.String(), diag.String())
			}
			t.Logf("canonical %s %s", name, out.String())
			cmd := testharness.SourceGoCommand(t, "../..", "run", append([]string{"./cmd/datly"}, args...)...)
			actual, e := cmd.CombinedOutput()
			if e != nil {
				t.Fatalf("actual %s %v %s", name, e, actual)
			}
			t.Logf("actual %s %s", name, actual)
			after, err := os.ReadFile(source)
			if err != nil || string(after) != string(dql) {
				t.Fatal("source drift", err)
			}
		})
	}
}
