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

func TestStockExpandedCanonicalCurrentCLI(t *testing.T) {
	cases := map[string]string{
		"absent-physical-current":      "",
		"authority-readonly-child":     "",
		"authority-missing-nested-aux": `auxiliary lookup view::Chain|namespace:Chain with nested relations requires explicit authored current authority`,
		"authority-nonstandard-aux":    `auxiliary lookup view::Carrier|namespace:Carrier with nested relations requires explicit authored current authority`,
		"deep-aux":                     "",
		"deep-aux-root":                "",
		"physical-aux-physical":        "",
		"authority-absent":             `auxiliary lookup view::Carrier|namespace:Carrier with nested relations requires explicit authored current authority`,
		"authority-query-conflict":     `auxiliary current CurrentCarrier conflicts with authored binding`,
		"authority-header-conflict":    `auxiliary current CurrentCarrier conflicts with authored binding`,
		"authority-ambiguous":          `generation current-state input for Carrier is ambiguous`,
		"two-physical-roles":           "",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			schema, err := os.ReadFile("../../transcribe/stock_current_resources/expanded-schema.sql")
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
			t.Logf("CLI_AUTHORITY_STAGE layout=%s code=%d stdout=%s diag=%s", name, code, out.String(), diag.String())
			if want != "" {
				if code != 1 || !strings.Contains(diag.String(), want) {
					t.Fatalf("intended authority stage %q; actual code%d %s", want, code, diag.String())
				}
			} else if code != 0 || !strings.Contains(out.String(), "Generated go patch:") {
				t.Fatalf("two same-table control code%d %s %s", code, out.String(), diag.String())
			}
			command := testharness.SourceGoCommand(t, "../..", "run", append([]string{"./cmd/datly"}, args...)...)
			actual, e := command.CombinedOutput()
			t.Logf("ACTUAL_EXECUTABLE_AUTHORITY_STAGE layout=%s output=%s", name, actual)
			if want != "" {
				if e == nil || !strings.Contains(string(actual), want) {
					t.Fatalf("intended actual executable stage %q; actual %v %s", want, e, actual)
				}
			} else if e != nil {
				t.Fatalf("actual two same-table control %v %s", e, actual)
			}
			after, err := os.ReadFile(source)
			if err != nil || string(after) != string(dql) {
				t.Fatal("source drift", err)
			}
			if want != "" {
				for _, folder := range []string{"generated", "source"} {
					files, _ := filepath.Glob(filepath.Join(root, folder, "*.go"))
					if len(files) > 0 {
						t.Fatal("negative emitted Go", files)
					}
				}
			}
		})
	}
}
