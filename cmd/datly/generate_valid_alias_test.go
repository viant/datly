package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

func TestGenExecutableValidAlias(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, alias := range []string{"value", "_value"} {
		for _, cast := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cast=%v", alias, cast), func(t *testing.T) {
				db := sqlite.New(t)
				inner := "SELECT 1 AS " + alias
				var scalar int
				if err := db.DB.QueryRowContext(ctx, inner).Scan(&scalar); err != nil || scalar != 1 {
					t.Fatalf("SQLite: %d %v", scalar, err)
				}
				text := "#package('api/probe')\n#setting($_ = $route('/probe','GET'))\nSELECT probe.*"
				if cast {
					text += ", CAST(probe." + alias + " AS int)"
				}
				text += " FROM (" + inner + ") probe"
				compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Name: "Probe", Scope: "github.com/viant/datly/validalias/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: text})
				if err != nil {
					t.Fatal(err)
				}
				view := compiled.Component.RootView
				if !strings.Contains(view.Source.SQL, inner) {
					t.Fatalf("authored SQL changed: %s", view.Source.SQL)
				}
				if len(view.Columns) != 1 || view.Columns[0].Name != alias {
					t.Fatalf("SQL alias changed: %+v", view.Columns)
				}
				if cast && (view.Columns[0].Type.Name != "int" || !view.Columns[0].ExplicitType) {
					t.Fatalf("outer CAST lost: %+v", view.Columns[0])
				}
				root := t.TempDir()
				const module = "github.com/viant/datly/validalias"
				(testharness.GeneratedModule{Path: module}).Write(t, root)
				if err := os.Mkdir(filepath.Join(root, "source"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "source/Probe.dql"), []byte(text), 0644); err != nil {
					t.Fatal(err)
				}
				out, err := exec.CommandContext(ctx, binary, "transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module+"/source").CombinedOutput()
				if err != nil {
					t.Fatalf("generate: %v\n%s", err, out)
				}
				views, err := os.ReadFile(filepath.Join(root, "api/probe/views.go"))
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("authored SQL alias=%q CAST=%v\ngenerated views:\n%s", alias, cast, views)
				consumer := strings.ReplaceAll(genpatch.ValidAliasRuntime, "ALIAS", alias)
				if err := os.WriteFile(filepath.Join(root, "api/probe/valid_alias_runtime_test.go"), []byte(consumer), 0644); err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-v", "./...")
				command.Dir = root
				out, err = command.CombinedOutput()
				t.Logf("generated reader:\n%s", out)
				if err != nil {
					t.Fatalf("generated module: %v", err)
				}
			})
		}
	}
}
