package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
)

func TestDQLImportDiscoveryCLIWithGeneratedDestinationDirectories(t *testing.T) {
	for _, test := range []struct {
		name      string
		precreate bool
	}{
		{name: "absent"},
		{name: "empty", precreate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
				t.Fatal(err)
			}
			root := writeDQLImportCLIProject(t, genpatch.DestinationDQL)
			if test.precreate {
				for _, dir := range []string{"requests", "responses", "entities", "items"} {
					if err := os.MkdirAll(filepath.Join(root.dir, dir), 0755); err != nil {
						t.Fatal(err)
					}
				}
			}
			runDatlyTranscribeCLI(t, root.dir, root.module, filepath.Join(db.TempDir, "test.db"), true)
		})
	}
}

func TestDQLImportDiscoveryCLIPreservesAuthoredPackageFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		setup   func(t *testing.T, root cliProject)
		rewrite func(module string) string
		want    string
	}{
		{
			name: "missing hook type",
			rewrite: func(module string) string {
				return strings.Replace(genpatch.DestinationDQL(module), "lifecycle_type(o,'rh.Hooks')", "lifecycle_type(o,'rh.MissingHooks')", 1)
			},
			want: "MissingHooks",
		},
		{
			name: "malformed go",
			setup: func(t *testing.T, root cliProject) {
				writeSourceFile(t, root.dir, "entities/bad.go", "package entities\nfunc")
			},
			rewrite: func(module string) string { return genpatch.DestinationDQL(module) },
			want:    "bad.go",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
				t.Fatal(err)
			}
			root := writeDQLImportCLIProject(t, test.rewrite)
			if test.setup != nil {
				test.setup(t, root)
			}
			output := runDatlyTranscribeCLI(t, root.dir, root.module, filepath.Join(db.TempDir, "test.db"), false)
			if !strings.Contains(output, test.want) {
				t.Fatalf("CLI error did not preserve %q boundary:\n%s", test.want, output)
			}
		})
	}
}

type cliProject struct {
	dir    string
	module string
}

func writeDQLImportCLIProject(t *testing.T, source func(string) string) cliProject {
	t.Helper()
	module := "github.com/viant/datly/gencommand"
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	for dir, content := range map[string]string{
		"source":      source(module),
		"hooks/root":  genpatch.RootHooks,
		"hooks/child": genpatch.ChildHooks,
	} {
		name := "hooks.go"
		if dir == "source" {
			name = "Orders.dql"
		}
		writeSourceFile(t, root, filepath.Join(dir, name), strings.ReplaceAll(content, "github.com/viant/datly/genfixture", module))
	}
	return cliProject{dir: root, module: module}
}

func runDatlyTranscribeCLI(t *testing.T, root, module, dsn string, wantSuccess bool) string {
	t.Helper()
	command := exec.Command("go", "run", "./cmd/datly", "transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", dsn, module+"/source")
	command.Dir = repoRoot(t)
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	if wantSuccess {
		if err != nil {
			t.Fatalf("datly transcribe CLI: %v\n%s", err, output)
		}
		compileGeneratedCLIProject(t, root)
	} else if err == nil {
		t.Fatalf("datly transcribe CLI succeeded unexpectedly:\n%s", output)
	}
	return string(output)
}

func compileGeneratedCLIProject(t *testing.T, root string) {
	t.Helper()
	command := exec.Command("go", "vet", "-mod=mod", "./...")
	command.Dir = root
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated CLI project compile: %v\n%s", err, output)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("repository root not found")
		}
		dir = next
	}
}
