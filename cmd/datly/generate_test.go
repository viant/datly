package main

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestGenCommandBoundary(t *testing.T) {
	for _, args := range [][]string{{"gen"}, {"gen", "-op", "delete", "example.com/app/orders"}, {"gen", "-op", "patch", "-lang", "java", "example.com/app/orders"}, {"gen", "-op", "patch", "-dest", "wrong", "example.com/app/orders"}, {"gen", "-op", "patch", "-dsn", "unused", "example.com/app/orders"}} {
		var out, diagnostic bytes.Buffer
		if code := run(context.Background(), args, &out, &diagnostic); code != 2 || diagnostic.Len() == 0 {
			t.Fatalf("%v: %d %s", args, code, &diagnostic)
		}
	}
}

func TestGenCommandSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/gencommand"}).Write(t, root)
	sourceDir := filepath.Join(root, "orders")
	if err := os.Mkdir(sourceDir, 0755); err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(sourceDir, "Orders.dql")
	dql := strings.Replace(genpatch.DQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, type(o,'Order'), type(Items,'Item'),", 1)
	if err := os.WriteFile(sourcePath, []byte(dql), 0644); err != nil {
		t.Fatal(err)
	}
	var out, diagnostic bytes.Buffer
	args := []string{"gen", "-dir", root, "-op", "patch", "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "github.com/viant/datly/gencommand/orders"}
	if code := run(ctx, args, &out, &diagnostic); code != 0 {
		t.Fatalf("code=%d %s", code, &diagnostic)
	}
	if !strings.Contains(out.String(), "Generated go patch") {
		t.Fatal(&out)
	}
	if content, err := os.ReadFile(sourcePath); err != nil || string(content) != dql {
		t.Fatal("authored source changed", err)
	}
	if matches, err := filepath.Glob(filepath.Join(root, "api", "orders", "orders_hooks.go")); err != nil || len(matches) != 1 {
		t.Fatal("missing generated hook", matches, err)
	}
	hookPath := filepath.Join(root, "api", "orders", "orders_hooks.go")
	assertGeneratedCommentPlacement(t, hookPath, map[string]string{
		"OrderLifecycle": "customizes role Input.Orders",
		"ItemLifecycle":  "customizes role Input.Orders.Items",
	}, "")
	assertGeneratedCommentPlacement(t, filepath.Join(root, "api", "orders", "orders_entities_gen.go"), nil, "BackfillIntervalIfNeeded")
	hookEdited, err := os.ReadFile(hookPath)
	if err != nil {
		t.Fatal(err)
	}
	hookEdited = append(hookEdited, []byte("\nvar cliHookEditPreserved = true\n")...)
	if err = os.WriteFile(hookPath, hookEdited, 0644); err != nil {
		t.Fatal(err)
	}
	if code := run(ctx, args, &out, &diagnostic); code != 0 {
		t.Fatalf("regenerate code=%d %s", code, &diagnostic)
	}
	hookAfter, err := os.ReadFile(hookPath)
	if err != nil || string(hookAfter) != string(hookEdited) {
		t.Fatalf("edited create-once hook changed on rerun: %v\n%s", err, hookAfter)
	}

	// Compile the actual executable without this test binary's SQLite metadata imports.
	command := exec.CommandContext(ctx, "go", append([]string{"run", "."}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("real gen command: %v\n%s", err, output)
	}
}

func assertGeneratedCommentPlacement(t *testing.T, path string, typeDocs map[string]string, helperDoc string) {
	t.Helper()
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "package //") {
		t.Fatalf("declaration doc leaked into package clause in %s:\n%s", path, source[:min(len(source), 300)])
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.ParseComments)
	if err != nil {
		t.Fatalf("generated Go does not parse %s: %v\n%s", path, err, source[:min(len(source), 300)])
	}
	for name, want := range typeDocs {
		doc := generatedTypeDoc(file, name)
		if !strings.Contains(doc, want) {
			t.Fatalf("generated type %s doc=%q, want %q in %s", name, doc, want, path)
		}
	}
	if helperDoc != "" {
		doc := generatedFuncDoc(file, helperDoc)
		if !strings.Contains(doc, "hydrates omitted group values") {
			t.Fatalf("generated helper %s doc=%q in %s", helperDoc, doc, path)
		}
	}
}

func generatedTypeDoc(file *ast.File, name string) string {
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range general.Specs {
			typed, ok := spec.(*ast.TypeSpec)
			if ok && typed.Name.Name == name && general.Doc != nil {
				return general.Doc.Text()
			}
		}
	}
	return ""
}

func generatedFuncDoc(file *ast.File, name string) string {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name && function.Doc != nil {
			return function.Doc.Text()
		}
	}
	return ""
}

func TestGenExecutableDestinations(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	const module = "github.com/viant/datly/gencommand"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	for directory, content := range map[string]string{"source": genpatch.DestinationDQL(module), "hooks/root": genpatch.RootHooks, "hooks/child": genpatch.ChildHooks} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
		name := "hooks.go"
		if directory == "source" {
			name = "Orders.dql"
		}
		content = strings.ReplaceAll(content, "github.com/viant/datly/genfixture", module)
		if err := os.WriteFile(filepath.Join(root, directory, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"run", ".", "gen", "-dir", root, "-op", "patch", "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
	for iteration := 0; iteration < 2; iteration++ {
		command := exec.CommandContext(ctx, "go", args...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("destination gen executable: %v\n%s", err, output)
		}
	}
	for _, directory := range []string{"api/orders", "requests", "responses", "entities", "items"} {
		if _, err := os.Stat(filepath.Join(root, directory)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(err) {
		t.Fatal("forced generated directory", err)
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("CLI products: %v\n%s", err, output)
	}
}
