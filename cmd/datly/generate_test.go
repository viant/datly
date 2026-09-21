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
	for _, args := range [][]string{{"transcribe"}, {"transcribe", "delete", "example.com/app/orders"}, {"transcribe", "patch", "-lang", "java", "example.com/app/orders"}, {"transcribe", "patch", "-dest", "wrong", "example.com/app/orders"}, {"transcribe", "patch", "-dsn", "unused", "example.com/app/orders"}} {
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
	dql := strings.Replace(genpatch.DQL, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, type(o,'Order'), type(Items,'Item'), lifecycle_type(o,'OrderLifecycle'), lifecycle_type(Items,'ItemLifecycle'),", 1)
	if err := os.WriteFile(sourcePath, []byte(dql), 0644); err != nil {
		t.Fatal(err)
	}
	var out, diagnostic bytes.Buffer
	args := []string{"transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "github.com/viant/datly/gencommand/orders"}
	if code := run(ctx, args, &out, &diagnostic); code != 0 {
		t.Fatalf("code=%d %s", code, &diagnostic)
	}
	if !strings.Contains(out.String(), "Generated go patch") {
		t.Fatal(&out)
	}
	if content, err := os.ReadFile(sourcePath); err != nil || string(content) != dql {
		t.Fatal("authored source changed", err)
	}
	if matches, err := filepath.Glob(filepath.Join(root, "api", "orders", "lifecycle.go")); err != nil || len(matches) != 1 {
		t.Fatal("missing generated hook", matches, err)
	}
	hookPath := filepath.Join(root, "api", "orders", "lifecycle.go")
	assertGeneratedCommentPlacement(t, hookPath, map[string]string{
		"OrderLifecycle": "customizes role Input.Orders",
		"ItemLifecycle":  "customizes role Input.Orders.Items",
	}, "")
	assertGeneratedCommentPlacement(t, filepath.Join(root, "api", "orders", "entities.go"), nil, "BackfillIntervalIfNeeded")
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
	args := []string{"run", ".", "transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
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

func TestTranscribeOperationsSQLite(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"get", "patch", "post", "put"} {
		for _, layout := range []string{"defaults", "prefix", "overrides"} {
			t.Run(operation+"/"+layout, func(t *testing.T) {
				root := t.TempDir()
				const module = "github.com/viant/datly/transcribecommand"
				(testharness.GeneratedModule{Path: module}).Write(t, root)
				sourceDir := filepath.Join(root, "source")
				if err := os.Mkdir(sourceDir, 0755); err != nil {
					t.Fatal(err)
				}
				source := strings.Replace(genpatch.DQL, "'PATCH'", "'"+strings.ToUpper(operation)+"'", 1)
				roles := []string{"input", "output", "router", "view"}
				support := []string{}
				if operation == "get" {
					// Readers use the registered reader; no handler wrapper is emitted.
				} else {
					source = strings.Replace(source, "SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Kinds.*, lifecycle_type(o,'OrderRules'), lifecycle_type(Items,'ItemRules'),", 1)
					roles = append(roles, "mutation", "lifecycle", "links")
					if operation != "post" {
						roles = append(roles, "resources")
					}
					// Hook scaffolds are create-once only when a component declares
					// authored hooks. Hookless writers must not emit an empty hooks.go.
					support = []string{"entities", "frames", "previous", "layout", "actions", "mutation_output", "validation", "invariants"}
				}
				if layout != "defaults" {
					source = "#setting($_ = $file_prefix('orders_'))\n" + source
				}
				filenames := map[string]string{}
				for _, role := range append(append([]string{}, roles...), support...) {
					name := role + ".go"
					if role == "view" {
						name = "views.go"
					}
					if layout == "prefix" {
						name = "orders_" + name
					}
					if layout == "overrides" {
						name = "chosen_" + name
					}
					filenames[role] = name
				}
				if layout == "overrides" {
					for _, role := range roles {
						directive := role + "_dest"
						if role == "view" {
							directive = "dest"
						}
						source = "#setting($_ = $" + directive + "('" + filenames[role] + "'))\n" + source
					}
					for _, role := range support {
						source = "#setting($_ = $support_dest('" + role + "','" + filenames[role] + "'))\n" + source
					}
				}
				if err := os.WriteFile(filepath.Join(sourceDir, "Orders.dql"), []byte(source), 0644); err != nil {
					t.Fatal(err)
				}
				args := []string{"transcribe", operation, "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
				generate := func() {
					t.Helper()
					output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
					if err != nil || !strings.Contains(string(output), "Generated go "+operation) {
						t.Fatalf("CLI: %v\n%s", err, output)
					}
				}
				generate()
				directory := filepath.Join(root, "api", "orders")
				for role, filename := range filenames {
					content, err := os.ReadFile(filepath.Join(directory, filename))
					if err != nil || len(content) == 0 {
						t.Fatalf("%s file %s: %v", role, filename, err)
					}
				}
				files, err := os.ReadDir(directory)
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range files {
					name := file.Name()
					if !strings.HasSuffix(name, ".go") {
						continue
					}
					if strings.HasSuffix(name, "_gen.go") || strings.HasPrefix(name, "NewOrders") {
						t.Fatalf("unexpected generated filename %s", name)
					}
					if layout == "defaults" && strings.HasPrefix(name, "orders_") {
						t.Fatalf("implicit prefix: %s", name)
					}
					if layout == "prefix" && !strings.HasPrefix(name, "orders_") {
						t.Fatalf("prefix missing: %s", name)
					}
				}
				var hooks []byte
				if operation != "get" {
					path := filepath.Join(directory, filenames["lifecycle"])
					hooks, err = os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					hooks = append(hooks, []byte("\n// application-owned lifecycle edit\nconst LifecycleEdit = true\n")...)
					if err := os.WriteFile(path, hooks, 0644); err != nil {
						t.Fatal(err)
					}
				}
				generate()
				if hooks != nil {
					after, err := os.ReadFile(filepath.Join(directory, filenames["lifecycle"]))
					if err != nil || !bytes.Equal(hooks, after) {
						t.Fatalf("lifecycle changed: %v", err)
					}
				}
				command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./api/orders")
				command.Dir = root
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("generated %s compile: %v\n%s", operation, err, output)
				}
			})
		}
	}
}
