package transcribe

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestLifecycleTargetRejectsReaderBeforeWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "datly")
	build := exec.Command("go", "build", "-o", binary, "./cmd/datly")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	for _, entry := range []string{"source API", "compiled API", "transcribe API", "artifact API", "planned artifact API", "CLI"} {
		for _, hook := range []string{"", "OrderLifecycle", "hooks.OrderLifecycle"} {
			t.Run(entry+"/"+hook, func(t *testing.T) {
				root := t.TempDir()
				(testharness.GeneratedModule{Path: "example.com/shop"}).Write(t, root)
				writeSourceFile(t, root, "hooks/hooks.go", "package hooks\ntype OrderLifecycle struct{}\n")
				catalog := typecatalog.NewCatalog()
				pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), "hooks")
				if err != nil {
					t.Fatal(err)
				}
				if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
					t.Fatal(err)
				}
				text := `#package('example.com/shop/read')
#import('hooks','example.com/shop/hooks')
#setting($_ = $input_type('OrdersInput'))
#setting($_ = $output_type('OrdersOutput'))
#setting($_ = $route('/orders','GET'))
SELECT orders.*,type(orders,'Order')`
				if hook != "" {
					text += ",lifecycle_type(orders,'" + hook + "')"
				}
				text += " FROM (SELECT ID FROM ORDERS) orders"
				writeSourceFile(t, root, "source/Orders.dql", text)
				snapshot := func() map[string]string {
					t.Helper()
					result := map[string]string{}
					err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
						if err != nil {
							return err
						}
						relative, _ := filepath.Rel(root, path)
						if entry.IsDir() {
							result[relative] = "directory"
							return nil
						}
						data, err := os.ReadFile(path)
						result[relative] = string(data)
						return err
					})
					if err != nil {
						t.Fatal(err)
					}
					return result
				}
				before := snapshot()
				source := &Source{Name: "Orders", Scope: "example.com/shop/source", Text: text, Types: catalog, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
				switch entry {
				case "source API":
					_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
				case "compiled API":
					var compiled *Result
					compiled, err = NewCompiler().Compile(ctx, source)
					if err == nil {
						_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
					}
				case "transcribe API":
					_, err = NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root})
				case "artifact API", "planned artifact API":
					var compiled *Result
					compiled, err = NewCompiler().Compile(ctx, source)
					if err == nil {
						var input gen.Input
						var dir string
						input, dir, err = generationInput(root, "", compiled)
						if err == nil {
							if entry == "planned artifact API" {
								var plan *gen.Plan
								plan, err = gen.New(input).Plan()
								if err == nil {
									_, err = gen.EmitScaffold(filepath.Join(root, dir), plan)
								}
							} else {
								_, err = gen.New(input).Generate(filepath.Join(root, dir))
							}
						}
					}
				case "CLI":
					command := exec.Command(binary, "transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/shop/source")
					out, runErr := command.CombinedOutput()
					if runErr != nil {
						err = fmt.Errorf("CLI: %s", out)
					}
				}
				if hook == "" {
					if err != nil {
						t.Fatal(err)
					}
					if _, err = os.Stat(filepath.Join(root, "read/router.go")); err != nil {
						t.Fatal("hookless GET was not generated", err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), "requires the generated Go mutation lifecycle") || !strings.Contains(err.Error(), "OrdersInput.Init") || !strings.Contains(err.Error(), "OrdersOutput.Finalize") || !strings.Contains(err.Error(), "OnFetch") {
					t.Fatalf("unsupported reader lifecycle: %v", err)
				}
				if !reflect.DeepEqual(before, snapshot()) {
					t.Fatal("rejected reader lifecycle modified the workspace")
				}
			})
		}
	}
}

func TestLifecycleTargetRejectsUnsupportedWriterLowering(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"direct Go", "Velty", "high-level Velty", "planned artifact without dispatch"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			before, err := os.ReadDir(root)
			if err != nil {
				t.Fatal(err)
			}
			source := &Source{Types: typecatalog.NewCatalog(), Name: "Orders", Scope: "example.com/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('api/orders')
#setting($_ = $route('/orders','POST'))
SELECT orders.*,lifecycle_type(orders,'OrderLifecycle') FROM ORDERS orders`}
			if mode == "planned artifact without dispatch" {
				var compiled *Result
				compiled, err = NewCompiler().Compile(ctx, source)
				if err == nil {
					var input gen.Input
					var dir string
					input, dir, err = generationInput(root, "", compiled)
					if err == nil {
						var plan *gen.Plan
						plan, err = gen.New(input).Plan()
						if err == nil {
							if validation := plan.ValidateDestination(filepath.Join(root, dir)); validation == nil {
								t.Fatal("undispatched lifecycle passed destination validation")
							}
							_, err = gen.EmitScaffold(filepath.Join(root, dir), plan)
						}
					}
				}
			} else if mode == "high-level Velty" {
				_, err = (Generator{Operation: "post", Language: HandlerVelty}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
			} else {
				target := HandlerGo
				if mode == "Velty" {
					target = HandlerVelty
				}
				_, err = NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: target, Operation: WritePost}}})
			}
			if err == nil || !strings.Contains(err.Error(), "requires the generated Go mutation lifecycle") {
				t.Fatalf("unsupported target: %v", err)
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil || len(entries) != len(before) {
				t.Fatalf("rejected lowering wrote artifacts: %v %v", entries, readErr)
			}
			for i := range entries {
				if entries[i].Name() != before[i].Name() {
					t.Fatal("rejected lowering changed files")
				}
			}
		})
	}
}
