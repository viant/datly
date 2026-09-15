package main

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/internal/testharness/sqlite"
	xshape "github.com/viant/x/shape"
)

func TestGenExecutableNamedGraphRegeneration(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "local shapes", true: "separate packages"}[split], func(t *testing.T) {
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			const module = "github.com/viant/datly/genregeneration"
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			if err := os.Mkdir(filepath.Join(root, "source"), 0755); err != nil {
				t.Fatal(err)
			}
			sourcePath := filepath.Join(root, "source", "Orders.dql")
			shapePath, owner := filepath.Join(root, "api/orders/orders.go"), "OrdersView"
			base := genpatch.NamedGraphDQL
			if split {
				base = `#import('requests','` + module + `/requests')
#import('responses','` + module + `/responses')
#import('rows','` + module + `/entities')
#import('children','` + module + `/items')
#setting($_ = $input_type('requests.OrdersInput'))
#setting($_ = $output_type('responses.OrdersOutput'))
` + strings.Replace(base, "kind.*", "kind.*, type(orders,'rows.Order'), dest(orders,'order.go'), type(items,'children.Item'), dest(items,'item.go')", 1)
				shapePath, owner = filepath.Join(root, "entities/order.go"), "Order"
			}
			write := func(path, text string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(text), 0644); err != nil {
					t.Fatal(err)
				}
			}
			read := func(path string) string {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return string(data)
			}
			snapshot := func() map[string]string {
				t.Helper()
				result := map[string]string{}
				err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !entry.IsDir() {
						result[path] = read(path)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			args := []string{"gen", "-dir", root, "-op", "patch", "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
			run := func(t *testing.T, failure bool) {
				t.Helper()
				output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
				if failure {
					if err == nil || !strings.Contains(string(output), "customized type or tag") {
						t.Fatalf("expected source conflict: %v\n%s", err, output)
					}
				} else if err != nil || !strings.Contains(string(output), "Generated go patch") {
					t.Fatalf("gen: %v\n%s", err, output)
				}
			}
			annotate := func(annotations string) string {
				if annotations == "" {
					return base
				}
				return strings.Replace(base, "\nFROM", ", "+annotations+"\nFROM", 1)
			}
			invariant := func(name string) string {
				return "invariant(orders.START,'" + name + "'), invariant(orders.END,'" + name + "')"
			}
			write(sourcePath, base)
			run(t, false)
			hookPath := filepath.Join(root, "api/orders/orders_hooks.go")
			hooks := strings.Replace(read(hookPath), "return nil", "// authored lifecycle\n\tlifecycleCalls++\n\treturn nil", 1) + "\nvar lifecycleCalls int\n"
			write(hookPath, hooks)
			write(filepath.Join(root, "api/orders/regeneration_test.go"), `package orders
import (
 "context"
 "testing"
 xhandler "github.com/viant/xdatly/handler"
)
func TestAuthoredLifecycle(t *testing.T) {
 lifecycleCalls = 0
 if err := (&`+owner+`Lifecycle{}).Init(context.Background(), &`+owner+`{}, xhandler.EntityState[`+owner+`, xhandler.NoParent]{}); err != nil { t.Fatal(err) }
 if lifecycleCalls != 1 { t.Fatal("authored lifecycle was not called") }
}
`)
			for _, step := range []struct {
				name, annotations, group, fieldType, required string
				removeName                                    bool
			}{
				{name: "add invariants", annotations: invariant("Window"), group: "Window", fieldType: "*string", required: "true"},
				{name: "change invariants", annotations: invariant("Delivery"), group: "Delivery", fieldType: "*string", required: "true"},
				{name: "remove invariants", fieldType: "*string", required: "true"},
				{name: "optional value cast", annotations: `tag(orders.NAME,'sqlx:"NAME,required=false"'), CAST(orders.NAME AS string)`, fieldType: "string", required: "false"},
				{name: "required nullable cast", annotations: `tag(orders.NAME,'sqlx:"NAME,required=true"'), CAST(orders.NAME AS *string)`, fieldType: "*string", required: "true"},
				{name: "cast value", annotations: `CAST(orders.NAME AS string)`, fieldType: "string", required: "true"},
				{name: "remove cast", fieldType: "*string", required: "true"},
				{name: "remove projection", removeName: true},
			} {
				t.Run(step.name, func(t *testing.T) {
					dql := annotate(step.annotations)
					if step.removeName {
						dql = strings.Replace(dql, "orders.*", "orders.ID, orders.KIND_ID, orders.START, orders.END", 1)
						dql = strings.Replace(dql, "SELECT o.* FROM ORDERS", "SELECT o.ID, o.KIND_ID, o.START, o.END FROM ORDERS", 1)
					}
					write(sourcePath, dql)
					run(t, false)
					parsed, err := (xshape.SourceParser{}).ParseFile(shapePath)
					if err != nil {
						t.Fatal(err)
					}
					foundDates, foundName := 0, false
					for _, field := range parsed.Fields {
						if field.Owner != owner || len(field.Names) != 1 {
							continue
						}
						switch field.Names[0] {
						case "Start", "End":
							foundDates++
							if actual := reflect.StructTag(field.Tag).Get("invariant"); actual != step.group {
								t.Fatalf("%s invariant=%q want %q", field.Names[0], actual, step.group)
							}
						case "Name":
							foundName = true
							if step.removeName || field.TypeExpr != step.fieldType || !strings.Contains(field.Tag, "required="+step.required) {
								t.Fatalf("Name: %+v", field)
							}
						}
					}
					if foundDates != 2 || foundName == step.removeName {
						t.Fatalf("fields: dates=%d name=%t", foundDates, foundName)
					}
					if step.removeName {
						for _, field := range parsed.Fields {
							if field.Owner == owner+"Has" && len(field.Names) == 1 && field.Names[0] == "Name" {
								t.Fatal("removed projection retained original-presence field")
							}
						}
					}
					invariantPath := filepath.Join(root, "api/orders/NewOrdersHandler_invariants_gen.go")
					if step.group != "" {
						if !strings.Contains(read(invariantPath), "Backfill"+step.group+"IfNeeded") {
							t.Fatal("invariant implementation did not follow DQL")
						}
					} else if _, err := os.Stat(invariantPath); !os.IsNotExist(err) {
						t.Fatal("removed invariant phase retained", err)
					}
					if read(hookPath) != hooks {
						t.Fatal("authored lifecycle changed")
					}
					before := snapshot()
					run(t, false)
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("identical gen changed project bytes")
					}
					command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./...")
					command.Dir = root
					if output, err := command.CombinedOutput(); err != nil {
						t.Fatalf("generated module: %v\n%s", err, output)
					}
				})
			}
			// Restoring the DQL column is a generated addition, never a manual fix.
			write(sourcePath, base)
			run(t, false)
			clean := read(shapePath)
			for _, edit := range []struct{ name, old, next, annotations string }{
				{"tag", `sqlx:"START,required=true"`, `sqlx:"START,required=true" custom:"edited"`, invariant("Window")},
				{"tag equals proposal", `sqlx:"START,required=true"`, `invariant:"Window" sqlx:"START,required=true"`, invariant("Window")},
				{"type before cast", "Name *string", "Name *int", "CAST(orders.NAME AS string)"},
			} {
				t.Run("conflict/"+edit.name, func(t *testing.T) {
					changed := strings.Replace(clean, edit.old, edit.next, 1)
					if changed == clean {
						t.Fatalf("missing edit target %q", edit.old)
					}
					write(shapePath, changed)
					write(sourcePath, annotate(edit.annotations))
					before := snapshot()
					run(t, true)
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("conflicting gen partially wrote project")
					}
				})
			}
		})
	}
}
