package main

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/internal/testharness/sqlite"
	xshape "github.com/viant/x/shape"
)

func TestGenExecutableOuterProjection(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, operation := range []string{"get", "patch"} {
		for _, split := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/local", true: "/split"}[split], func(t *testing.T) {
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				const module = "github.com/viant/datly/outerprojection"
				(testharness.GeneratedModule{Path: module}).Write(t, root)
				if err := os.Mkdir(filepath.Join(root, "source"), 0755); err != nil {
					t.Fatal(err)
				}
				write := func(path, value string) {
					t.Helper()
					if err := os.WriteFile(path, []byte(value), 0644); err != nil {
						t.Fatal(err)
					}
				}
				read := func(path string) string {
					t.Helper()
					b, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					return string(b)
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
				args := []string{"transcribe", operation, "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
				run := func() {
					t.Helper()
					out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
					if err != nil {
						t.Fatalf("transcribe: %v\n%s", err, out)
					}
				}
				rootPath, rootType := filepath.Join(root, "api/orders/views.go"), "OrdersView"
				childPath, childType := rootPath, "ItemsView"
				if split {
					rootPath, rootType = filepath.Join(root, "entities/order.go"), "Order"
					childPath, childType = filepath.Join(root, "items/item.go"), "Item"
				}
				var hooks string
				hookPath := filepath.Join(root, "api/orders/lifecycle.go")
				for _, step := range []struct {
					name, parent, child string
					rootName, childName string
				}{
					{"initial", "orders.*", "items.*", "Name", "Name"},
					{"outer root removal", "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.*", "", "Name"},
					{"outer child removal", "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.ID", "", ""},
					{"unlisted child", "orders.ID,orders.KIND_ID,orders.START,orders.END", "", "", ""},
					{"changed child restriction", "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.ID", "", ""},
					{"aliases", "orders.ID,orders.KIND_ID,orders.START,orders.END,orders.NAME AS DisplayName", "items.ID,items.NAME AS ItemLabel", "DisplayName", "ItemLabel"},
					{"SQL alias spelling", "orders.ID,orders.KIND_ID,orders.START,orders.END,orders.NAME AS Exact_Name", "items.ID,items.NAME AS Item_Label", "ExactName", "ItemLabel"},
					{"internal keys", "orders.START,orders.END", "items.NAME", "", "Name"},
					{"aliased keys", "orders.ID AS RootKey,orders.KIND_ID,orders.START,orders.END,orders.NAME AS DisplayName", "items.ID AS ItemKey,items.ORDER_ID AS ParentKey,items.NAME AS ItemLabel", "DisplayName", "ItemLabel"},
					{"aliased inner keys", "orders.ID AS RootKey,orders.KIND_ID,orders.START,orders.END,orders.NAME AS DisplayName", "items.ID AS ItemKey,items.StoredParent AS ParentKey,items.NAME AS ItemLabel", "DisplayName", "ItemLabel"},
					{"restore", "orders.*", "items.*", "Name", "Name"},
				} {
					t.Log(step.name)
					source := genpatch.OuterProjectionDQL(module, operation, step.parent, step.child, split)
					if operation == "patch" {
						source = strings.Replace(source, "\nFROM", ", lifecycle_type(orders,'OrderRules')\nFROM", 1)
					}
					childID := 10
					if step.name == "changed child restriction" {
						source = strings.Replace(source, "i.ID=10", "i.ID=11", 1)
						childID = 11
					}
					if step.name == "aliased inner keys" {
						source = strings.Replace(source, "SELECT i.* FROM ITEMS", "SELECT i.ID,i.ORDER_ID AS StoredParent,i.NAME FROM ITEMS", 1)
						source = strings.Replace(source, "items.ORDER_ID=orders.ID", "items.StoredParent=orders.ID", 1)
					}
					write(filepath.Join(root, "source/Orders.dql"), source)
					run()
					if operation == "patch" {
						if hooks == "" {
							hooks = strings.Replace(read(hookPath), "return nil", "// authored lifecycle remains\n lifecycleCalls++\n return nil", 1) + "\nvar lifecycleCalls int\n"
							write(hookPath, hooks)
						} else if read(hookPath) != hooks {
							t.Fatal("authored Lifecycle changed")
						}
					}
					for _, expected := range []struct{ path, owner, name string }{{rootPath, rootType, step.rootName}, {childPath, childType, step.childName}} {
						parsed, err := (xshape.SourceParser{}).ParseFile(expected.path)
						if err != nil {
							t.Fatal(err)
						}
						found := false
						for _, field := range parsed.Fields {
							if field.Owner != expected.owner && field.Owner != expected.owner+"Has" {
								continue
							}
							if step.name == "internal keys" && field.Owner == expected.owner && len(field.Names) == 1 && (field.Names[0] == "Id" || field.Names[0] == "KindId" || field.Names[0] == "OrderId") && reflect.StructTag(field.Tag).Get("internal") != "true" {
								t.Fatalf("backing field not internal: %+v", field)
							}
							for _, name := range field.Names {
								if name == expected.name && field.Owner == expected.owner {
									found = true
								}
								if (name == "Name" || name == "DisplayName" || name == "ItemLabel" || name == "ExactName") && name != expected.name {
									t.Fatalf("%s retained stale %s", expected.owner, name)
								}
							}
						}
						if expected.name != "" && !found {
							t.Fatalf("%s lost alias %s", expected.owner, expected.name)
						}
					}
					before := snapshot()
					run()
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("non-idempotent shapes")
					}
					rootKey, childKey, childFK := "Id", "Id", "OrderId"
					if step.name == "aliased keys" || step.name == "aliased inner keys" {
						rootKey, childKey, childFK = "RootKey", "ItemKey", "ParentKey"
					}
					imports, writer := "", ""
					if operation == "patch" {
						imports = genpatch.OuterProjectionWriterImports
						body := `{"Data":[{"` + strings.ToLower(rootKey[:1]) + rootKey[1:] + `":1`
						if step.rootName != "" {
							body += `,"` + strings.ToLower(step.rootName[:1]) + step.rootName[1:] + `":"changed"`
						}
						body += `,"Items":[{"` + strings.ToLower(childKey[:1]) + childKey[1:] + `":` + strconv.Itoa(childID)
						if step.childName != "" {
							body += `,"` + strings.ToLower(step.childName[:1]) + step.childName[1:] + `":"changed child"`
						}
						body += `}]}]}`
						wantRoot, wantChild := "before", "old"
						if childID == 11 {
							wantChild = "hidden child"
						}
						if step.rootName != "" {
							wantRoot = "changed"
						}
						if step.childName != "" {
							wantChild = "changed child"
						}
						writer = strings.NewReplacer("BODY", strconv.Quote(body), "WANT_ROOT", strconv.Quote(wantRoot), "WANT_CHILD", strconv.Quote(wantChild), "CHILD_ID", strconv.Itoa(childID)).Replace(genpatch.OuterProjectionWriterTest)
					}
					consumer := strings.NewReplacer("WRITER_IMPORTS", imports, "WRITER_TEST", writer, "ROOT_KEY", rootKey, "CHILD_KEY", childKey, "CHILD_FK", childFK, "ROOT_NAME", step.rootName, "CHILD_NAME", step.childName, "CHILD_ID", strconv.Itoa(childID)).Replace(genpatch.OuterProjectionRuntime)
					if operation == "patch" {
						start, end := strings.Index(consumer, " READER_START"), strings.Index(consumer, " READER_END")
						consumer = consumer[:start] + consumer[end+len(" READER_END"):]
						consumer = strings.Replace(consumer, ` "github.com/viant/datly/sql/reader"`, "", 1)
					} else {
						consumer = strings.NewReplacer(" READER_START", "", " READER_END", "").Replace(consumer)
					}
					write(filepath.Join(root, "api/orders/outer_runtime_test.go"), consumer)
					command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./...")
					command.Dir = root
					if out, err := command.CombinedOutput(); err != nil {
						t.Fatalf("generated module: %v\n%s", err, out)
					}
				}
				for _, missing := range []string{"root", "child", "child wildcard"} {
					text := genpatch.OuterProjectionDQL(module, operation, "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.ID", split)
					switch missing {
					case "root":
						text = strings.Replace(text, "SELECT o.* FROM ORDERS", "SELECT o.NAME,o.KIND_ID,o.START,o.END FROM ORDERS", 1)
					case "child":
						text = strings.Replace(text, "SELECT i.* FROM ITEMS", "SELECT i.ID,i.NAME FROM ITEMS", 1)
					case "child wildcard":
						text = strings.Replace(text, "SELECT i.* FROM ITEMS", "SELECT i.ID,i.NAME FROM ITEMS", 1)
						text = strings.Replace(text, "items.ID, kind.*", "items.*, kind.*", 1)
					}
					write(filepath.Join(root, "source/Orders.dql"), text)
					before := snapshot()
					out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
					if err == nil || !strings.Contains(string(out), "required relation output") {
						t.Fatalf("missing %s linking output: %v %s", missing, err, out)
					}
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("missing linking output partially wrote project")
					}
				}
				for _, annotation := range []string{"CAST(orders.NAME AS string)", "invariant(orders.NAME,'Window')", "CAST(items.NAME AS string)", "invariant(items.NAME,'Window')", "CAST(orders.OLD_NAME AS string)"} {
					text := genpatch.OuterProjectionDQL(module, operation, "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.ID", split)
					text = strings.Replace(text, "\nFROM", ", "+annotation+"\nFROM", 1)
					write(filepath.Join(root, "source/Orders.dql"), text)
					before := snapshot()
					out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
					if err == nil || !strings.Contains(string(out), "absent from the SQL projection") {
						t.Fatalf("stale %s: %v %s", annotation, err, out)
					}
					if !reflect.DeepEqual(before, snapshot()) {
						t.Fatal("rejected annotation partially wrote project")
					}
				}
				// The new relation-alias authority still rejects edited generated links.
				shape := read(rootPath)
				edited := strings.Replace(shape, "Id:orders.ID=OrderId:items.ORDER_ID", "Id:orders.ID=OrderId:items.EDITED", 1)
				if edited == shape {
					t.Fatal("missing relation edit target")
				}
				write(rootPath, edited)
				write(filepath.Join(root, "source/Orders.dql"), genpatch.OuterProjectionDQL(module, operation, "orders.ID AS RootKey,orders.KIND_ID,orders.START,orders.END,orders.NAME", "items.ID,items.ORDER_ID AS ParentKey,items.NAME", split))
				before := snapshot()
				out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
				if err == nil {
					t.Fatalf("customized relation overwritten: %s", out)
				}
				if !reflect.DeepEqual(before, snapshot()) {
					t.Fatal("conflicting relation partially wrote project")
				}
			})
		}
	}
}
