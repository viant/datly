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

func TestOuterProjectionPublicMutationContractControls(t *testing.T) {
	ctx := context.Background()
	binary := filepath.Join(t.TempDir(), "datly")
	if output, err := testharness.SourceGoCommand(t, "../..", "build", "-o", binary, "./cmd/datly").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, operation := range []string{"patch"} {
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
					{"internal supplied", "orders.ID,orders.KIND_ID,orders.START,orders.END", "", "", ""},
					{"internal omitted", "orders.ID,orders.KIND_ID,orders.START,orders.END", "", "", ""},
					{"public id", "orders.ID,orders.KIND_ID,orders.START,orders.END", "items.ID", "", ""},
					{"internal root", "orders.START,orders.END", "items.NAME", "", "Name"},
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
						imports = "\n \"fmt\"\n \"sync\"\n \"github.com/viant/sqlx/testutil/sqlfault\"\n" + genpatch.OuterProjectionWriterImports
						body := `{"Data":[{"` + strings.ToLower(rootKey[:1]) + rootKey[1:] + `":1`
						if step.rootName != "" {
							body += `,"` + strings.ToLower(step.rootName[:1]) + step.rootName[1:] + `":"changed"`
						}
						body += `,"Items":[{"` + strings.ToLower(childKey[:1]) + childKey[1:] + `":` + strconv.Itoa(childID)
						if step.childName != "" {
							body += `,"` + strings.ToLower(step.childName[:1]) + step.childName[1:] + `":"changed child"`
						}
						body += `}]}]}`
						if step.name == "internal omitted" {
							body = `{"Data":[{"id":1}]}`
						}
						wantRoot, wantChild := "before", "old"
						if childID == 11 {
							wantChild = "hidden child"
						}
						if step.rootName != "" {
							wantRoot = "changed"
						}
						if step.childName != "" && step.name != "internal root" {
							wantChild = "changed child"
						}

						observerDir, observerPkg := filepath.Join(root, "api/orders"), "orders"
						if split {
							observerDir, observerPkg = filepath.Join(root, "requests"), "requests"
						}
						observer := strings.ReplaceAll(outerContractObserver, "PKG", observerPkg)
						observer = strings.ReplaceAll(observer, "MODE", step.name)
						write(filepath.Join(observerDir, "input_observe.go"), observer)
						t.Logf("ACTUAL_REQUEST_BODY=%s", body)

						expectedError := "NOT NULL constraint failed: ITEMS.NAME"
						if step.name == "internal root" {
							expectedError = "validate writer Orders: field End failed notnull validation; field KindId failed notnull validation; field Start failed notnull validation"
						}
						writer = strings.NewReplacer("MODE_NAME", strconv.Quote(step.name), "EXPECT_ERROR", strconv.FormatBool(step.name == "internal supplied" || step.name == "internal root"), "ERROR_CONTAINS", strconv.Quote(expectedError), "BODY", strconv.Quote(body), "WANT_ROOT", strconv.Quote(wantRoot), "WANT_CHILD", strconv.Quote(wantChild), "CHILD_ID", strconv.Itoa(childID)).Replace(outerContractWriter)
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
					command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-race", "-count=3", "-v", "./...")
					command.Dir = root
					out, commandErr := command.CombinedOutput()
					t.Logf("ACTUAL_RUNTIME=%s", out)
					{

						if evidence := os.Getenv("OUTER_NAME_EVIDENCE"); evidence != "" {
							filepath.WalkDir(root, func(path string, e fs.DirEntry, walkErr error) error {
								if walkErr != nil || e.IsDir() {
									return walkErr
								}
								rel, re := filepath.Rel(root, path)
								if re != nil {
									return re
								}
								dst := filepath.Join(evidence, map[bool]string{false: "local", true: "split"}[split], step.name, rel)
								if re = os.MkdirAll(filepath.Dir(dst), 0755); re != nil {
									return re
								}
								data, re := os.ReadFile(path)
								if re != nil {
									return re
								}
								return os.WriteFile(dst, data, 0600)
							})
						}
						if commandErr != nil {
							t.Fatalf("generated module: %v\n%s", commandErr, out)
						}
					}
				}

			})
		}
	}
}

const outerContractWriter = `
 var mutationMu sync.Mutex;var mutationSQL []string
 runtimeDB:=db.FaultDB(t,func(_ context.Context,call sqlfault.Call)error{if call.Phase=="prepare"{upper:=strings.ToUpper(strings.TrimSpace(call.SQL));if strings.HasPrefix(upper,"INSERT ")||strings.HasPrefix(upper,"UPDATE ")||strings.HasPrefix(upper,"DELETE "){t.Logf("ACTUAL_NATIVE_PREPARED_MUTATION_SQL=%s",call.SQL);mutationMu.Lock();mutationSQL=append(mutationSQL,call.SQL);mutationMu.Unlock()}};return nil});runtimeDB.SetMaxOpenConns(1)
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:runtimeDB}});if err!=nil{t.Fatal(err)}
 metadata,err:=writerhandler.Compile(artifact.Component,reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}),"patch");if err!=nil{t.Fatal(err)}
 childRecord:=metadata.Root.Relations[0].Child;if childRecord.Table!="ITEMS"||childRecord.Auxiliary||len(childRecord.Keys)!=1||childRecord.Keys[0].Column!="ID"||childRecord.CurrentField<0{t.Fatalf("native child Current/key mapping changed: %+v",childRecord)}
 t.Logf("NATIVE_METADATA=table:%s role:%s current:%s keys:%+v fields:%+v",childRecord.Table,childRecord.Path,reflect.TypeOf(OrdersInput{}).Field(childRecord.CurrentField).Name,childRecord.Keys,childRecord.Fields)
 handler,err:=writerhandler.New(artifact.Component,reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}),"patch");if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:runtimeDB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 snapshot:=func() map[string]string {m:=map[string]string{};for _,query:=range []string{"SELECT * FROM ORDERS ORDER BY ID","SELECT * FROM ITEMS ORDER BY ID","SELECT * FROM ORDER_KINDS ORDER BY ID","SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name","SELECT * FROM sqlite_sequence ORDER BY name"}{rows,e:=db.DB.QueryContext(ctx,query);if e!=nil{t.Fatal(e)};cols,e:=rows.Columns();if e!=nil{t.Fatal(e)};var values [][]any;for rows.Next(){row:=make([]any,len(cols));ptr:=make([]any,len(cols));for i:=range row{ptr[i]=&row[i]};if e=rows.Scan(ptr...);e!=nil{t.Fatal(e)};values=append(values,row)};if e=rows.Err();e!=nil{t.Fatal(e)};rows.Close();m[query]=fmt.Sprint(values)};return m}
 before:=snapshot()
 request:=httptest.NewRequest("PATCH","/orders",strings.NewReader(BODY));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 _,err=rt.ExecuteRoute(ctx,"PATCH","/orders",scope)
 if EXPECT_ERROR {if err==nil||!strings.Contains(err.Error(),ERROR_CONTAINS){t.Fatalf("expected unchanged hidden-ID insert failure; got %v",err)};t.Logf("PRESERVED_EXPECTED_FAILURE=%v",err)} else if err!=nil {t.Fatal(err)}
 if !reflect.DeepEqual(before,snapshot()){t.Fatalf("physical rows/DDL/triggers/allocators changed: before=%v after=%v",before,snapshot())}
 mutationMu.Lock();mutations:=append([]string(nil),mutationSQL...);mutationMu.Unlock()
 itemsInsert:=false;for _,query:=range mutations{upper:=strings.ToUpper(query);if strings.Contains(upper,"INSERT INTO ITEMS"){itemsInsert=true}}
 if MODE_NAME=="internal supplied"&&!itemsInsert{t.Fatalf("hidden-ID negative never prepared native ITEMS insert: %v",mutations)}
 if MODE_NAME=="internal root"&&len(mutations)!=0{t.Fatalf("hidden-root validation queued SQL: %v",mutations)}
 if lifecycleCalls==0{t.Fatal("authored Lifecycle was not invoked")}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS WHERE ID=1"},[]struct{Name string}{{WANT_ROOT}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=CHILD_ID"},[]struct{Name string}{{WANT_CHILD}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=11"},[]struct{Name string}{{"hidden child"}})
 t.Log("PHYSICAL_GUARD_PASS=3tables+allDDL+3triggers+sqlite_sequence")
`
const outerContractObserver = `package PKG
import("context";"encoding/json";"fmt";"reflect")
func (i *OrdersInput) Init(context.Context) error {
 data,_:=json.Marshal(outerNameValue(reflect.ValueOf(i)));fmt.Printf("PROBE_NATIVE_BOUND_INPUT=%s\n",data)
 root:=reflect.ValueOf(i).Elem();if "MODE"=="internal root" {for _,name:=range []string{"CurrentOrders","CurrentItems","CurrentKind"}{if root.FieldByName(name).Len()!=0{return fmt.Errorf("hidden root key created Current authority")}};row:=root.FieldByName("Orders").Index(0).Elem();child:=row.FieldByName("Items").Index(0).Elem();if !row.FieldByName("Id").IsNil()||row.FieldByName("Has").Elem().FieldByName("Id").Bool()||!child.FieldByName("Id").IsNil()||child.FieldByName("Has").Elem().FieldByName("Id").Bool(){return fmt.Errorf("hidden root/child identity bound")};return nil};current:=root.FieldByName("CurrentItems");if current.Len()!=1{return fmt.Errorf("Current child count %d",current.Len())};cur:=current.Index(0).Elem();id:=cur.FieldByName("Id");parent:=cur.FieldByName("OrderId");if id.IsNil()||id.Elem().Int()!=10||parent.IsNil()||parent.Elem().Int()!=1{return fmt.Errorf("Current identity/parent changed")}
 rows:=root.FieldByName("Orders");row:=rows.Index(0).Elem();items:=row.FieldByName("Items");has:=row.FieldByName("Has").Elem().FieldByName("Items").Bool()
 switch "MODE" {case "internal omitted":if items.Len()!=0||has{return fmt.Errorf("omitted child acquired presence")};case "internal supplied":if items.Len()!=1||!has{return fmt.Errorf("supplied child lost collection")};child:=items.Index(0).Elem();if !child.FieldByName("Id").IsNil()||child.FieldByName("Has").Elem().FieldByName("Id").Bool(){return fmt.Errorf("private backing identity accepted from public body")};case "public id":if items.Len()!=1||!has{return fmt.Errorf("public child lost collection")};child:=items.Index(0).Elem();id:=child.FieldByName("Id");if id.IsNil()||id.Elem().Int()!=10||!child.FieldByName("Has").Elem().FieldByName("Id").Bool(){return fmt.Errorf("public identity not bound")}}
 return nil
}
func outerNameValue(v reflect.Value) any {if !v.IsValid(){return nil};for v.Kind()==reflect.Pointer {if v.IsNil(){return nil};v=v.Elem()};switch v.Kind(){case reflect.Struct:m:=map[string]any{};for n:=0;n<v.NumField();n++{f:=v.Type().Field(n);if f.PkgPath==""{m[f.Name]=outerNameValue(v.Field(n))}};return m;case reflect.Slice:out:=[]any{};for n:=0;n<v.Len();n++{out=append(out,outerNameValue(v.Index(n)))};return out;default:if v.CanInterface(){return v.Interface()};return nil}}
`
