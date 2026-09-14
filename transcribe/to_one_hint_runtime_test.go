package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestJoinToOneHintReaderWriterRegenerationSQLite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler HandlerOptions
	}{
		{name: "reader"},
		{name: "Go writer", handler: HandlerOptions{Target: HandlerGo, Operation: WritePost}},
		{name: "Velty writer", handler: HandlerOptions{Target: HandlerVelty, Operation: WritePost}},
		{name: "mutation writer", handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, hintRuntimeSchema...); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/hintfixture"}).Write(t, root)
			catalog := typecatalog.NewCatalog()
			for _, one := range []bool{false, true, false} {
				hint, want, cardinality := "", "[]*IView", spec.CardinalityMany
				if one {
					hint, want, cardinality = " AND 1=1", "*IView", spec.CardinalityOne
				}
				method, body, output := "GET", "", "view"
				if tc.handler.Operation != "" {
					method = "POST"
					output = "body"
					body = "#define($_ = $Orders<[]*OrdersView>(body/Data).Cardinality('Many').Required())\n"
				}
				text := fmt.Sprintf("#setting($_ = $route('/orders','%s'))\n%s#define($_ = $Data<[]*OrdersView>(output/%s))\nSELECT o.*, i.*, n.* FROM ORDERS o JOIN ITEMS i ON i.ORDER_ID=o.ID AND i.TENANT=o.TENANT%s JOIN NOTES n ON n.ORDER_ID=o.ID", method, body, output, hint)
				source := &Source{Scope: "github.com/viant/datly/hintfixture/generated", Name: "Orders", Connector: "main", Text: text, Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
				compiled, err := NewCompiler().Compile(ctx, source)
				if err != nil {
					t.Fatal(err)
				}
				relation := compiled.Component.RootView.Relations[0]
				if relation.Cardinality != cardinality || one && relation.View.Cardinality != cardinality || len(relation.On) != 2 || compiled.Component.RootView.Relations[1].Cardinality != spec.CardinalityMany {
					t.Fatalf("canonical relation metadata: %+v", relation)
				}
				generated, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: Options{Handler: tc.handler}})
				if err != nil {
					t.Fatal(err)
				}
				descriptor, found, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+".OrdersView")
				if err != nil || !found {
					t.Fatalf("emitted descriptor: %v %v", found, err)
				}
				fields, err := xshape.New(descriptor, nil).Fields()
				if err != nil {
					t.Fatal(err)
				}
				holder := false
				for _, field := range fields {
					if field.Name == "I" {
						holder = true
						if field.TypeExpr != want {
							t.Fatalf("emitted holder %s want %s", field.TypeExpr, want)
						}
					}
				}
				if !holder {
					t.Fatal("emitted holder missing")
				}
				encoded, err := json.Marshal(compiled.Component)
				if err != nil {
					t.Fatal(err)
				}
				writeSourceFile(t, root, "generated/component_fixture.json", string(encoded))
				schema, err := json.Marshal(hintRuntimeSchema)
				if err != nil {
					t.Fatal(err)
				}
				imports, factory, execute := "", "", hintReaderExecute
				if tc.handler.Operation != "" {
					imports = `druntime "github.com/viant/datly/runtime"; "github.com/viant/datly/runtime/registry"; "github.com/viant/datly/sql/dml"; requestprovider "github.com/viant/bindly/provider/request"; "net/http/httptest"; "strings"`
					factory = "generated,err:=NewOrdersHandler();if err!=nil{t.Fatal(err)}"
					if tc.handler.Target == HandlerGo {
						imports += `;customhandler "github.com/viant/datly/runtime/handler/custom"`
						factory = "generated:=customhandler.New[OrdersInput,OrdersOutput](NewOrdersHandler())"
					}
					if tc.handler.Go.Execution == GoExecutionMutation {
						imports = strings.Replace(imports, "runtime/handler/custom", "runtime/handler/mutation", 1)
					}
					execute = hintWriterExecute
				} else {
					imports = `"github.com/viant/datly/sql/reader"; dsql "github.com/viant/datly/sql"`
				}
				child := `[{"name":"item"}]`
				if one {
					child = `{"name":"item"}`
				}
				consumer := strings.NewReplacer("{{IMPORTS}}", imports, "{{FACTORY}}", factory, "{{EXECUTE}}", execute, "{{SCHEMA}}", string(schema), "{{ONE}}", fmt.Sprint(one), "{{CHILD}}", child).Replace(hintRuntimeConsumer)
				// Execute placeholder replacement also applies to the inserted target body.
				consumer = strings.NewReplacer("{{FACTORY}}", factory, "{{CHILD}}", child).Replace(consumer)
				if tc.handler.Operation == "" {
					// Reload the reader from emitted Go tags, without the original DQL graph.
					consumer = strings.Replace(consumer, "artifact,err:=bootstrap", "component.RootView=nil;artifact,err:=bootstrap", 1)
				}
				writeSourceFile(t, root, "generated/hint_runtime_test.go", consumer)
				cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "-timeout=30s", "./...")
				cmd.Dir = root
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("one=%v generated consumer: %v\n%s", one, err, output)
				}
			}
		})
	}
}

var hintRuntimeSchema = []string{
	"CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY AUTOINCREMENT, TENANT INTEGER NOT NULL, NAME TEXT NOT NULL)",
	"CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL, TENANT INTEGER NOT NULL, NAME TEXT NOT NULL)",
	"CREATE TABLE NOTES(ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL, NAME TEXT NOT NULL)",
}

const hintRuntimeConsumer = `package orders
import("context";"encoding/json";"os";"reflect";"testing";"github.com/viant/bindly/resource";"github.com/viant/datly/bootstrap";"github.com/viant/datly/internal/testharness/sqlite";"github.com/viant/datly/spec";_ "github.com/viant/sqlx/metadata/product/sqlite";{{IMPORTS}})
func TestHintGeneratedRuntime(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t)
 var schema []string;if err:=json.Unmarshal([]byte(` + "`{{SCHEMA}}`" + `),&schema);err!=nil{t.Fatal(err)}
 if err:=db.ExecStatements(ctx,schema...);err!=nil{t.Fatal(err)}
 content,err:=os.ReadFile("component_fixture.json");if err!=nil{t.Fatal(err)}
 component:=&spec.Component{};if err=json.Unmarshal(content,component);err!=nil{t.Fatal(err)}
 resources:=resource.New()
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 holder,_:=reflect.TypeOf(OrdersView{}).FieldByName("I");want:=reflect.Slice;if {{ONE}}{want=reflect.Ptr};if holder.Type.Kind()!=want{t.Fatalf("holder %v",holder.Type)}
 {{EXECUTE}}
}
`
const hintReaderExecute = `
 if err:=db.ExecStatements(ctx,"INSERT INTO ORDERS VALUES(1,7,'parent'),(2,8,'empty')","INSERT INTO ITEMS VALUES(10,1,7,'item'),(11,1,999,'wrong tenant')","INSERT INTO NOTES VALUES(20,1,'note')");err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{}),Plan:artifact.Reader,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 actual,err:=execution.Read(ctx,&OrdersInput{},nil,nil);if err!=nil{t.Fatal(err)}
 rows:=actual.(*OrdersOutput).Data;if len(rows)!=2{t.Fatalf("rows=%d",len(rows))}
 var parent,empty *OrdersView;for _,row:=range rows{if row.Name!=nil&&*row.Name=="parent"{parent=row}else{empty=row}}
 if parent==nil||empty==nil{t.Fatal("missing parent rows")}
 child:=reflect.ValueOf(parent).Elem().FieldByName("I");missing:=reflect.ValueOf(empty).Elem().FieldByName("I")
 if child.Kind()==reflect.Slice{if child.Len()!=1||missing.Len()!=0{t.Fatal("composite relation mismatch")};child=child.Index(0)}else if !missing.IsNil(){t.Fatal("missing to-one child not nil")}
 if child.IsNil()||child.Elem().FieldByName("Name").Elem().String()!="item"||len(parent.N)!=1{t.Fatal("relation match or sibling holder lost")}
`
const hintWriterExecute = `
 {{FACTORY}}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:generated,DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 run:=func(body string)(any,error){request:=httptest.NewRequest("POST","/orders",strings.NewReader(body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{return nil,err};defer scope.Close();return rt.ExecuteRoute(ctx,"POST","/orders",scope)}
 actual,err:=run(` + "`" + `{"Data":[{"tenant":7,"name":"parent","I":{{CHILD}},"N":[{"name":"note"}]},{"tenant":8,"name":"empty"}]}` + "`" + `);if err!=nil{t.Fatal(err)}
 if actual==nil{t.Fatal("missing output")}
 type row struct{ID,Tenant int;Name string};db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,TENANT,NAME FROM ORDERS ORDER BY ID"},[]row{{1,7,"parent"},{2,8,"empty"}})
 type item struct{ID,OrderID,Tenant int;Name string};db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,ORDER_ID AS OrderID,TENANT,NAME FROM ITEMS"},[]item{{1,1,7,"item"}})
 type note struct{ID,OrderID int;Name string};db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,ORDER_ID AS OrderID,NAME FROM NOTES"},[]note{{1,1,"note"}})
 // Native validation must reject an incomplete entity before any buffered DML commits.
 if _,err=run(` + "`" + `{"Data":[{"tenant":7}]}` + "`" + `);err==nil{t.Fatal("missing required name accepted")}
 type count struct{Count int};db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS Count FROM ORDERS"},[]count{{2}})
`
