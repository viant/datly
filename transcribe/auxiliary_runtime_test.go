package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

var auxiliarySchema = []string{
	"CREATE TABLE ORDERS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)",
	"CREATE TABLE AUXILIARY(ID INTEGER PRIMARY KEY AUTOINCREMENT, ORDER_ID INTEGER NOT NULL, NAME TEXT NOT NULL)",
	"INSERT INTO ORDERS VALUES(1,'before')",
	"INSERT INTO AUXILIARY VALUES(7,1,'lookup')",
	"CREATE TRIGGER no_aux_insert BEFORE INSERT ON AUXILIARY BEGIN SELECT RAISE(ABORT,'auxiliary insert forbidden'); END",
	"CREATE TRIGGER no_aux_update BEFORE UPDATE ON AUXILIARY BEGIN SELECT RAISE(ABORT,'auxiliary update forbidden'); END",
	"CREATE TRIGGER no_aux_delete BEFORE DELETE ON AUXILIARY BEGIN SELECT RAISE(ABORT,'auxiliary delete forbidden'); END",
}

func TestAuxiliaryRelationsGenerateAndRegenerateWithoutMutationSQLite(t *testing.T) {
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		for _, operation := range []WriteOperation{WritePost, WritePut, WritePatch} {
			for _, auxRoot := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/rootAux=%v", target, operation, auxRoot), func(t *testing.T) {
					ctx := context.Background()
					db := sqlite.New(t)
					schema := append([]string(nil), auxiliarySchema...)
					if auxRoot {
						for _, action := range []string{"insert", "update", "delete"} {
							schema = append(schema, fmt.Sprintf("CREATE TRIGGER no_orders_%s BEFORE %s ON ORDERS BEGIN SELECT RAISE(ABORT,'root auxiliary write forbidden'); END", action, strings.ToUpper(action)))
						}
					}
					if err := db.ExecStatements(ctx, schema...); err != nil {
						t.Fatal(err)
					}
					root := t.TempDir()
					(testharness.GeneratedModule{Path: "github.com/viant/datly/auxfixture"}).Write(t, root)
					text := fmt.Sprintf(`#setting($_ = $route('/orders','%s'))
#define($_ = $Orders<[]*OrdersView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentOrders<?>(view/CurrentOrders).Cardinality('Many') /* SELECT ID,NAME FROM ORDERS */)
#define($_ = $CurrentAux<?>(view/CurrentAux).Cardinality('Many') /* SELECT ID,ORDER_ID,NAME FROM (AUXILIARY) */)
#define($_ = $Data<[]*OrdersView>(output/body))
SELECT o.*, Aux.* FROM ORDERS o JOIN (AUXILIARY) Aux ON Aux.ORDER_ID = o.ID`, strings.ToUpper(string(operation)))
					if auxRoot {
						text = strings.Replace(text, "FROM ORDERS o JOIN", "FROM (ORDERS) o JOIN", 1)
					}
					source := &Source{Scope: "github.com/viant/datly/auxfixture/generated", Name: "Orders", Connector: "main", Text: text, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
					compiled, err := NewCompiler().Compile(ctx, source)
					if err != nil {
						t.Fatal(err)
					}
					if len(compiled.Component.RootView.Relations) != 1 || !compiled.Component.RootView.Relations[0].View.Auxiliary {
						t.Fatal("parenthesized table intent lost")
					}
					childIdentity, err := compiled.Component.RootView.Relations[0].View.Identity()
					if err != nil {
						t.Fatal(err)
					}
					options := Options{Handler: HandlerOptions{Target: target, Operation: operation}}
					if operation == WritePatch {
						options.Handler.Current = "CurrentOrders"
						options.Handler.Currents = []CurrentBinding{{ViewIdentity: childIdentity, Param: "CurrentAux"}}
					}
					request := Request{Source: source, Destination: root, Options: options}
					generated, err := NewCompiler().Transcribe(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					foundAuxTag := false
					for _, view := range generated.Result.Plan.Views {
						for _, field := range view.Fields {
							if strings.Contains(field.Tag, "auxiliary=true") {
								foundAuxTag = true
							}
						}
					}
					if !foundAuxTag {
						t.Fatal("generated Go shape lost auxiliary tag")
					}
					if source.Text != text {
						t.Fatal("canonical authored DQL input changed")
					}
					componentJSON, err := json.Marshal(compiled.Component)
					if err != nil {
						t.Fatal(err)
					}
					writeSourceFile(t, root, "generated/component_fixture.json", string(componentJSON))
					schemaJSON, err := json.Marshal(schema)
					if err != nil {
						t.Fatal(err)
					}
					body := `{"Data":[{"id":1,"name":"after","Aux":[{"orderId":999,"name":"business"}]}]}`
					if operation == WritePost {
						body = `{"Data":[{"name":"after","Aux":[{"orderId":999,"name":"business"}]}]}`
					}
					factory := "generated,err:=NewOrdersHandler();if err!=nil{t.Fatal(err)}"
					imports := ""
					if target == HandlerGo {
						factory = "generated:=customhandler.New[OrdersInput,OrdersOutput](NewOrdersHandler())"
						imports = `customhandler "github.com/viant/datly/runtime/handler/custom"`
					}
					testSource := strings.NewReplacer("{{AUX_ROOT}}", fmt.Sprint(auxRoot), "{{IMPORTS}}", imports, "{{FACTORY}}", factory, "{{SCHEMA}}", string(schemaJSON), "{{METHOD}}", strings.ToUpper(string(operation)), "{{BODY}}", body, "{{OPERATION}}", string(operation)).Replace(auxiliaryRuntimeSource)
					writeSourceFile(t, root, "generated/auxiliary_fixture_test.go", testSource)
					if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
						t.Fatalf("regenerate: %v", err)
					}
					command := exec.Command("go", "test", "-race", "-mod=mod", "./...")
					command.Dir = root
					if output, err := command.CombinedOutput(); err != nil {
						t.Fatalf("generated auxiliary %s/%s: %v\n%s", target, operation, err, output)
					}
				})
			}
		}
	}
}

const auxiliaryRuntimeSource = `package orders
import(
 "context"
 "encoding/json"
 "os"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 rhandler "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
 {{IMPORTS}}
)
func TestAuxiliaryBusinessInputAndMutationExclusion(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t)
 var schema []string
 if err:=json.Unmarshal([]byte(` + "`{{SCHEMA}}`" + `),&schema);err!=nil{t.Fatal(err)}
 if err:=db.ExecStatements(ctx,schema...);err!=nil{t.Fatal(err)}
 content,err:=os.ReadFile("component_fixture.json");if err!=nil{t.Fatal(err)}
 component:=&spec.Component{};if err:=json.Unmarshal(content,component);err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 {{FACTORY}}
 checked:=false
 handler:=rhandler.HandlerFunc(func(ctx context.Context,invocation rhandler.Invocation)(any,error){
  input:=invocation.Input.(*OrdersInput)
  if len(input.CurrentAux)!=1 {t.Fatalf("auxiliary read input disappeared: %+v",input)}
  checked=true
  return generated.Execute(ctx,invocation)
 })
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 request:=httptest.NewRequest("{{METHOD}}","/orders",strings.NewReader(` + "`{{BODY}}`" + `));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 actual,err:=rt.ExecuteRoute(ctx,"{{METHOD}}","/orders",scope);if err!=nil{t.Fatal(err)}
 if !checked{t.Fatal("business input was not checked")}
 raw,err:=json.Marshal(actual);if err!=nil{t.Fatal(err)}
 var result struct{Data []struct{Id *int64;Name string;Aux []struct{Id *int64;OrderId int64;Name string}}}
 if err:=json.Unmarshal(raw,&result);err!=nil{t.Fatal(err)}
 if len(result.Data)!=1||len(result.Data[0].Aux)!=1||result.Data[0].Aux[0].Id!=nil||result.Data[0].Aux[0].OrderId!=999||result.Data[0].Aux[0].Name!="business"{t.Fatalf("auxiliary IDs or business data changed: %s",raw)}
 if "{{AUX_ROOT}}"=="true" && "{{OPERATION}}"=="post" && result.Data[0].Id!=nil{t.Fatalf("auxiliary root ID was sequenced: %s",raw)}
 type row struct{ID int;Name string}
 want:=[]row{{1,"after"}};if "{{OPERATION}}"=="post"{want=[]row{{1,"before"},{2,"after"}}}
 if "{{AUX_ROOT}}"=="true"{want=[]row{{1,"before"}}}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,NAME FROM ORDERS ORDER BY ID"},want)
 type auxiliary struct{ID,OrderID int;Name string}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,ORDER_ID AS OrderID,NAME FROM AUXILIARY"},[]auxiliary{{7,1,"lookup"}})
}
`
