package transcribe

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestTranscribedCompositeStructQLPatchSQLite(t *testing.T) {
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		t.Run(string(target), func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE RECORDS(TENANT_ID INTEGER NOT NULL, ID INTEGER NOT NULL, NAME TEXT NOT NULL, ACTIVE BOOLEAN NOT NULL, QUANTITY INTEGER NOT NULL, PRIMARY KEY(TENANT_ID,ID))"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/compositefixture"}).Write(t, root)
			dql := `#setting($_ = $route('/records','PATCH'))
#define($_ = $Records<[]*RecordsView>(body/Data).Cardinality('Many').Required())
#define($_ = $RecordKeys<?>(param/Records).Cardinality('Many') /* ? SELECT TenantId AS TENANT_ID, Id AS ID FROM ` + "`/`" + ` */)
#define($_ = $CurrentRecords<?>(view/CurrentRecords).Cardinality('Many') /* SELECT r.TENANT_ID,r.ID,r.NAME,r.ACTIVE,r.QUANTITY FROM RECORDS r WHERE $criteria.CompositeIn("r", $RecordKeys) ORDER BY r.TENANT_ID,r.ID */)
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT TENANT_ID,ID,NAME,ACTIVE,QUANTITY FROM RECORDS`
			source := &Source{Scope: "github.com/viant/datly/compositefixture/generated", Name: "Records", Connector: "main", Text: dql, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
			request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: target, Operation: WritePatch, Current: "CurrentRecords"}}}
			if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
				t.Fatal(err)
			}
			factory := "handler,err:=NewRecordsHandler();if err!=nil{t.Fatal(err)}"
			imports := ""
			if target == HandlerGo {
				factory = "handler:=customhandler.New[RecordsInput,RecordsOutput](NewRecordsHandler())"
				imports = `customhandler "github.com/viant/datly/runtime/handler/custom"`
			}
			writeSourceFile(t, root, "generated/composite_patch_test.go", strings.NewReplacer("{{FACTORY}}", factory, "{{IMPORTS}}", imports).Replace(compositeStructQLRuntimeSource))
			if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
				t.Fatalf("regenerate: %v", err)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated %s composite PATCH: %v\n%s", target, err, output)
			}
		})
	}
}

const compositeStructQLRuntimeSource = `package records
import (
 "context"
 "encoding/json"
 "fmt"
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
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
 {{IMPORTS}}
)
func(i *RecordsInput)Init(context.Context)error{
 if len(i.RecordKeys)!=len(i.Records)||len(i.CurrentRecords)!=len(i.Records){return fmt.Errorf("tuple filtering cardinality: keys=%+v current=%+v records=%+v",i.RecordKeys,i.CurrentRecords,i.Records)}
 type pair struct{TenantId,Id int64}
 var payload,current []pair
 var keys []struct{TENANT_ID,ID int64}
 for _,part:=range []struct{source,destination any}{{i.Records,&payload},{i.RecordKeys,&keys},{i.CurrentRecords,&current}}{
  raw,err:=json.Marshal(part.source);if err!=nil{return err};if err:=json.Unmarshal(raw,part.destination);err!=nil{return err}
 }
 wanted:=map[[2]int64]bool{}
 for _,record:=range i.Records{
  if record.Has==nil||!record.Has.TenantId||!record.Has.Id{return fmt.Errorf("composite key presence missing: %+v",record)}
 }
 for _,record:=range payload{wanted[[2]int64{record.TenantId,record.Id}]=true}
 for _,key:=range keys{if !wanted[[2]int64{key.TENANT_ID,key.ID}]{return fmt.Errorf("StructQL key not in input: %+v",key)}}
 for _,record:=range current{if !wanted[[2]int64{record.TenantId,record.Id}]{return fmt.Errorf("current-state cross-product leak: %+v",record)}}
 return nil
}
func TestCompositeSparsePatch(t *testing.T){
 type row struct{TenantId,Id int64;Name string;Active bool;Quantity int64}
 initial:=[]row{{0,0,"zero",true,9},{0,1,"cross-a",true,8},{1,0,"cross-b",true,7},{1,1,"one",true,6},{2,1,"other-tenant",true,5}}
 for _,test:=range []struct{name,body string;want []row}{
  {"zero keys and explicit zero false","{\"Data\":[{\"tenantId\":0,\"id\":0,\"quantity\":0,\"active\":false}]}",[]row{{0,0,"zero",false,0},initial[1],initial[2],initial[3],initial[4]}},
  {"reordered sparse tuple subset","{\"Data\":[{\"tenantId\":1,\"id\":1,\"name\":\"changed\"},{\"tenantId\":0,\"id\":0,\"quantity\":0}]}",[]row{{0,0,"zero",true,0},initial[1],initial[2],{1,1,"changed",true,6},initial[4]}},
 }{
  t.Run(test.name,func(t *testing.T){
   ctx:=context.Background();db:=sqlite.New(t)
   if err:=db.ExecStatements(ctx,
    "CREATE TABLE RECORDS(TENANT_ID INTEGER NOT NULL, ID INTEGER NOT NULL, NAME TEXT NOT NULL, ACTIVE BOOLEAN NOT NULL, QUANTITY INTEGER NOT NULL, PRIMARY KEY(TENANT_ID,ID))",
    "INSERT INTO RECORDS VALUES(0,0,'zero',1,9),(0,1,'cross-a',1,8),(1,0,'cross-b',1,7),(1,1,'one',1,6),(2,1,'other-tenant',1,5)",
    "CREATE TRIGGER no_wrong_tenant BEFORE UPDATE ON RECORDS WHEN (OLD.TENANT_ID=0 AND OLD.ID=1) OR (OLD.TENANT_ID=1 AND OLD.ID=0) OR OLD.TENANT_ID=2 BEGIN SELECT RAISE(ABORT,'wrong tuple mutation'); END");err!=nil{t.Fatal(err)}
   holder:=reflect.TypeOf(Component{});field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("missing generated holder")}
   tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatalf("holder tag: %v",err)}
   source:=&bootstrap.RouteSource{HolderType:"Component",FieldName:field.Name,PackageName:"records",PackagePath:holder.PkgPath(),Tag:tag,InputType:"RecordsInput",OutputType:"RecordsOutput"}
   component,err:=source.Resolve(reflect.TypeOf(RecordsInput{}),reflect.TypeOf(RecordsOutput{}));if err!=nil{t.Fatal(err)}
   resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
   artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
   views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
   {{FACTORY}}
   rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(RecordsOutput{}),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
   request:=httptest.NewRequest("PATCH","/records",strings.NewReader(test.body));request.Header.Set("Content-Type","application/json")
   scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
   actual,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);if err!=nil{t.Fatal(err)}
   raw,err:=json.Marshal(actual);if err!=nil{t.Fatal(err)}
   var got,body struct{Data []struct{TenantId,Id int64}}
   if err:=json.Unmarshal(raw,&got);err!=nil{t.Fatal(err)};if err:=json.Unmarshal([]byte(test.body),&body);err!=nil{t.Fatal(err)}
   if !reflect.DeepEqual(got,body){t.Fatalf("output identity/order changed: %s",raw)}
   db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT TENANT_ID AS TenantId,ID AS Id,NAME,ACTIVE,QUANTITY FROM RECORDS ORDER BY TENANT_ID,ID"},test.want)
  })
 }
}
`
