package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedPostExecutionWithPatchRoute(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.DQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	generated, err := (Generator{Operation: "post"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.Settings.Mutation != "post" {
		t.Fatal("transport overwrote mutation policy")
	}
	dir := filepath.Join(root, strings.TrimPrefix(generated.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	runtimeSource := postExecutionPatchRouteRuntime
	if generated.Result.Plan.Resources == nil {
		runtimeSource = strings.Replace(runtimeSource, "if err:=resources.Register(OrdersDatlyResourceNamespace,OrdersDatlyResources);err!=nil{t.Fatal(err)}", "", 1)
	}
	genpatch.Run(t, root, dir, runtimeSource, "-race")
}

const postExecutionPatchRouteRuntime = `package orders

import (
 "context"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"

 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/genpatch"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 writerhandler "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
)

func TestGeneratedInsertOnlyPatchRoute(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t);db.DB.SetMaxOpenConns(1)
 if err:=db.ExecStatements(ctx,genpatch.Schema...);err!=nil{t.Fatal(err)}
 var foreignKeys int;if err:=db.DB.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys);err!=nil||foreignKeys!=1{t.Fatal("foreign keys not enabled",err)}
 holder:=reflect.TypeOf(OrdersComponent{});field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("missing holder")}
 tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:"OrdersComponent",FieldName:field.Name,PackageName:"orders",PackagePath:holder.PkgPath(),Tag:tag,InputType:"OrdersInput",OutputType:"OrdersOutput"}
 component,err:=source.Resolve(reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}));if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(OrdersDatlyResourceNamespace,OrdersDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 var providers []locator.Provider
 if len(artifact.ViewDependencies)>0{views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)};providers=append(providers,views)}
 if artifact.Component.Settings==nil || artifact.Component.Settings.Mutation!="post"{t.Fatal("generated registration lost insert-only policy")}
 handler,err:=writerhandler.New(artifact.Component,reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}),artifact.Component.Settings.Mutation);if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:handler,Providers:providers,DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 invoke:=func(body string)(any,error){
  request:=httptest.NewRequest("PATCH","/orders",strings.NewReader(body));request.Header.Set("Content-Type","application/json")
  scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
  return rt.ExecuteRoute(ctx,"PATCH","/orders",scope)
 }

 _,err=invoke(` + "`" + `{"Data":[{"id":51,"name":"inserted","kindId":7,"start":"2026-09-01T00:00:00Z","end":"2026-09-10T00:00:00Z","Items":[{"id":501,"name":"inserted child"}]}]}` + "`" + `)
 if err!=nil{t.Fatal(err)}
 assertState:=func(){
  t.Helper()
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,NAME FROM ORDERS ORDER BY ID"},[]struct{Id int;Name string}{{1,"before"},{51,"inserted"}})
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,ORDER_ID,NAME FROM ITEMS ORDER BY ID"},[]struct{Id int;OrderId int;Name string}{{10,1,"old"},{501,51,"inserted child"}})
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,NAME FROM ORDER_KINDS ORDER BY ID"},[]struct{Id int;Name string}{{1,"decoy"},{7,"standard"}})
 }
 assertState()
 _,err=invoke(` + "`" + `{"Data":[{"id":1,"name":"must not update","kindId":7,"start":"2026-09-01T00:00:00Z","end":"2026-09-10T00:00:00Z","Items":[{"id":502,"name":"must not insert child"}]}]}` + "`" + `)
 if err==nil{t.Fatal("PATCH transport changed insert-only operation into update")}
 assertState()
 _,err=invoke(` + "`" + `{"Data":[{"id":52,"name":"must rollback","kindId":7,"start":"2026-09-01T00:00:00Z","end":"2026-09-10T00:00:00Z","Items":[{"id":503,"name":"must rollback child"}]},{"id":1,"name":"duplicate","kindId":7,"start":"2026-09-01T00:00:00Z","end":"2026-09-10T00:00:00Z"}]}` + "`" + `)
 if err==nil{t.Fatal("duplicate-ID batch succeeded")}
 assertState()
}
`
