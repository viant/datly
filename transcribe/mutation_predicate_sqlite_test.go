package transcribe

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedMutationPredicatesSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id TEXT PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/predicatefixture"}).Write(t, root)
	source := strings.Replace(mutationPredicateDQL, "mutation_predicate(r,7)", "mutation_predicate(r,7),delete_not_found(r,'ignore')", 1)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/predicatefixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"get", "post"} {
		invalid := request
		copy := *request.Source
		invalid.Source = &copy
		invalid.Source.Text = strings.Replace(source, "'/records','PATCH'", "'/records','"+strings.ToUpper(operation)+"'", 1)
		invalid.Source.Text = strings.Replace(invalid.Source.Text, ",delete_not_found(r,'ignore')", "", 1)
		invalid.Destination = t.TempDir()
		if _, err := (Generator{Operation: operation}).Generate(ctx, invalid); err == nil || !strings.Contains(err.Error(), "mutation_predicate") {
			t.Fatalf("unsupported operation %s error=%v", operation, err)
		}
	}
	invalid := request
	copy := *request.Source
	invalid.Source = &copy
	invalid.Source.Text = strings.Replace(source, "mutation_predicate(r,7)", "mutation_predicate(r,8)", 1)
	invalid.Destination = t.TempDir()
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, invalid); err == nil || !strings.Contains(err.Error(), "no predicate inputs") {
		t.Fatalf("missing group error=%v", err)
	}
	writeSourceFile(t, root, "generated/predicate_test.go", mutationPredicateRuntime)
	for _, required := range []bool{false, true} {
		if required {
			request.Source.Text = strings.Replace(source, "(query/expectedOwner).Optional()", "(query/expectedOwner).Required()", 1)
		}
		if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
			t.Fatalf("regeneration required=%v: %v", required, err)
		}
		testSource := mutationPredicateRuntime
		if required {
			testSource = strings.ReplaceAll(testSource, `{desc:"optional omission updates by identity",input:input{body:`, `{desc:"required omission fails before writing",input:input{body:`)
			testSource = strings.Replace(testSource, `expect:expect{title:"changed",remaining:1}},`, `expect:expect{bindingFailure:true,title:"keep",remaining:1}},`, 1)
			testSource = strings.Replace(testSource, `desc:"inactive optional mutation group permits idempotent missing delete",input:input{body:`, `desc:"required scope omission fails for missing delete",input:input{query:"?expectedOwner=old",body:`, 1)
			testSource = strings.Replace(testSource, `desc:"required scope omission fails for missing delete"`, `desc:"active required scope keeps missing deletion strict"`, 1)
			testSource = strings.Replace(testSource, `expect:expect{title:"keep",remaining:1}},
  {desc:"active mutation group keeps`, `expect:expect{identityFailure:true,title:"keep",remaining:1}},
  {desc:"active mutation group keeps`, 1)
			testSource = strings.ReplaceAll(testSource, `query:"?maxAttempt=0"`, `query:"?expectedOwner=old&maxAttempt=0"`)
		}
		writeSourceFile(t, root, "generated/predicate_test.go", testSource)
		command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("generated runtime required=%v: %v\n%s", required, err, output)
		}
	}
}

const mutationPredicateDQL = `#package('github.com/viant/datly/predicatefixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $ExpectedOwner<string>(query/expectedOwner).Optional().WithPredicate(7,'equal','records','owner'))
#define($_ = $MaxAttempt<int>(query/maxAttempt).Optional().WithPredicate(7,'less_or_equal','records','attempt'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.id, r.title, r.owner, r.attempt, r.should_delete,
       type(r,'Record'), mutation_predicate(r,7),
       CAST(r.id AS string), CAST(r.title AS string),
       CAST(r.owner AS *string), CAST(r.attempt AS int),
       CAST(r.should_delete AS bool),
       tag(r.id,'sqlx:"id,primaryKey"'), delete_marker(r.should_delete)
FROM (SELECT id,title,owner,attempt,0 AS should_delete FROM records) r
`

const mutationPredicateRuntime = `package generated

import (
 "context"
 "database/sql"
 "encoding/json"
 "errors"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"

 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 writer "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 xhandler "github.com/viant/xdatly/handler"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestMutationPredicateGenerated(t *testing.T){
 type input struct{query,body string}
 type expect struct{conflict,bindingFailure,identityFailure bool;title string;remaining int}
 type useCase struct{desc string;input input;expect expect}
 for _,test:=range []useCase{
  {desc:"optional omission updates by identity",input:input{body:` + "`" + `{"Data":[{"id":"one","title":"changed"}]}` + "`" + `},expect:expect{title:"changed",remaining:1}},
  {desc:"provided compound equality and range",input:input{query:"?expectedOwner=old&maxAttempt=0",body:` + "`" + `{"Data":[{"id":"one","title":"changed"}]}` + "`" + `},expect:expect{title:"changed",remaining:1}},
  {desc:"mismatched owner cannot update",input:input{query:"?expectedOwner=other",body:` + "`" + `{"Data":[{"id":"one","title":"changed"}]}` + "`" + `},expect:expect{conflict:true,title:"keep",remaining:1}},
  {desc:"unchanged supplied value still checks matching criteria",input:input{query:"?expectedOwner=old",body:` + "`" + `{"Data":[{"id":"one","title":"keep"}]}` + "`" + `},expect:expect{title:"keep",remaining:1}},
  {desc:"unchanged supplied value cannot bypass stale criteria",input:input{query:"?expectedOwner=other",body:` + "`" + `{"Data":[{"id":"one","title":"keep"}]}` + "`" + `},expect:expect{conflict:true,title:"keep",remaining:1}},
  {desc:"explicit empty owner activates predicate",input:input{query:"?expectedOwner=",body:` + "`" + `{"Data":[{"id":"one","title":"changed"}]}` + "`" + `},expect:expect{conflict:true,title:"keep",remaining:1}},
  {desc:"provided zero range permits matching zero",input:input{query:"?maxAttempt=0",body:` + "`" + `{"Data":[{"id":"one","title":"changed"}]}` + "`" + `},expect:expect{title:"changed",remaining:1}},
  {desc:"stale delete cannot remove row",input:input{query:"?expectedOwner=other",body:` + "`" + `{"Data":[{"id":"one","shouldDelete":true}]}` + "`" + `},expect:expect{conflict:true,title:"keep",remaining:1}},
  {desc:"inactive optional mutation group permits idempotent missing delete",input:input{body:` + "`" + `{"Data":[{"id":"missing","shouldDelete":true}]}` + "`" + `},expect:expect{title:"keep",remaining:1}},
  {desc:"active mutation group keeps missing delete strict",input:input{query:"?expectedOwner=old",body:` + "`" + `{"Data":[{"id":"missing","shouldDelete":true}]}` + "`" + `},expect:expect{identityFailure:true,title:"keep",remaining:1}},
  {desc:"matching delete removes row",input:input{query:"?expectedOwner=old",body:` + "`" + `{"Data":[{"id":"one","shouldDelete":true}]}` + "`" + `},expect:expect{remaining:0}},
 }{
  t.Run(test.desc,func(t *testing.T){
   ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
   if _,err=db.Exec("CREATE TABLE records(id TEXT PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER);INSERT INTO records VALUES('one','keep','old',0)");err!=nil{t.Fatal(err)}
   holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatalf("holder: %v",err)}
   source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
   component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
   if component.RootView.MutationPredicateGroup==nil{t.Fatal("generated mutation predicate metadata was lost in bootstrap")}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
   artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
   views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
   handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
   rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
   request:=httptest.NewRequest("PATCH","/records"+test.input.query,strings.NewReader(test.input.body));request.Header.Set("Content-Type","application/json")
   scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
   output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope)
   if test.expect.bindingFailure{if err==nil{t.Fatal("required omission unexpectedly succeeded")}}else if test.expect.identityFailure{if err==nil||!strings.Contains(err.Error(),"matched complete identity"){t.Fatalf("expected strict guarded identity error, got %v",err)}}else if test.expect.conflict{var conflict *xhandler.Conflict;if !errors.As(err,&conflict){t.Fatalf("expected atomic conflict, got %v",err)}}else if err!=nil{t.Fatal(err)}
   var count int;if err=db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count);err!=nil||count!=test.expect.remaining{t.Fatalf("count=%d err=%v",count,err)}
   if count!=0{var title string;if err=db.QueryRow("SELECT title FROM records").Scan(&title);err!=nil||title!=test.expect.title{t.Fatalf("title=%q err=%v",title,err)}}
   if !test.expect.conflict && !test.expect.bindingFailure && !test.expect.identityFailure{if _,err=json.Marshal(output);err!=nil{t.Fatal(err)}}
  })
 }
}
`
