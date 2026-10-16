package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedIdentifiersSurviveRegenerationAndInstanceChanges(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE RECORDS_SOAK(id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/identifierfixture"}).Write(t, root)
	values, err := constant.New(map[string]string{"suffix": "_SOAK"})
	if err != nil {
		t.Fatal(err)
	}
	const authored = "#package('pkg/records')\n#setting($_ = $route('/records','GET'))\n#setting($_ = $const('suffix','_DEFAULT'))\nSELECT id,name FROM `RECORDS${suffix}`"
	request := GenerationRequest{Destination: root, Source: &Source{
		Name: "Records", Scope: "github.com/viant/datly/identifierfixture/pkg/records", Text: authored,
		Connector: "main", Const: values, ColumnRefiner: column.New(column.Connections{"main": h.DB}),
	}}
	generator := Generator{Operation: "get"}
	result, err := generator.Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, "pkg/records")
	before := sourceHandlerSnapshot(t, pkg)
	for name, content := range before {
		if strings.Contains(content, "_SOAK") {
			t.Fatalf("discovery environment frozen in %s", name)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := generator.Generate(ctx, request); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, sourceHandlerSnapshot(t, pkg)) {
			t.Fatal("regeneration changed generated artifacts")
		}
	}
	runtimeTest := strings.NewReplacer("INPUT_TYPE", result.Result.Plan.Input.Type, "OUTPUT_TYPE", result.Result.Plan.Output.Type).Replace(identifierRuntimeTest)
	if err := os.WriteFile(filepath.Join(pkg, "identifiers_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated instance execution: %v\n%s", err, output)
	}
}

func TestGeneratedBigQueryIdentifiersUseRuntimeProduct(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	// SQLite's quoted table name provides a database-independent execution probe
	// for the exact BigQuery path. Product authority is supplied at bootstrap.
	if err := h.ExecStatements(t.Context(), "CREATE TABLE `viant-soak.scope3.emissions_data`(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/identifierfixture"}).Write(t, root)
	values, err := constant.New(map[string]string{"project": "viant-soak"})
	if err != nil {
		t.Fatal(err)
	}
	const authored = "#package('pkg/records')\n#setting($_ = $route('/records','GET'))\n#setting($_ = $const('project','authored-project'))\nSELECT id,name FROM `${project}.scope3.emissions_data`"
	request := GenerationRequest{Destination: root, Source: &Source{
		Name: "Records", Scope: "github.com/viant/datly/identifierfixture/pkg/records", Text: authored,
		Connector: "main", Const: values, ColumnRefiner: column.New(column.Connections{"main": h.DB}),
	}}
	generator := Generator{Operation: "get"}
	result, err := generator.Generate(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, "pkg/records")
	before := sourceHandlerSnapshot(t, pkg)
	for name, content := range before {
		if strings.Contains(content, "viant-soak") {
			t.Fatalf("discovery environment frozen in %s", name)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := generator.Generate(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, sourceHandlerSnapshot(t, pkg)) {
			t.Fatal("regeneration changed generated artifacts")
		}
	}
	runtimeTest := strings.NewReplacer("INPUT_TYPE", result.Result.Plan.Input.Type, "OUTPUT_TYPE", result.Result.Plan.Output.Type).Replace(identifierBigQueryRuntimeTest)
	if err := os.WriteFile(filepath.Join(pkg, "identifiers_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated BigQuery identifier execution: %v\n%s", err, output)
	}
}

func TestGeneratedWriterIdentifiersUseInstanceConstraints(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE RULES_SOAK(id INTEGER PRIMARY KEY,owner_id INTEGER,code TEXT UNIQUE)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/identifierfixture"}).Write(t, root)
	values, err := constant.New(map[string]string{"suffix": "_SOAK", "Table": "RULES_SOAK"})
	if err != nil {
		t.Fatal(err)
	}
	const authored = `#package('pkg/rules')
#setting($_ = $route('/rules','POST'))
#setting($_ = $const('suffix','_DEFAULT'))
#setting($_ = $const('Table','RULES_DEFAULT'))
SELECT r.*,
 tag(r.code,'sqlx:"code,unique,table=RULES${suffix}"'),
 tag(r.owner_id,'sqlx:"owner_id,refTable=OWNERS${suffix},refColumn=id"')
FROM $Unsafe.Table r`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Rules", Scope: "github.com/viant/datly/identifierfixture/pkg/rules", Text: authored,
		Connector: "main", Const: values, ColumnRefiner: column.New(column.Connections{"main": h.DB})}}
	generator := Generator{Operation: "post"}
	result, err := generator.Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, "pkg/rules")
	before := sourceHandlerSnapshot(t, pkg)
	for name, content := range before {
		if strings.Contains(content, "_SOAK") {
			t.Fatalf("writer discovery environment frozen in %s", name)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := generator.Generate(ctx, request); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, sourceHandlerSnapshot(t, pkg)) {
			t.Fatal("writer regeneration changed generated artifacts")
		}
	}
	runtimeTest := strings.NewReplacer("INPUT_TYPE", result.Result.Plan.Input.Type, "OUTPUT_TYPE", result.Result.Plan.Output.Type).Replace(identifierWriterRuntimeTest)
	if err := os.WriteFile(filepath.Join(pkg, "identifiers_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated writer instance execution: %v\n%s", err, output)
	}
}

const identifierWriterRuntimeTest = `package rules
import (
 "context"
 "fmt"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/constant"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 writer "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/xdatly/response"
)
func TestGeneratedWriterTwoInstances(t *testing.T){
 holder:=reflect.TypeFor[RulesComponent]();field,_:=holder.FieldByName("Contract")
 tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"rules",PackagePath:holder.PkgPath(),Tag:tag}).Resolve(reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(RulesDatlyResourceNamespace,RulesDatlyResources);err!=nil{t.Fatal(err)}
 for _,stage:=range []string{"SOAK","PROD"}{t.Run(stage,func(t *testing.T){t.Parallel()
  ctx:=context.Background();db:=sqlite.New(t)
  owner:=1;if stage=="PROD"{owner=2}
  if err:=db.ExecStatements(ctx,"CREATE TABLE OWNERS_"+stage+"(id INTEGER PRIMARY KEY)",fmt.Sprintf("INSERT INTO OWNERS_%s VALUES(%d)",stage,owner),"CREATE TABLE RULES_"+stage+"(id INTEGER PRIMARY KEY,owner_id INTEGER,code TEXT UNIQUE)","INSERT INTO RULES_"+stage+" VALUES(10,"+fmt.Sprint(owner)+",'existing')");err!=nil{t.Fatal(err)}
  values,err:=constant.New(map[string]string{"suffix":"_"+stage,"Table":"RULES_"+stage});if err!=nil{t.Fatal(err)}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,Const:values,InputType:reflect.TypeFor[INPUT_TYPE](),OutputType:reflect.TypeFor[OUTPUT_TYPE](),Resources:resources});if err!=nil{t.Fatal(err)}
  handler,err:=writer.New(artifact.Component,reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE](),"post");if err!=nil{t.Fatal(err)}
  rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[OUTPUT_TYPE](),Handler:handler,DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
  defer rt.Shutdown(ctx)
  invoke:=func(id,owner int,code string)error{
   req:=httptest.NewRequest("POST","/rules",strings.NewReader(fmt.Sprintf("{\"Data\":[{\"id\":%d,\"ownerId\":%d,\"code\":%q}]}",id,owner,code)));req.Header.Set("Content-Type","application/json")
   scope,err:=requestprovider.New(req);if err!=nil{return err};defer scope.Close()
   _,err=rt.ExecuteRoute(ctx,"POST","/rules",scope);return err
  }
  if err:=invoke(20,owner,"new");err!=nil{t.Fatal(err)}
  if err:=invoke(21,owner+10,"missing-owner");err==nil||response.ErrorStatusCode(err,500)!=422{t.Fatalf("foreign key validation: %v",err)}
  if err:=invoke(22,owner,"existing");err==nil||response.ErrorStatusCode(err,500)!=422{t.Fatalf("unique validation: %v",err)}
  var count int;if err:=db.DB.QueryRow("SELECT COUNT(*) FROM RULES_"+stage).Scan(&count);err!=nil||count!=2{t.Fatalf("writes count=%d err=%v",count,err)}
 })}
}
`

const identifierRuntimeTest = `package records
import (
 "context"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/constant"
 "github.com/viant/datly/internal/testharness/sqlite"
 dsql "github.com/viant/datly/sql"
 dtag "github.com/viant/datly/tag"
)
func TestOneGeneratedBuildTwoInstances(t *testing.T) {
 holder:=reflect.TypeFor[RecordsComponent]()
 field,_:=holder.FieldByName("Contract")
 tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"records",PackagePath:holder.PkgPath(),Tag:tag}).Resolve(reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 for _,stage:=range []string{"SOAK","PROD"}{t.Run(stage,func(t *testing.T){t.Parallel()
  ctx:=context.Background();db:=sqlite.New(t)
  if err:=db.ExecStatements(ctx,"CREATE TABLE RECORDS_"+stage+"(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO RECORDS_"+stage+" VALUES(1,'"+stage+"')");err!=nil{t.Fatal(err)}
  instance,err:=constant.New(map[string]string{"suffix":"_"+stage});if err!=nil{t.Fatal(err)}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,Const:instance,InputType:reflect.TypeFor[INPUT_TYPE](),OutputType:reflect.TypeFor[OUTPUT_TYPE](),Resources:resources});if err!=nil{t.Fatal(err)}
  if artifact.Component.RootView.Source.Table!="RECORDS_"+stage{t.Fatalf("runtime table: %q",artifact.Component.RootView.Source.Table)}
  execution,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
  for i:=0;i<2;i++{
   result,err:=execution.Read(ctx,new(INPUT_TYPE),nil,nil);if err!=nil{t.Fatal(err)}
   rows:=reflect.ValueOf(result).Elem().FieldByName("Data")
   if rows.Len()!=1{t.Fatalf("rows: %v",result)}
   row:=rows.Index(0);for row.Kind()==reflect.Pointer{row=row.Elem()}
   name:=row.FieldByName("Name");for name.Kind()==reflect.Pointer{name=name.Elem()}
   if name.String()!=stage{t.Fatalf("wrong instance row: %v",result)}
  }
 })}
 if strings.Contains(component.RootView.Source.Table,"SOAK")||strings.Contains(component.RootView.Source.Table,"PROD"){t.Fatal("shared metadata mutated")}
}
`

const identifierBigQueryRuntimeTest = `package records
import (
 "reflect"
 "strings"
 "testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/constant"
 "github.com/viant/datly/internal/testharness/sqlite"
 dsql "github.com/viant/datly/sql"
 dtag "github.com/viant/datly/tag"
)
func TestOneBuildTwoBigQueryProjects(t *testing.T) {
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract")
 tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"records",PackagePath:holder.PkgPath(),Tag:tag}).Resolve(reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 for _,project:=range []string{"viant-soak","viant-prod"}{t.Run(project,func(t *testing.T){t.Parallel()
  db:=sqlite.New(t);table:=project+".scope3.emissions_data"
  if err:=db.ExecStatements(t.Context(),"CREATE TABLE ` + "`" + `"+table+"` + "`" + `(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO ` + "`" + `"+table+"` + "`" + ` VALUES(1,'"+project+"')");err!=nil{t.Fatal(err)}
  values,err:=constant.New(map[string]string{"project":project});if err!=nil{t.Fatal(err)}
  input:=bootstrap.ArtifactInput{Component:component,Const:values,InputType:reflect.TypeFor[INPUT_TYPE](),OutputType:reflect.TypeFor[OUTPUT_TYPE](),Resources:resources,
    IdentifierProducts:func(connector string)(string,error){if connector!="main"{t.Fatalf("connector: %q",connector)};return "BigQuery",nil}}
  artifact,err:=bootstrap.BuildArtifact(input);if err!=nil{t.Fatal(err)}
  if strings.Trim(artifact.Component.RootView.Source.Table,"` + "`" + `")!=table{t.Fatalf("metadata: %q",artifact.Component.RootView.Source.Table)}
  execution,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
  for i:=0;i<2;i++{
   result,err:=execution.Read(t.Context(),new(INPUT_TYPE),nil,nil);if err!=nil{t.Fatal(err)}
   rows:=reflect.ValueOf(result).Elem().FieldByName("Data");if rows.Len()!=1{t.Fatalf("rows: %v",result)}
   row:=rows.Index(0);for row.Kind()==reflect.Pointer{row=row.Elem()}
   name:=row.FieldByName("Name");for name.Kind()==reflect.Pointer{name=name.Elem()}
   if name.String()!=project{t.Fatalf("wrong environment row: %v",result)}
  }
 })}
 if !strings.Contains(component.RootView.Source.Table,"${project}"){t.Fatalf("symbolic metadata changed: %q",component.RootView.Source.Table)}
}
`
