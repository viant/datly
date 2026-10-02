package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"testing"
)

func TestGeneratedQueryCSVAndRepeatedSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/querylistfixture", SourceModFile: os.Getenv("DATLY_TEST_MODFILE")}).Write(t, root)
	source := `#package('github.com/viant/datly/querylistfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $IDs<[]int>(query/id).Optional().WithPredicate(0,'in','r','id'))
#define($_ = $Data<[]*Record>(output/view))
SELECT r.id,r.name,type(r,'Record'),CAST(r.id AS int),CAST(r.name AS string) FROM (SELECT id,name FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")} ORDER BY id) r`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/querylistfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "get"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/query_test.go", queryListGeneratedRuntime)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated query-list runtime %v\n%s", err, output)
	}
}

const queryListGeneratedRuntime = `package generated
import("context";"database/sql";"net/http/httptest";"reflect";"testing";_ "github.com/mattn/go-sqlite3";"github.com/viant/bindly/resource";"github.com/viant/datly/bootstrap";druntime "github.com/viant/datly/runtime";"github.com/viant/datly/runtime/registry";dsql "github.com/viant/datly/sql";dtag "github.com/viant/datly/tag";requestprovider "github.com/viant/bindly/provider/request";_ "github.com/viant/sqlx/metadata/product/sqlite")
func TestGeneratedQueryLists(t *testing.T){ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil {t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1);if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT);INSERT INTO records VALUES(1001,'one'),(1002,'two'),(1003,'three')");err!=nil {t.Fatal(err)};holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present {t.Fatal(err)};source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"};component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil {t.Fatal(err)};resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil {t.Fatal(err)};artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil {t.Fatal(err)};reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db}});if err!=nil {t.Fatal(err)};rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Reader:reader}},druntime.WithResources(resources));if err!=nil {t.Fatal(err)};for _,query:=range []string{"?id=1001,1002","?id=1001&id=1002","?id=1001,,1002","?id=%201001%20,%201002%20","?id=%5B1001,1002%5D","?id=1001,1002&id=1001"} {scope,err:=requestprovider.New(httptest.NewRequest("GET","/records"+query,nil));if err!=nil {t.Fatal(err)};result,err:=rt.ExecuteRoute(ctx,"GET","/records",scope);scope.Close();if err!=nil {t.Fatal(err)};out:=result.(*Output);if len(out.Data)!=2||out.Data[0].Id!=1001||out.Data[1].Id!=1002 {t.Fatalf("query%s result%+v",query,out)}}}
`
