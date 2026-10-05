package transcribe

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedWriterWrappedKeyAliasSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT,preamble TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/wildcardaliasfixture"}).Write(t, root)
	source := `#package('github.com/viant/datly/wildcardaliasfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
SELECT rows.*,rows.preamble AS Caption,type(rows,'Record'),required(rows.root_key)
FROM (SELECT c.id AS root_key,c.name,c.preamble FROM records c) rows`
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/wildcardaliasfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/wrapped_key_test.go", strings.ReplaceAll(wildcardAliasGeneratedRuntime, `"id":`, `"rootKey":`))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated wrapped-key SQLite runtime: %v\n%s", err, out)
	}
}

func TestGeneratedWriterWildcardAliasedNonkeySQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT,preamble TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/wildcardaliasfixture"}).Write(t, root)
	source := `#package('github.com/viant/datly/wildcardaliasfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
SELECT rows.*,rows.preamble AS Caption,type(rows,'Record'),required(rows.id),tag(rows.id,'sqlx:"id,primaryKey=true"') FROM (SELECT c.* FROM records c) rows`
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/wildcardaliasfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/wildcard_alias_test.go", wildcardAliasGeneratedRuntime)
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated wildcard alias runtime: %v\n%s", err, out)
	}
}

const wildcardAliasGeneratedRuntime = `package generated
import("context";"database/sql";"net/http/httptest";"reflect";"strings";"testing";_ "github.com/mattn/go-sqlite3";"github.com/viant/bindly/resource";"github.com/viant/datly/bootstrap";druntime "github.com/viant/datly/runtime";"github.com/viant/datly/runtime/registry";writer "github.com/viant/datly/runtime/handler/writer";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";viewprovider "github.com/viant/datly/sql/reader/provider";dtag "github.com/viant/datly/tag";requestprovider "github.com/viant/bindly/provider/request";"github.com/viant/bindly/locator";_ "github.com/viant/sqlx/metadata/product/sqlite")
func TestWildcardAliasWriter(t *testing.T){for _,tc:=range []struct{name,body,want string;failure bool}{
{"insert",` + "`" + `{"Data":[{"id":"new","name":"inserted","caption":"new caption"}]}` + "`" + `,"new caption",false},
{"update",` + "`" + `{"Data":[{"id":"existing","caption":"changed"}]}` + "`" + `,"changed",false},
{"omitted",` + "`" + `{"Data":[{"id":"existing","name":"updated"}]}` + "`" + `,"original",false},
{"null",` + "`" + `{"Data":[{"id":"existing","caption":null}]}` + "`" + `,"",false},
{"rollback",` + "`" + `{"Data":[{"id":"existing","caption":"partial"},{"id":"reject","name":"reject"}]}` + "`" + `,"original",true},
}{t.Run(tc.name,func(t *testing.T){ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1);if _,err=db.Exec("CREATE TABLE records(id TEXT PRIMARY KEY,name TEXT CHECK(name<>'reject'),preamble TEXT);INSERT INTO records VALUES('existing','old','original')");err!=nil{t.Fatal(err)};holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)};source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"};component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)};resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)};artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)};views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)};handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)};rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};request:=httptest.NewRequest("PATCH","/records",strings.NewReader(tc.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close();_,err=rt.ExecuteRoute(ctx,"PATCH","/records",scope);if (err!=nil)!=tc.failure{t.Fatalf("failure=%v error=%v",tc.failure,err)};if tc.failure&&!strings.Contains(err.Error(),"CHECK constraint failed"){t.Fatalf("expected late SQL constraint failure, got %v",err)};id:="existing";if tc.name=="insert"{id="new"};var caption sql.NullString;if err=db.QueryRow("SELECT preamble FROM records WHERE id=?",id).Scan(&caption);err!=nil{t.Fatal(err)};if caption.String!=tc.want{t.Fatalf("caption=%q want%q",caption.String,tc.want)};if tc.name=="null"&&caption.Valid{t.Fatal("explicit NULL lost")};if tc.name=="update"||tc.name=="rollback"{var name string;if err=db.QueryRow("SELECT name FROM records WHERE id='existing'").Scan(&name);err!=nil||name!="old"{t.Fatal("omitted physical field changed")}}})}}
`
