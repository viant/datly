package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os/exec"
	"strings"
	"testing"
)

func TestGeneratedAssignedUpdateIdentitySQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/identityfixture"}).Write(t, root)
	source := `#package('github.com/viant/datly/identityfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.id,r.name,type(r,'Record'),writer_identity(r,'assigned-update'),CAST(r.id AS *int),CAST(r.name AS *string),tag(r.id,'sqlx:"id,primaryKey=true,autoincrement=true"') FROM (SELECT id,name FROM records) r`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/identityfixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"get", "post", "put"} {
		copy := *request.Source
		copy.Text = strings.Replace(source, "'/records','PATCH'", "'/records','"+strings.ToUpper(operation)+"'", 1)
		if _, err := (Generator{Operation: operation}).Generate(ctx, GenerationRequest{Destination: t.TempDir(), Source: &copy}); err == nil {
			t.Fatal("unsupported operation accepted", operation)
		}
	}
	writeSourceFile(t, root, "generated/identity_test.go", identityGeneratedRuntime)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated identity runtime %v\n%s", err, output)
	}
}

const identityGeneratedRuntime = `package generated
import("context";"database/sql";"net/http/httptest";"reflect";"strings";"testing";_ "github.com/mattn/go-sqlite3";"github.com/viant/bindly/resource";"github.com/viant/datly/bootstrap";druntime "github.com/viant/datly/runtime";"github.com/viant/datly/runtime/registry";writer "github.com/viant/datly/runtime/handler/writer";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";viewprovider "github.com/viant/datly/sql/reader/provider";dtag "github.com/viant/datly/tag";requestprovider "github.com/viant/bindly/provider/request";"github.com/viant/bindly/locator";_ "github.com/viant/sqlx/metadata/product/sqlite")
func TestGeneratedIdentity(t *testing.T){for _,tc:=range []struct{name,body string;count int}{{"missing assigned",` + "`" + `{"Data":[{"id":999,"name":"none"}]}` + "`" + `,1},{"omitted",` + "`" + `{"Data":[{"name":"new"}]}` + "`" + `,2},{"null",` + "`" + `{"Data":[{"id":null,"name":"new"}]}` + "`" + `,2},{"zero",` + "`" + `{"Data":[{"id":0,"name":"new"}]}` + "`" + `,2},{"existing",` + "`" + `{"Data":[{"id":1,"name":"changed"}]}` + "`" + `,1}} {t.Run(tc.name,func(t *testing.T){ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil {t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1);if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT);INSERT INTO records VALUES(1,'old')");err!=nil {t.Fatal(err)};holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present {t.Fatal(err)};source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"};component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil {t.Fatal(err)};if component.RootView.WriterIdentityPolicy!="assigned-update" {t.Fatal("DQL identity policy lost")};resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil {t.Fatal(err)};artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil {t.Fatal(err)};views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil {t.Fatal(err)};handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil {t.Fatal(err)};rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil {t.Fatal(err)};request:=httptest.NewRequest("PATCH","/records",strings.NewReader(tc.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil {t.Fatal(err)};defer scope.Close();if _,err=rt.ExecuteRoute(ctx,"PATCH","/records",scope);err!=nil {t.Fatal(err)};var count int;if err=db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count);err!=nil||count!=tc.count {t.Fatalf("count%d err%v",count,err)};if tc.name=="existing" {var name string;if err=db.QueryRow("SELECT name FROM records WHERE id=1").Scan(&name);err!=nil||name!="changed" {t.Fatal("matched update lost")}}})}}
`
