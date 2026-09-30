package compile_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestOperationGetRetainsNativeTimestampKeysInWhereAndOrder(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE timestamps(id INTEGER PRIMARY KEY,value TEXT)"); err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	root := t.TempDir()
	const module = "github.com/viant/datly/timestampfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "source"), 0755); err != nil {
		t.Fatal(err)
	}
	source := `#package('api/records')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $AfterSecond<string>(query/afterSecond).Optional())
#define($_ = $AfterNano<int64>(query/afterNano).Optional())
#define($_ = $Maintenance<bool>(query/maintenance).Optional())
#define($_ = $Data<[]*Row>(output/view))
SELECT sorted_records.id AS Identifier,sorted_records.value,type(sorted_records,'Row')
FROM (
 SELECT t.id,t.value FROM timestamps t WHERE 1=1
 #if($Maintenance)
 AND (${View.TimestampSecondsUTC("t.value")} > $AfterSecond OR (${View.TimestampSecondsUTC("t.value")} = $AfterSecond AND ${View.TimestampNanoseconds("t.value")} > $AfterNano))
 #end
 ORDER BY
 #if($Maintenance)
 ${View.TimestampSecondsUTC("t.value")} ASC,${View.TimestampNanoseconds("t.value")} ASC,
 #end
 t.id ASC
) sorted_records`
	if err := os.WriteFile(filepath.Join(root, "source", "reader.dql"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "run", "./cmd/datly", "transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module+"/source")
	command.Dir = repo
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("operation generation: %v\n%s", err, out)
	}
	generated, err := os.ReadFile(filepath.Join(root, "api", "records", "sql", "reader.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`View.TimestampSecondsUTC("t.value")`, `View.TimestampNanoseconds("t.value")`, `#if($Maintenance)`} {
		if !strings.Contains(string(generated), fragment) {
			t.Fatalf("generated source dropped native timestamp expression %q", fragment)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "api", "records", "timestamp_runtime_test.go"), []byte(generatedTimestampRuntime), 0644); err != nil {
		t.Fatal(err)
	}
	command = exec.CommandContext(ctx, "go", "test", "-mod=mod", "./api/records", "-count=1")
	command.Dir = root
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated timestamp runtime: %v\n%s", err, out)
	}
}

const generatedTimestampRuntime = `package records
import("context";"reflect";"testing";"strings"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
 "github.com/viant/datly/sql/builder"
 sqltemplate "github.com/viant/datly/sql/template"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/sqlx/metadata/database"
 "github.com/viant/sqlx/metadata/info"
 xhandler "github.com/viant/xdatly/handler"
)
type binder struct{}
func(binder)Bind(context.Context,any)error{return nil}
func(binder)Lookup(context.Context,xhandler.ValueKey)(any,bool,error){return nil,false,nil}
func TestGeneratedTimestampRuntime(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t);if err:=db.ExecStatements(ctx,"CREATE TABLE timestamps(id INTEGER PRIMARY KEY,value TEXT)","INSERT INTO timestamps VALUES(1,'2025-12-31T23:59:59.999999999Z'),(2,'2026-01-01 00:00:00.000000001'),(3,'2025-12-31 17:00:00.000000002-07:00'),(4,'2026-01-01T00:00:00.999999999Z')");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[ReaderComponent]();field,_:=holder.FieldByName("Contract");tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:"ReaderComponent",FieldName:field.Name,PackageName:"records",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(ReaderDatlyResourceNamespace,ReaderDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Plan:artifact.Reader,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 ordinary:=&Input{};ordinary.SetMaintenance(false);result,err:=execution.Read(ctx,ordinary,binder{},nil);if err!=nil{t.Fatal(err)};if len(result.(*Output).Data)!=4{t.Fatal("ordinary read changed")}
 filtered:=&Input{};filtered.SetMaintenance(true);filtered.SetAfterSecond("2026-01-01 00:00:00");filtered.SetAfterNano(1)
 result,err=execution.Read(ctx,filtered,binder{},func(name string)(any,bool,error){switch strings.ToLower(name){case "aftersecond":return filtered.AfterSecond,true,nil;case "afternano":return filtered.AfterNano,true,nil};return nil,false,nil});if err!=nil{t.Fatal(err)}
 ids:=[]int{};for _,row:=range result.(*Output).Data{value:=reflect.ValueOf(row).Elem().FieldByName("Identifier");if value.Kind()==reflect.Pointer{if value.IsNil(){t.Fatal("identifier missing")};value=value.Elem()};ids=append(ids,int(value.Int()))};if !reflect.DeepEqual(ids,[]int{3,4}){t.Fatalf("generated chronological IDs=%v",ids)}
 program,err:=(sqltemplate.Compiler{Source:artifact.Reader.Root.View.Spec.Source.SQL,InputType:reflect.TypeFor[Input]()}).Compile();if err!=nil{t.Fatal(err)}
 query,err:=builder.NewBuilder().Build(ctx,builder.WithBuilderTemplate(program),builder.WithBuilderInput(reflect.ValueOf(filtered)),builder.WithBuilderDialect(&info.Dialect{Product:database.Product{Name:"mysql"},Placeholder:"?"}),builder.WithBuilderParameterResolver(func(name string)(any,bool,error){switch strings.ToLower(name){case "aftersecond":return filtered.AfterSecond,true,nil;case "afternano":return filtered.AfterNano,true,nil};return nil,false,nil}));if err!=nil{t.Fatal(err)}
 if !strings.Contains(query.SQL,"DATE_SUB(")||!strings.Contains(query.SQL,"REGEXP_LIKE(")||strings.Contains(query.SQL,"$View")||strings.Contains(query.SQL,"GLOB"){t.Fatal("generated evaluated MySQL timestamp SQL invalid")}
}
`
