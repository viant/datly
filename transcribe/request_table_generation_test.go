package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedRequestTableIsInvocationLocal(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE regions_default(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	for _, shape := range []struct{ name, sql, embedded string }{
		{"direct", "SELECT region.* FROM $Unsafe.RegionTable region WHERE region.id=:ID", ""},
		{"embedded", "SELECT region.*,type(region,'RegionView') FROM (${embed:regions.sql}) region", "SELECT id,name FROM $Unsafe.RegionTable WHERE id=:ID"},
		{"nested", "SELECT region.* FROM (SELECT id,name FROM (SELECT id,name FROM $Unsafe.RegionTable WHERE id=:ID) r) region", ""},
		{"mixed", "SELECT region.* FROM (${embed:regions.sql}) region", "SELECT id,name FROM ${schema}.$Unsafe.RegionTable WHERE id=:ID"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/requesttablefixture"}).Write(t, root)
			sql := "#package('pkg/regions')\n#setting($_ = $route('/regions','GET'))\n#setting($_ = $const('schema','main'))\n#define($_ = $RegionTable<string>(query/table).Value('regions_default'))\n#define($_ = $ID<int>(query/id).Value('42'))\n" + shape.sql
			resources := resource.New()
			if err := resources.Register("", fstest.MapFS{"regions.sql": &fstest.MapFile{Data: []byte(shape.embedded)}}); err != nil {
				t.Fatal(err)
			}
			source := &Source{Name: "Regions", Scope: "github.com/viant/datly/requesttablefixture/pkg/regions", Text: sql, Connector: "main", Resources: resources, ColumnRefiner: column.New(column.Connections{"main": h.DB})}
			compiled, err := NewCompiler().Compile(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			view := compiled.Component.RootView
			if view.Source.Table != "" {
				t.Fatalf("request table became instance metadata: %q", view.Source.Table)
			}
			if len(view.Columns) != 2 {
				t.Fatalf("unexpected fields/hidden identity: %+v", view.Columns)
			}
			for _, c := range view.Columns {
				if c.PrimaryKey || c.AutoIncrement || c.Unique || c.Type.Name == "any" {
					t.Fatalf("dynamic output metadata: %+v", c)
				}
			}
			generator := Generator{Operation: "get"}
			request := GenerationRequest{Compiled: compiled, Destination: root}
			generated, err := generator.Generate(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			pkg := filepath.Join(root, "pkg/regions")
			before := sourceHandlerSnapshot(t, pkg)
			for i := 0; i < 2; i++ {
				// Compile again too: discovery must never grow hidden fields or
				// turn its invocation defaults into metadata on later runs.
				request.Compiled, err = NewCompiler().Compile(ctx, source)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := generator.Generate(ctx, request); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, sourceHandlerSnapshot(t, pkg)) {
					t.Fatal("generation was not byte-identical")
				}
			}
			runtime := strings.NewReplacer("INPUT_TYPE", generated.Result.Plan.Input.Type, "OUTPUT_TYPE", generated.Result.Plan.Output.Type).Replace(requestTableRuntimeTest)
			if err := os.WriteFile(filepath.Join(pkg, "request_table_runtime_test.go"), []byte(runtime), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("generated request-table reader: %v\n%s", err, output)
			}
		})
	}
}

const requestTableRuntimeTest = `package regions
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
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 dsql "github.com/viant/datly/sql"
 dtag "github.com/viant/datly/tag"
 xexec "github.com/viant/xdatly/exec"
)
func TestOneInstanceThreeTables(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t)
 for _,name:=range []string{"default","east","west"}{
  if err:=db.ExecStatements(ctx,"CREATE TABLE regions_"+name+"(id INTEGER PRIMARY KEY,name TEXT NOT NULL)",fmt.Sprintf("INSERT INTO regions_%s VALUES(42,'%s'),(99,'decoy')",name,name));err!=nil{t.Fatal(err)}
 }
 holder:=reflect.TypeFor[RegionsComponent]();field,_:=holder.FieldByName("Contract")
 tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"regions",PackagePath:holder.PkgPath(),Tag:tag}).Resolve(reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(RegionsDatlyResourceNamespace,RegionsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[INPUT_TYPE](),OutputType:reflect.TypeFor[OUTPUT_TYPE](),Resources:resources});if err!=nil{t.Fatal(err)}
 if artifact.Component.RootView.Source.Table!=""{t.Fatal("runtime source acquired instance-time table metadata")}
 reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 entry,err:=artifact.Registration(registry.RegisteredComponent{Reader:reader});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 t.Cleanup(func(){rt.Shutdown(ctx)})
 read:=func(t *testing.T,name string){
  target:="/regions";if name!="default"{target+="?table=regions_"+name}
  scope,err:=requestprovider.New(httptest.NewRequest("GET",target,nil));if err!=nil{t.Fatal(err)};defer scope.Close()
  ec:=xexec.New()
  result,err:=rt.ExecuteRoute(xexec.WithContext(ctx,ec),"GET","/regions",scope);if err!=nil{t.Fatal(err)}
  rows:=reflect.ValueOf(result).Elem().FieldByName("Data");if rows.Len()!=1{t.Fatalf("rows: %v",result)}
  row:=rows.Index(0);for row.Kind()==reflect.Pointer{row=row.Elem()}
  value:=row.FieldByName("Name");for value.Kind()==reflect.Pointer{value=value.Elem()}
  if value.String()!=name{t.Fatalf("table=%s got=%s",name,value.String())}
  executions:=0
  for _,metric:=range ec.Metrics{for _,query:=range metric.Executions{
   executions++
   if !strings.Contains(query.SQL,"regions_"+name)||!strings.Contains(query.SQL,"?")||strings.Contains(query.SQL,"$Unsafe")||strings.Contains(query.SQL,":ID"){t.Fatalf("SQL target/binding: %s",query.SQL)}
   if len(query.Args)!=1{t.Fatalf("expected separate ID argument: %v",query.Args)}
   arg:=reflect.ValueOf(query.Args[0]);for arg.Kind()==reflect.Pointer{arg=arg.Elem()}
   if fmt.Sprint(arg.Interface())!="42"{t.Fatalf("ID argument=%v",query.Args[0])}
  }}
  if executions==0{t.Fatal("no executed SQL recorded")}
 }
 for _,name:=range []string{"default","east","west","default","west","east"}{read(t,name)}
 for _,name:=range []string{"default","east","west"}{t.Run(name,func(t *testing.T){t.Parallel();read(t,name)})}
}
`
