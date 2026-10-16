package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedQuotedUnsafeConstantBinding(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(t.Context(), "CREATE TABLE `viant-mediator.mdp.history`(id INTEGER PRIMARY KEY,recovered REAL)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/unsafebindingfixture"}).Write(t, root)
	const authored = "#package('pkg/history')\n#setting($_ = $route('/history','GET'))\n#define($_ = $HistoryTable<string>(const/frequencyTargetHistoryTable).Value('viant-mediator.mdp.history'))\nSELECT id,recovered FROM `$Unsafe.HistoryTable`"
	request := GenerationRequest{Destination: root, Source: &Source{Name: "History", Scope: "github.com/viant/datly/unsafebindingfixture/pkg/history", Text: authored, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB})}}
	generator := Generator{Operation: "get"}
	result, err := generator.Generate(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, "pkg/history")
	before := sourceHandlerSnapshot(t, pkg)
	for i := 0; i < 2; i++ {
		if _, err := generator.Generate(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, sourceHandlerSnapshot(t, pkg)) {
			t.Fatal("regeneration changed generated artifacts")
		}
	}
	runtimeTest := strings.NewReplacer("INPUT_TYPE", result.Result.Plan.Input.Type, "OUTPUT_TYPE", result.Result.Plan.Output.Type).Replace(unsafeBindingRuntimeTest)
	if err := os.WriteFile(filepath.Join(pkg, "unsafe_binding_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated constant-binding execution: %v\n%s", err, output)
	}
}

const unsafeBindingRuntimeTest = `package history
import (
 "context"
 "fmt"
 "math"
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
 "github.com/viant/datly/runtime/registry"
 dsql "github.com/viant/datly/sql"
 dtag "github.com/viant/datly/tag"
 xexec "github.com/viant/xdatly/exec"
)
func TestHistoryBindingOneBuildTwoConfigurations(t *testing.T) {
 holder:=reflect.TypeFor[HistoryComponent]();field,_:=holder.FieldByName("Contract")
 tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"history",PackagePath:holder.PkgPath(),Tag:tag}).Resolve(reflect.TypeFor[INPUT_TYPE](),reflect.TypeFor[OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(HistoryDatlyResourceNamespace,HistoryDatlyResources);err!=nil{t.Fatal(err)}
 for _,override:=range []bool{false,true}{t.Run(fmt.Sprint(override),func(t *testing.T){t.Parallel()
  db:=sqlite.New(t)
  for _,table:=range []struct{name string;ratio float64}{{"viant-mediator.mdp.history",0.42},{"viant-prod.mdp.history",0.67},{"unrelated-project.mdp.history",0.00005}}{
   if err:=db.ExecStatements(t.Context(),fmt.Sprintf("CREATE TABLE %q(id INTEGER PRIMARY KEY,recovered REAL)",table.name),fmt.Sprintf("INSERT INTO %q VALUES(1,%g)",table.name,table.ratio));err!=nil{t.Fatal(err)}
  }
  constants:=map[string]string{"HistoryTable":"unrelated-project.mdp.history"}
  target,want:="viant-mediator.mdp.history",0.42
  if override{target,want="viant-prod.mdp.history",0.67;constants["frequencyTargetHistoryTable"]=target}
  values,err:=constant.New(constants);if err!=nil{t.Fatal(err)}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,Const:values,InputType:reflect.TypeFor[INPUT_TYPE](),OutputType:reflect.TypeFor[OUTPUT_TYPE](),Resources:resources,IdentifierProducts:func(string)(string,error){return "BigQuery",nil}});if err!=nil{t.Fatal(err)}
  if strings.Trim(artifact.Component.RootView.Source.Table,"\x60")!=target{t.Fatalf("metadata target: %q expected %q",artifact.Component.RootView.Source.Table,target)}
  reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
  entry,err:=artifact.Registration(registry.RegisteredComponent{Reader:reader});if err!=nil{t.Fatal(err)}
  rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
  defer rt.Shutdown(context.Background())
  for i:=0;i<2;i++{
   scope,err:=requestprovider.New(httptest.NewRequest("GET","/history",nil));if err!=nil{t.Fatal(err)}
   ec:=xexec.New();result,err:=rt.ExecuteRoute(xexec.WithContext(t.Context(),ec),"GET","/history",scope);scope.Close();if err!=nil{t.Fatal(err)}
   rows:=reflect.ValueOf(result).Elem().FieldByName("Data");if rows.Len()!=1{t.Fatalf("rows: %v",result)}
   row:=rows.Index(0);for row.Kind()==reflect.Pointer{row=row.Elem()}
   recovered:=row.FieldByName("Recovered");for recovered.Kind()==reflect.Pointer{recovered=recovered.Elem()}
   if math.Abs(recovered.Float()-want)>1e-9{t.Fatalf("recovered=%v want=%v",recovered.Interface(),want)}
   count:=0;for _,metric:=range ec.Metrics{for _,query:=range metric.Executions{
    count++;if !strings.Contains(query.SQL,target)||strings.Contains(query.SQL,"$Unsafe")||strings.Contains(query.SQL,"unrelated-project"){t.Fatalf("executed SQL disagrees with metadata: %s",query.SQL)}
   }};if count==0{t.Fatal("no history SQL executed")}
  }
 })}
 if !strings.Contains(component.RootView.Source.Table,"$Unsafe.HistoryTable"){t.Fatal("symbolic metadata mutated")}
}
`
