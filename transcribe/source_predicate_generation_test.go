package transcribe

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

// A real predicate may consume the input that transcription has yet to emit.
// Source signature proof breaks that cycle, but never substitutes for Go build.
func TestSourcePredicateFirstGeneration(t *testing.T) {
	for _, invalidBody := range []bool{false, true} {
		t.Run(map[bool]string{false: "real generated input", true: "invalid body still fails Go build"}[invalidBody], func(t *testing.T) {
			base := t.TempDir()
			const module = "example.com/sourcepredicate"
			(testharness.GeneratedModule{Path: module}).Write(t, base)
			body := "_ = new(records.RecordsInput)"
			if invalidBody {
				body = "_ = records.DoesNotExist"
			}
			writeSourceFile(t, base, "security/predicate.go", `package security
import (
    ctx "context"
    "reflect"
    "fmt"
    records "example.com/sourcepredicate/records"
    criteria "github.com/viant/xdatly/predicate"
)
type Threshold struct{}
var _anchorThreshold = reflect.TypeFor[Threshold]()
func (*Threshold) Compute(_ ctx.Context, value any) (*criteria.Criteria, error) {
    `+body+`
    minimum, _ := value.(int)
    if minimum < 0 { return nil, fmt.Errorf("minimum cannot be negative") }
    if minimum == 0 { minimum = 2 }
    return &criteria.Criteria{Expression: "id >= ?", Placeholders: []any{minimum}}, nil
}

`)
			writeSourceFile(t, base, "records/records.dql", `#package('example.com/sourcepredicate/records')
#import('security','example.com/sourcepredicate/security')
#setting($_ = $route('/records','GET'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('example.com/sourcepredicate/records.RecordsInput'))
#setting($_ = $output_type('example.com/sourcepredicate/records.RecordsOutput'))
#define($_ = $Minimum<int>(query/min).ApplyWhenAbsentPredicate(0,'handler','security.Threshold').Optional())
SELECT records.*, CAST(records.id AS int)
FROM (SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}) records`)
			discovery := &Discovery{BaseDir: base, Include: []string{module + "/records"}, Connector: "main"}
			project, err := discovery.Compile(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(project.Components) != 1 {
				t.Fatalf("components=%d", len(project.Components))
			}
			compiled := project.Components[0]
			if got := compiled.Component.Parameters[0].Predicates[0].Args[0]; got != module+"/security.Threshold" {
				t.Fatalf("handler identity=%s", got)
			}
			input, dir, err := generationInput(base, "records", compiled)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = NewCompiler().generateInputAt(context.Background(), base, dir, compiled, input); err != nil {
				t.Fatal(err)
			}
			if !invalidBody {
				writeSourceFile(t, base, "security/runtime_test.go", sourcePredicateHTTPRuntime)
			}
			command := exec.Command("go", "test", "-mod=mod", "-count=1", "./records", "./security")
			command.Dir = filepath.Clean(base)
			output, err := command.CombinedOutput()
			if invalidBody {
				if err == nil || !strings.Contains(string(output), "undefined: records.DoesNotExist") {
					t.Fatalf("invalid body build error=%v output=%s", err, output)
				}
			} else if err != nil {
				t.Fatalf("real predicate/generated input build: %v\n%s", err, output)
			}
		})
	}
}

const sourcePredicateHTTPRuntime = `package security_test
import (
 "context"
 "database/sql"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "testing"
 records "example.com/sourcepredicate/records"
 security "example.com/sourcepredicate/security"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/datly/application"
 "github.com/viant/datly/bootstrap"
 resources "github.com/viant/datly/bootstrap/resources"
 "github.com/viant/datly/runtime/registry"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/typecatalog"
 "github.com/viant/x"
 xmodule "github.com/viant/x/module"
)
func TestRealSourcePredicateHTTP(t *testing.T) {
 ctx:=context.Background()
 db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY);INSERT INTO records VALUES(1),(2),(3)");err!=nil{t.Fatal(err)}
 types:=typecatalog.NewCatalog()
 for _,typ:=range []reflect.Type{reflect.TypeFor[security.Threshold](),reflect.TypeFor[records.RecordsInput](),reflect.TypeFor[records.RecordsOutput]()} {
  if err=types.Register(typecatalog.TypeOriginPackage,x.NewType(typ));err!=nil{t.Fatal(err)}
 }
 workspace,err:=(xmodule.LocalWorkspace{BaseDir:".."}).Resolve(ctx);if err!=nil{t.Fatal(err)}
 routes,err:=(bootstrap.PackageDiscovery{Workspace:workspace,Include:[]string{"example.com/sourcepredicate/records"}}).Discover(ctx);if err!=nil||len(routes)!=1{t.Fatalf("routes=%d err=%v",len(routes),err)}
 component,err:=routes[0].Resolve(reflect.TypeFor[records.RecordsInput](),reflect.TypeFor[records.RecordsOutput]());if err!=nil{t.Fatal(err)}
 assets,err:=(resources.Loader{Workspace:workspace,Packages:[]string{"example.com/sourcepredicate/records"}}).Load(ctx);if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,Types:types,Resources:assets.Store,InputType:reflect.TypeFor[records.RecordsInput](),OutputType:reflect.TypeFor[records.RecordsOutput]()});if err!=nil{t.Fatal(err)}
 reader,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 manager,err:=application.New(types);if err!=nil{t.Fatal(err)}
 if err=manager.Reload(ctx,application.Request{Revision:1,Compile:func(context.Context,*typecatalog.Catalog)(*application.Build,error){return &application.Build{Types:types,Resources:assets.Store,Components:[]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[records.RecordsOutput](),Reader:reader}}},nil}});err!=nil{t.Fatal(err)}
 for _,tc:=range []struct{query string;want []int}{{"",[]int{2,3}},{"?min=3",[]int{3}}} {
  response:=httptest.NewRecorder();manager.ServeHTTP(response,httptest.NewRequest("GET","/records"+tc.query,nil));if response.Code!=200{t.Fatalf("HTTP %d: %s",response.Code,response.Body.String())}
  var body struct{Data []struct{Id int}};if err=json.Unmarshal(response.Body.Bytes(),&body);err!=nil{t.Fatal(err)}
  got:=[]int{};for _,row:=range body.Data{got=append(got,row.Id)};if !reflect.DeepEqual(got,tc.want){t.Fatalf("query=%s got=%v want=%v body=%s",tc.query,got,tc.want,response.Body.String())}
 }
 response:=httptest.NewRecorder();manager.ServeHTTP(response,httptest.NewRequest("GET","/records?min=-1",nil));if response.Code!=500{t.Fatalf("negative request HTTP %d: %s",response.Code,response.Body.String())}
}
`
