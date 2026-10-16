package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

// Equal canonical types reuse only the Go row; the read remains the owner of
// its query and values. This deliberately supersedes the rejected lineage claim.
func TestOrdinarySharedBodyEqualProjectionQueryOwnership(t *testing.T) {
	for _, tc := range []struct{ name, projection, want string }{
		{"direct", "r.id", "5"}, {"computed", "r.id+1 AS id", "6"}, {"literal", "7 AS id", "7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if base := os.Getenv("ORDINARY_EQUAL_ARTIFACTS"); base != "" {
				root = filepath.Join(base, tc.name)
				if err := os.MkdirAll(root, 0700); err != nil {
					t.Fatal(err)
				}
			}
			(testharness.GeneratedModule{Path: "example.com/equalprojection"}).Write(t, root)
			db := sqlite.New(t)
			if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			text := `#package('example.com/equalprojection/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Data<[]*Record>(output/body))
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('Record').WithColumnType('id','int').WithColumnTag('id','json:"id"') /* SELECT ` + tc.projection + ` FROM records r */)
SELECT r.id,r.should_delete,type(r,'Record'),tag(r.id,'json:"id"'),CAST(r.id AS int),CAST(r.should_delete AS bool),internal_transient(r.should_delete),delete_marker(r.should_delete)
FROM (SELECT src.*,'' AS should_delete FROM records src) r`
			source := &Source{Name: "records", Scope: "example.com/equalprojection/source", Connector: "main", Text: text, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
			compiled, err := NewCompiler().Compile(t.Context(), source)
			if err != nil {
				t.Fatal(err)
			}
			query := compiled.Component.Views[0].Source.SQL
			result, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
			if err != nil {
				t.Fatal(err)
			}
			owners, shared := 0, 0
			for _, v := range result.Result.Plan.Views {
				switch v.Ownership {
				case gen.ViewGenerated:
					owners++
				case gen.ViewSharedBody:
					shared++
				}
			}
			if owners != 1 || shared != 1 {
				t.Fatalf("generated owners=%d shared=%d", owners, shared)
			}
			preserved := false
			for _, f := range result.Result.Files {
				if filepath.Ext(f.Path) == ".sql" {
					raw, err := os.ReadFile(f.Path)
					if err != nil {
						t.Fatal(err)
					}
					if strings.TrimSpace(string(raw)) == strings.TrimSpace(query) {
						preserved = true
					}
				}
			}
			if !preserved || source.Text != text || compiled.Component.Views[0].Source.SQL != query {
				t.Fatal("read SQL/source changed")
			}
			runtime := strings.ReplaceAll(ordinaryEqualProjectionRuntime, "EXPECTED_VALUE", tc.want)
			writeSourceFile(t, root, "generated/equal_projection_native_test.go", runtime)
			command := exec.CommandContext(context.Background(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-run", "^TestEqualProjectionReadOwnsSQL$", "./generated")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			output, err := command.CombinedOutput()
			t.Logf("generated %s native query proof:\n%s", tc.name, output)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("equal canonical int/one row owner, read-owned query preserved=%s", query)
		})
	}
}

const ordinaryEqualProjectionRuntime = `package generated
import(
 "context"
 "database/sql"
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
 rh "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 dsql "github.com/viant/datly/sql"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func TestEqualProjectionReadOwnsSQL(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1);if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY);INSERT INTO records VALUES(5)");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"};component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 observed:=false;handler:=rh.HandlerFunc(func(_ context.Context,inv rh.Invocation)(any,error){in:=inv.Input.(*Input);if len(in.Current)!=1||in.Current[0].Id!=EXPECTED_VALUE||in.Current[0].Has!=nil||in.Current[0].ShouldDelete{t.Fatalf("query result/source defaults changed: %+v",in.Current)};if len(in.Records)!=1||in.Records[0].Id!=99{t.Fatal("read replaced body")};observed=true;return &Output{Data:in.Current},nil})
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,Providers:[]locator.Provider{views}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader("{\"Data\":[{\"id\":99}]}"));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close();if _,err=rt.ExecuteRoute(ctx,"PATCH","/records",scope);err!=nil{t.Fatal(err)};if !observed{t.Fatal("no native read binding")}
 var count,id int;if err=db.QueryRow("SELECT COUNT(*),MAX(id) FROM records").Scan(&count,&id);err!=nil||count!=1||id!=5{t.Fatalf("independent read mutated rows: count=%d id=%d err=%v",count,id,err)}
}
`
