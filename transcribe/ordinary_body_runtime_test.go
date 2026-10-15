package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

func TestOrdinarySharedBodyTwoReadsNativeRuntime(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if base := os.Getenv("ORDINARY_BODY_ARTIFACTS"); base != "" {
		root = filepath.Join(base, "two-reads")
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY, type_id INTEGER, entity_id INTEGER, contact_id INTEGER, created INTEGER, updated DATETIME)"); err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(ordinaryBodyCurrentDQL, "CURRENT_ROW_TYPE", "Record", 1)
	text = strings.ReplaceAll(text, ".WithColumnType('contact_id','int')", ".WithColumnType('contact_id','*int')")
	text = strings.ReplaceAll(text, "$Current<?>(view/Current)", "$CurrentRecords<?>(view/CurrentRecords)")
	text = strings.Replace(text, "SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated FROM records r", "SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated FROM records r WHERE r.id IN (1,2)", 1)
	extra := `#define($_ = $RemovalCandidates<?>(view/RemovalCandidates).Cardinality('Many').TypeName('Record').WithColumnType('id','int').WithColumnType('type_id','int').WithColumnType('entity_id','int').WithColumnType('contact_id','*int').WithColumnType('created','int').WithColumnType('updated','int') /* SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated FROM records r WHERE r.id=2 */)
`
	text = strings.Replace(text, "SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated, r.should_delete,", extra+"SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated, r.should_delete,", 1)
	text = strings.Replace(text, "CAST(r.contact_id AS int)", `CAST(r.contact_id AS *int),tag(r.contact_id,'internal:"true" json:"-"')`, 1)
	text = strings.ReplaceAll(text, ".WithColumnType('updated','int')", ".WithColumnType('updated','time.Time')")
	text = strings.Replace(text, "CAST(r.updated AS int)", `CAST(r.updated AS time.Time),tag(r.updated,'json:"updated"')`, 1)
	text = strings.Replace(text, "#setting($_ = $connector", "#import('time','time')\n#setting($_ = $case_format('lc'))\n#setting($_ = $connector", 1)

	source := &Source{Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source", Connector: "main", Text: text, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	var first map[string]string
	for pass := 0; pass < 2; pass++ {
		result, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		owners, shared := 0, 0
		for _, v := range result.Result.Plan.Views {
			if v.Name == "Record" {
				switch v.Ownership {
				case gen.ViewGenerated:
					owners++
					if len(v.SetMarkerFields) == 0 {
						t.Fatal("body presence owner missing")
					}
				case gen.ViewSharedBody:
					shared++
					if v.SharedBodyIdentity == "" || len(v.Fields) != 0 {
						t.Fatal("shared read became independent row owner")
					}
				}
			}
		}
		if owners != 1 || shared != 2 {
			t.Fatalf("owners=%d shared=%d", owners, shared)
		}
		snapshot := map[string]string{}
		for _, f := range result.Result.Files {
			data, e := os.ReadFile(f.Path)
			if e != nil {
				t.Fatal(e)
			}
			snapshot[f.Path] = string(data)
		}
		if pass == 0 {
			first = snapshot
		} else if !reflect.DeepEqual(first, snapshot) {
			t.Fatal("two-pass artifact drift")
		}
	}
	if source.Text != text {
		t.Fatal("source mutated")
	}
	input, err := os.ReadFile(filepath.Join(root, "generated/input.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CurrentRecords", "RemovalCandidates"} {
		if !strings.Contains(string(input), name) || !strings.Contains(string(input), "in="+name) {
			t.Fatal("independent read binding lost", name)
		}
	}
	sqlTexts := []string{}
	entries, err := os.ReadDir(filepath.Join(root, "generated/sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(root, "generated/sql", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sqlTexts = append(sqlTexts, string(raw))
	}
	joined := strings.Join(sqlTexts, "\n")
	if !strings.Contains(joined, "IN (1,2)") || !strings.Contains(joined, "r.id=2") {
		t.Fatal("independent predicates/resources lost", joined)
	}
	writeSourceFile(t, root, "generated/ordinary_body_native_test.go", ordinarySharedBodyRuntime)
	command := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-run", "^TestOrdinarySharedNativeBindingAndActions$", "./generated")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	t.Logf("generated native runtime:\n%s", output)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("two-pass native one-owner artifacts=%d, two independent read resources preserved", len(first))
}

const ordinarySharedBodyRuntime = `package generated
import (
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
 writer "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
// The transient marker is source-owned internal policy, not a public JSON input.
// Exercise the generated public setter through the normal initializer phase.
func(i *Input)Init(context.Context)error{if len(i.Records)==1&&i.Records[0].Id==2{i.Records[0].SetShouldDelete(true)};return nil}
type observeOrdinary struct {*writer.Handler;t *testing.T;seen *Input;pointer *Record}
func(h *observeOrdinary)Execute(ctx context.Context,inv rh.Invocation)(any,error){
 input:=inv.Input.(*Input);h.seen=input
 if reflect.TypeFor[Record]().Field(5).Tag.Get("json")!="updated"{h.t.Fatal("body Updated presentation changed")};if len(input.CurrentRecords)!=2||len(input.RemovalCandidates)!=1{h.t.Fatalf("read cardinality current=%d removal=%d",len(input.CurrentRecords),len(input.RemovalCandidates))}
 if input.CurrentRecords[0].Updated.Year()!=2000||input.CurrentRecords[1].Updated.Year()!=2001{h.t.Fatal("nullable metadata value-time read changed")};if input.CurrentRecords[0].Id!=1||input.CurrentRecords[0].ContactId!=nil||input.CurrentRecords[1].ContactId==nil||*input.CurrentRecords[1].ContactId!=8||input.RemovalCandidates[0].Id!=2{h.t.Fatal("read predicates/nullable values changed")}
 for _,row:=range append(append([]*Record{},input.CurrentRecords...),input.RemovalCandidates...){if row.ShouldDelete||row.Has!=nil{h.t.Fatalf("read synthesized writer-only state: %+v",row)}}
 if len(input.Records)!=1||input.Records[0].Has==nil||!input.Records[0].Has.Id{h.t.Fatal("body presence missing")};h.pointer=input.Records[0]
 result,err:=h.Handler.Execute(ctx,inv)
 if input.Records[0]!=h.pointer{h.t.Fatal("writer replaced canonical body pointer")}
 return result,err
}
func TestOrdinarySharedNativeBindingAndActions(t *testing.T){
 for _,tc:=range []struct{name,body string;count,kind int}{
 {"sparse update","{\"Data\":[{\"id\":1,\"typeId\":9}]}",2,9},
 {"explicit zero","{\"Data\":[{\"id\":1,\"typeId\":0}]}",2,0},
 {"delete","{\"Data\":[{\"id\":2}]}",1,1},
 {"null updated limit","{\"Data\":[{\"id\":1,\"typeId\":9}]}",2,1},
 }{t.Run(tc.name,func(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER,entity_id INTEGER,contact_id INTEGER,created INTEGER,updated DATETIME);INSERT INTO records VALUES(1,1,11,NULL,3,'2000-01-02 03:04:05'),(2,2,22,8,5,'2001-02-03 04:05:06')");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 observed:=&observeOrdinary{Handler:native,t:t}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:observed,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader(tc.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 if tc.name=="null updated limit" {if _,err=db.Exec("UPDATE records SET updated=NULL WHERE id=1");err!=nil{t.Fatal(err)}}
 output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope)
 if tc.name=="null updated limit" {
  if err==nil||!strings.Contains(err.Error(),"unsupported Scan")||observed.seen!=nil{t.Fatalf("value-time NULL limit changed or reached handler: %v",err)}
  var count,kind int;if e:=db.QueryRow("SELECT COUNT(*),MIN(type_id) FROM records").Scan(&count,&kind);e!=nil||count!=2||kind!=1{t.Fatalf("failed binding had effects count=%d kind=%d err=%v",count,kind,e)}
  t.Log("native SQLX NULL-to-value-time limitation retained before handler with zero writer effects; original legacy NULL equivalence OPEN");return
 }
 if err!=nil{t.Fatal(err)};if output.(*Output).Data[0]!=observed.pointer{t.Fatal("output lost body pointer identity")}
 var count,kind,entity int;if err=db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count);err!=nil||count!=tc.count{t.Fatalf("count=%d err=%v",count,err)}
 if err=db.QueryRow("SELECT type_id,entity_id FROM records WHERE id=1").Scan(&kind,&entity);err!=nil||kind!=tc.kind||entity!=11{t.Fatalf("sparse action result kind=%d entity=%d err=%v",kind,entity,err)}
 if tc.name!="delete"&&(!observed.pointer.Has.TypeId||observed.pointer.Has.EntityId){t.Fatal("sparse presence changed")}
 t.Logf("genuine two-read binding/nullability/read Has nil/body pointer/sparse action %s PASS",tc.name)
 })}
}
`
