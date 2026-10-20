package transcribe

import (
	"context"
	"crypto/sha256"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestWriterUpdateCommandNativeGeneration(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t).DB
	if _, err := db.Exec(`CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,owner TEXT REFERENCES owners(name),enabled BOOLEAN NOT NULL DEFAULT 1,amount INTEGER NOT NULL DEFAULT 7);CREATE TABLE owners(name TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/updatecommand"}).Write(t, root)
	text := `#package('example.com/updatecommand/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.*, type(r,'Record'), writer_action_policy(r,'update') FROM records r
`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "example.com/updatecommand/source", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db})}}
	snapshot := func() map[string][32]byte {
		result := map[string][32]byte{}
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".sql")) {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			result[path] = sha256.Sum256(content)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	first := snapshot()
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	second := snapshot()
	if len(first) != len(second) {
		t.Fatal("artifact count changed")
	}
	policy := false
	for path, digest := range first {
		if second[path] != digest {
			t.Fatalf("unstable artifact %s", path)
		}
		content, _ := os.ReadFile(path)
		if strings.Contains(string(content), "writerActionPolicy=update") {
			policy = true
		}
		if strings.Contains(string(content), "CurrentRecords") {
			t.Fatalf("command generated Current in %s", path)
		}
	}
	if !policy {
		t.Fatal("generated policy missing")
	}
	writeSourceFile(t, root, "generated/update_command_runtime_test.go", updateCommandGeneratedRuntime)
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=mod", "./generated", "-count=1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated runtime: %v\n%s", err, out)
	}
}

const updateCommandGeneratedRuntime = `package generated
import (
 "context"
 "database/sql"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 writer "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func TestGeneratedUpdateCommand(t *testing.T){
 for _,test:=range []struct{name,body,want string; count,audits int;fail bool}{
 {name:"sparse",body:"{\"Data\":[{\"id\":1,\"amount\":0,\"enabled\":false,\"owner\":null}]}",want:"keep",count:2,audits:1},
 {name:"missing invalid FK and null required is SQL no-match",body:"{\"Data\":[{\"id\":99,\"owner\":\"missing\"}]}",want:"keep",count:2},
 {name:"zero key",body:"{\"Data\":[{\"id\":0,\"title\":\"zero changed\"}]}",want:"keep",count:2,audits:1},
 {name:"repeated key and equal value",body:"{\"Data\":[{\"id\":1,\"title\":\"next\"},{\"id\":1,\"title\":\"next\"}]}",want:"next",count:2,audits:2},
 {name:"identity only",body:"{\"Data\":[{\"id\":1}]}",want:"keep",count:2},
 {name:"late FK rolls back earlier update",body:"{\"Data\":[{\"id\":1,\"title\":\"changed\"},{\"id\":0,\"owner\":\"missing\"}]}",want:"keep",count:2,fail:true},
 }{t.Run(test.name,func(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 if _,err=db.Exec("PRAGMA foreign_keys=ON;CREATE TABLE owners(name TEXT PRIMARY KEY);INSERT INTO owners VALUES('old');CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,owner TEXT REFERENCES owners(name),enabled BOOLEAN NOT NULL DEFAULT 1,amount INTEGER NOT NULL DEFAULT 7);INSERT INTO records VALUES(0,'zero','old',1,7),(1,'keep','old',1,7);CREATE TABLE audit(title TEXT);CREATE TRIGGER updated AFTER UPDATE ON records BEGIN INSERT INTO audit VALUES(NEW.title);END");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 if component.RootView.WriterActionPolicy!="update"{t.Fatal("missing update policy")}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 if len(artifact.ViewDependencies)!=0{t.Fatal("command acquired automatic Current dependency")}
 handler,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:handler,DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader(test.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 _,err=rt.ExecuteRoute(ctx,"PATCH","/records",scope)
 if test.fail{if err==nil||!strings.Contains(err.Error(),"FOREIGN KEY constraint failed"){t.Fatalf("expected drain FK: %v",err)}}else if err!=nil{t.Fatal(err)}
 var title string;var count,audits int;if err=db.QueryRow("SELECT title FROM records WHERE id=1").Scan(&title);err!=nil||title!=test.want{t.Fatalf("title=%q err=%v",title,err)}
 db.QueryRow("SELECT COUNT(*) FROM records").Scan(&count);db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audits);if count!=test.count||audits!=test.audits{t.Fatalf("count/audits=%d/%d want=%d/%d",count,audits,test.count,test.audits)}
 if test.name=="sparse"{var amount int;var enabled bool;var owner *string;if err=db.QueryRow("SELECT amount,enabled,owner FROM records WHERE id=1").Scan(&amount,&enabled,&owner);err!=nil||amount!=0||enabled||owner!=nil{t.Fatalf("sparse: %d %v %v %v",amount,enabled,owner,err)}}
 if test.name=="zero key"{if err=db.QueryRow("SELECT title FROM records WHERE id=0").Scan(&title);err!=nil||title!="zero changed"{t.Fatalf("zero: %q %v",title,err)}}
 })}
}
`
