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
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestDynamicDQLUpdatesPersistedGoShapeAppendOnly(t *testing.T) {
	t.Parallel()
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(z TEXT,a INTEGER,b TEXT)", "INSERT INTO records VALUES('z',1,'b')"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/resources"}).Write(t, root)
	source := &Source{Scope: "example.com/generated/records", Name: "Records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#setting($_ = $route('/records','GET'))\nSELECT z,a FROM records"}
	generate := func() (*GeneratedPackage, error) {
		copy := *source
		copy.Types = typecatalog.NewCatalog()
		compiler := NewCompiler()
		compiled, err := compiler.Compile(ctx, &copy)
		if err != nil {
			return nil, err
		}
		input, dir, err := generationInput(root, "generated", compiled)
		if err != nil {
			return nil, err
		}
		input.SQLResources = true
		result, err := compiler.generateInputAt(ctx, root, dir, compiled, input)
		if compiled.Component.RootView.Source.SQL == "" {
			t.Fatal("resource emission mutated compiled runtime component")
		}
		return result, err
	}
	initial, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "generated", initial.Result.Plan.ViewDest)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(root, "generated", initial.Result.Plan.Output.Destination)
	outputBefore, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	source.Text = "#setting($_ = $route('/records','GET'))\nSELECT a,b,z FROM records"
	if _, err = generate(); err != nil {
		t.Fatal(err)
	}
	outputAfter, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(outputBefore) != string(outputAfter) {
		t.Fatal("SQL update rewrote stable output shape tags")
	}
	shape, err := (xshape.SourceParser{}).ParseFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, field := range shape.Fields {
		if field.Owner == initial.Result.Plan.Views[0].Name {
			names = append(names, field.Names...)
		}
	}
	if strings.Join(names, ",") != "Z,A,B" {
		t.Fatalf("updated field order %v, initial source %s", names, before)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "reload_test.go"), []byte(resourceReloadSource), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("updated generated module failed: %v\n%s", err, output)
	}
}

const resourceReloadSource = `package records
import(
 "context"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 gateway "github.com/viant/datly/gateway/http"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func TestReloadGeneratedSQL(t *testing.T){
 h:=sqlite.New(t);if err:=h.ExecStatements(context.Background(),"CREATE TABLE records(z TEXT,a INTEGER,b TEXT)","INSERT INTO records VALUES('z',1,'b')");err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(DatlyResourceNamespace,DatlyResources);err!=nil{t.Fatal(err)}
 component:=&spec.Component{Name:"Records",Key:spec.Key{Kind:spec.KindComponent,Name:"Records"},Routes:[]*spec.Route{{Method:"GET",Path:"/records"}}}
 if _,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{})});err==nil{t.Fatal("unregistered generated SQL resources accepted")}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{}),Resources:resources});if err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeOf(RecordsInput{}),OutputType:reflect.TypeOf(RecordsOutput{}),Plan:artifact.Reader,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 runtime,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(RecordsOutput{}),Reader:execution}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 response:=httptest.NewRecorder();gateway.NewHandler(runtime,nil,"test").ServeHTTP(response,httptest.NewRequest("GET","/records",nil));if response.Code!=200{t.Fatal(response.Body.String())}
 var actual struct{Data []struct{A int;B,Z string}};if err=json.Unmarshal(response.Body.Bytes(),&actual);err!=nil{t.Fatal(err)}
 if len(actual.Data)!=1||actual.Data[0].A!=1||actual.Data[0].B!="b"||actual.Data[0].Z!="z"{t.Fatalf("reloaded query returned %+v: %s",actual,response.Body.String())}
}
`
