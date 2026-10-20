package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratorAuxiliaryRowAccessorsAndNamedDependency(t *testing.T) {
	text := strings.ReplaceAll(auxiliaryOwnerDQL, "'model','github.com/viant/datly/auxownerfixture/model'", "'self','github.com/viant/datly/auxownerfixture/generated'")
	text = strings.ReplaceAll(text, "model.Caller", "self.Caller")
	text = strings.Replace(text, "#import('self'", "#import('model','github.com/viant/datly/auxownerfixture/model')\n#import('self'", 1)
	text = strings.Replace(text, "tag(r.caller,'sqlx:\"-\" json:\"-\"')", "tag(r.caller,'sqlx:\"-\" json:\"-\"'),c.*,type(c,'Child','Children'),CAST(c.id AS int),CAST(c.type_id AS int),e.*,type(e,'model.Audience','External')", 1)
	text += "\nLEFT JOIN (SELECT id,type_id FROM (records)) c ON c.type_id=r.id\nLEFT JOIN (SELECT id AS Name FROM (records)) e ON e.Name=r.id"
	text = strings.Replace(text, "SELECT r.*", "#define($_ = $Callers<?>(view/Callers).Cardinality('Many').TypeName('Caller').WithColumnType('id','int') /* SELECT id,type_id FROM records */)\nSELECT r.*", 1)
	root, source := auxiliaryOwnerFixture(t, text)
	writeSourceFile(t, root, "model/audience.go", "package model\ntype Audience struct { Name string }\n")
	external, err := loaderast.LoadPackageFS(t.Context(), os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Types.RegisterPackage(typecatalog.TypeOriginPackage, external); err != nil {
		t.Fatal(err)
	}
	var previous map[string]string
	for pass := 0; pass < 3; pass++ {
		generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			writeSourceFile(t, root, "generated/lifecycle.go", auxiliaryOwnerHooks)
			continue
		}
		current := map[string]string{}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			current[file.Path] = string(data)
		}
		if previous != nil && !reflect.DeepEqual(previous, current) {
			t.Fatal("auxiliary accessor regeneration drift")
		}
		previous = current
	}
	entities, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, signature := range []string{"func (entity *Record) GetId() int", "func (entity *Record) SetId(value int)", "func (entity *Record) GetCaller() *Caller", "func (entity *Record) SetCaller(value *Caller)", "func (entity *Caller) GetId() int", "func (entity *Caller) SetId(value int)", "func (entity *Record) GetChildren() []*Child", "func (entity *Record) SetChildren(value []*Child)"} {
		if strings.Count(string(entities), signature) != 1 {
			t.Fatalf("missing or duplicate owned accessor %s:\n%s", signature, entities)
		}
	}
	for _, physical := range []string{"OriginalInput", "CaptureInput", "synchronize", "OnInsert", "OnUpdate"} {
		if strings.Contains(string(entities), physical) {
			t.Fatalf("auxiliary accessors acquired physical support %s", physical)
		}
	}
	writeSourceFile(t, root, "generated/accessors_runtime_test.go", `package generated
import ("testing"; "github.com/viant/datly/auxownerfixture/model")
var _ interface { GetChildren() []*Child; SetChildren([]*Child); GetExternal() []*model.Audience; SetExternal([]*model.Audience) } = (*Record)(nil)
func TestAuxiliarySetters(t *testing.T){
 row:=&Record{};row.SetId(17);row.SetNote("body-only")
 if row.Has==nil||!row.Has.Id||!row.Has.Note||row.GetId()!=17||row.GetNote()!="body-only"{t.Fatal("root accessor presence")}
 caller:=&Caller{};caller.SetId(23)
 if caller.Has==nil||!caller.Has.Id||caller.GetId()!=23{t.Fatal("named dependency accessor presence")}
 row.SetCaller(caller);if row.GetCaller()!=caller||!row.Has.Caller{t.Fatal("named pointer setter")}
 row.SetCaller(nil);if row.GetCaller()!=nil||!row.Has.Caller{t.Fatal("nil pointer presence")}
 for _, test:=range []struct{name string;run func(*testing.T)}{
 {"local",func(t *testing.T){
  row:=&Record{};child:=&Child{};values:=[]*Child{child};row.SetChildren(values)
  if row.Has==nil||!row.Has.Children||row.GetChildren()[0]!=child{t.Fatal("local relation presence/identity")}
  values[0]=nil;if row.GetChildren()[0]!=nil{t.Fatal("local slice copied")}
  marker:=row.Has;row.SetId(7);row.SetChildren(nil)
  if row.Has!=marker||!row.Has.Id||!row.Has.Children||row.Children!=nil{t.Fatal("local nil presence")}
  row.SetChildren([]*Child{});if row.Children==nil||len(row.Children)!=0||row.Has!=marker||!row.Has.Id{t.Fatal("local empty slice presence")}
 }},
 {"external",func(t *testing.T){
  row:=&Record{};child:=&model.Audience{};values:=[]*model.Audience{child};row.SetExternal(values)
  if row.Has==nil||!row.Has.External||row.GetExternal()[0]!=child{t.Fatal("external relation presence/identity")}
  values[0]=nil;if row.GetExternal()[0]!=nil{t.Fatal("external slice copied")}
  marker:=row.Has;row.SetId(7);row.SetExternal(nil)
  if row.Has!=marker||!row.Has.Id||!row.Has.External||row.External!=nil{t.Fatal("external nil presence")}
  row.SetExternal([]*model.Audience{});if row.External==nil||len(row.External)!=0||row.Has!=marker||!row.Has.Id{t.Fatal("external empty slice presence")}
 }},
 } {t.Run(test.name,test.run)}
}
`)
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated auxiliary accessor module: %v\n%s", err, output)
	}
}
