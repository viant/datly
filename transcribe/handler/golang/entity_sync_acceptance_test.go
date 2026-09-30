package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedRecursivePresenceCore(t *testing.T) {
	t.Parallel()
	semantic := recursiveSemanticPlan(plan.OperationPost)
	root := semantic.Root
	child := root.Relations[0].Child
	leaf := child.Relations[0].Child
	names := []string{"Order", "Item", "Detail"}
	var types []RecordType
	for i, record := range []*plan.RecordPlan{root, child, leaf} {
		record.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: names[i]}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Markers"}, Keys: record.Keys, Fields: []plan.EntityField{
			{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
			{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
		}}
		if i < 2 {
			record.Entity.Fields = append(record.Entity.Fields, plan.EntityField{Name: []string{"Items", "Details"}[i], Type: spec.TypeRef{Name: "[]*" + names[i+1]}, Relation: true, Writable: true})
		}
		types = append(types, RecordType{Identity: record.Identity, Path: record.InputPath, Value: "[]*" + names[i]})
	}
	asset, err := EntitySupport(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err != nil {
		t.Fatal(err)
	}
	var source bytes.Buffer
	if err := format.Node(&source, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	(testharness.GeneratedModule{}).Write(t, dir)
	for name, content := range map[string][]byte{"entities.go": source.Bytes(), "entities_test.go": []byte(recursivePresenceCoreContract)} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-race", "-mod=mod", "-timeout=45s", "./...")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated recursive presence: %v\n%s\n%s", err, output, source.String())
	}
}

const recursivePresenceCoreContract = `package events
import("context";"testing";"strings")
type Markers struct{Id,Name,Items,Details bool}
type Order struct{Id *int64;Name string;Items []*Item;Has *Markers}
type Item struct{Id *int64;Name string;Details []*Detail;Has *Markers}
type Detail struct{Id *int64;Name string;Has *Markers}
type Input struct{Orders []*Order}
type Output struct{Data []*Order}
func TestRecursivePresence(t *testing.T){
 for _,mode:=range []string{"deep change","reordered subset","duplicate reference atomicity","duplicate identity atomicity","restore supplied","entity entry"}{t.Run(mode,func(t *testing.T){
  one,two,three:=int64(1),int64(2),int64(3)
  leaf:=&Detail{Id:&three,Name:"leaf",Has:&Markers{Id:true}}
  first:=&Item{Id:&one,Name:"first",Has:&Markers{Id:true},Details:[]*Detail{leaf}}
  second:=&Item{Id:&two,Name:"second",Has:&Markers{Id:true}}
  root:=&Order{Id:&one,Name:"root",Has:&Markers{Id:true},Items:[]*Item{first,second}}
  input:=&Input{Orders:[]*Order{root}}
  if mode=="duplicate identity atomicity"{
   duplicateOne,duplicateTwo:=int64(3),int64(3)
   second.Details=[]*Detail{
    {Id:&duplicateOne,Name:"duplicate one",Has:&Markers{Id:true}},
    {Id:&duplicateTwo,Name:"duplicate two",Has:&Markers{Id:true}},
   }
   if second.Details[0]==second.Details[1]||second.Details[0].Id==second.Details[1].Id{t.Fatal("duplicate fixture must use distinct objects and key pointers")}
  }
  if mode=="restore supplied"{root.Has.Name=true}
  captured,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
  snapshot:=captured.(*_newEventsHandlerOriginalInput)
  switch mode {
  case "deep change": leaf.Name="updated"
  case "reordered subset": root.Items=[]*Item{second};second.Name="updated"
  case "duplicate reference atomicity": root.Name="updated";leaf.Name="updated";root.Items=append(root.Items,first)
  case "duplicate identity atomicity": root.Name="updated";leaf.Name="updated"
  case "restore supplied": root.Has.Name=false
  case "entity entry": root.Name="updated"
  }
  if mode=="entity entry"{
   var absent *Order
   if err:=absent.SyncPresence(snapshot.Roots[0]);err!=nil{t.Fatal(err)}
   if err:=root.SyncPresence(nil);err==nil{t.Fatal("nil original accepted")}
   missing:=snapshot.Roots[0];missing=nil
   if err:=root.SyncPresence(missing);err==nil{t.Fatal("typed nil original accepted")}
   if root.Has.Name{t.Fatal("invalid snapshot changed markers")}
   err=root.SyncPresence(snapshot.Roots[0])
  }else{err=snapshot.SyncPresence(input)}
  if strings.HasPrefix(mode,"duplicate"){
   if err==nil||!strings.Contains(err.Error(),"duplicate"){t.Fatalf("expected duplicate error, got %v",err)}
   if root.Has.Name||root.Has.Items||first.Has.Details||leaf.Has.Name{t.Fatal("error partially changed markers")}
   return
  }
  if err!=nil{t.Fatal(err)}
  if mode=="deep change"{if !leaf.Has.Name||root.Has.Name||root.Has.Items||first.Has.Details{t.Fatal("deep scalar change marked wrong fields")}}
  if mode=="reordered subset"{if !root.Has.Items||!second.Has.Name||root.Has.Name{t.Fatal("membership/scalar marks lost")}}
  if mode=="restore supplied"{if !root.Has.Name||!snapshot.Roots[0].Has("Name"){t.Fatal("original supplied bit lost")}}
  if mode=="entity entry"&&!root.Has.Name{t.Fatal("entity method did not delegate synchronization")}
  if snapshot.Roots[0].original.Name!="root"{t.Fatal("original baseline mutated")}
 })}
}
`
