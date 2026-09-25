package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestEntityProcessingBaselineSelectedGraph(t *testing.T) {
	t.Parallel()
	for _, pointer := range []bool{false, true} {
		name := "value"
		prefix := ""
		if pointer {
			name = "pointer"
			prefix = "*"
		}
		t.Run(name, func(t *testing.T) {
			semantic := recursiveSemanticPlan(plan.OperationPost)
			root := semantic.Root
			child := root.Relations[0].Child
			leaf := child.Relations[0].Child
			records := []*plan.RecordPlan{root, child, leaf}
			names := []string{"Order", "Item", "Detail"}
			var types []RecordType
			for i, record := range records {
				record.Entity = &plan.EntityPlan{MarkerField: "Has", MarkerPointer: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}}, {Name: "Name", Type: spec.TypeRef{Name: "*string"}}, {Name: "Meta", Type: spec.TypeRef{Name: "map[string][]int"}}, {Name: "Opaque", Type: spec.TypeRef{Name: "any"}}}}
				if i < 2 {
					holder := []string{"Items", "Details"}[i]
					record.Entity.Fields = append(record.Entity.Fields, plan.EntityField{Name: holder, Type: spec.TypeRef{Name: "[]" + prefix + names[i+1]}, Relation: true})
				}
				types = append(types, RecordType{Identity: record.Identity, Path: record.InputPath, Value: "[]" + prefix + names[i]})
			}
			asset, err := EntitySupport(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
			if err != nil {
				t.Fatal(err)
			}
			var source bytes.Buffer
			if err = format.Node(&source, token.NewFileSet(), asset.File); err != nil {
				t.Fatal(err)
			}
			rootDir := t.TempDir()
			(testharness.GeneratedModule{}).Write(t, rootDir)
			contract := strings.ReplaceAll(baselineGraphContract, "{{P}}", prefix)
			for file, content := range map[string][]byte{"entities.go": source.Bytes(), "entities_test.go": []byte(contract)} {
				if err = os.WriteFile(filepath.Join(rootDir, file), content, 0644); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "test", "-mod=mod", "-race", "./...")
			command.Dir = rootDir
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated baseline: %v\n%s\n%s", err, output, source.Bytes())
			}
		})
	}
}

const baselineGraphContract = `package events
import("context";"testing";"net/http";"sync")
type Markers struct{Id,Name,Meta,Opaque,Items,Details bool}
type Order struct{Id *int64;Name *string;Meta map[string][]int;Opaque any;Items []{{P}}Item;Has *Markers;Service func();Request *http.Request;lock sync.Mutex}
type Item struct{Id *int64;Name *string;Meta map[string][]int;Opaque any;Details []{{P}}Detail;Has *Markers;Service func()}
type Detail struct{Id *int64;Name *string;Meta map[string][]int;Opaque any;Has *Markers;Service func()}
type Input struct{Orders []{{P}}Order;Service func()}
type Output struct{Data []{{P}}Order}
func TestSelectedGraph(t *testing.T){
 name:="before";meta:=map[string][]int{"values":{1,2}}
 input:=&Input{Orders:[]{{P}}Order{{Name:&name,Meta:meta,Has:&Markers{Name:true},Service:func(){},Items:[]{{P}}Item{{Name:&name,Meta:meta,Details:[]{{P}}Detail{{Name:&name,Meta:meta,Has:&Markers{Meta:true}}}}}}},Service:func(){}}
 value,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 snapshot:=value.(*_newEventsHandlerOriginalInput);baseline:=snapshot.Roots[0].original
 name="after";meta["values"][0]=99
 if *baseline.Name!="before"||*baseline.Items[0].Name!="before"||*baseline.Items[0].Details[0].Name!="before"{t.Fatal("value/pointer relation baseline aliased input")}
 if baseline.Name!=baseline.Items[0].Details[0].Name||baseline.Meta["values"][0]!=1{t.Fatal("selected shared graph was not detached intact")}
 baseline.Meta["values"][0]=17
 if baseline.Items[0].Details[0].Meta["values"][0]!=17{t.Fatal("selected map alias lost")}
 if baseline.Service!=nil||baseline.Request!=nil||baseline.Items[0].Service!=nil{t.Fatal("transient service entered processing baseline")}
 if baseline.Has==input.Orders[0].Has||!baseline.Has.Name||!baseline.Items[0].Details[0].Has.Meta{t.Fatal("original markers not captured independently")}
 input.Orders[0].Opaque=func(){}
 if invalid,err:=_newEventsHandlerCaptureInput(context.Background(),input);err==nil||invalid!=nil{t.Fatal("opaque selected business state did not fail closed")}
}
`
