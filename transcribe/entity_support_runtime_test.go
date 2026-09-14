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
)

func TestGeneratedEntitySettersAndOriginalCapture(t *testing.T) {
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		t.Run(string(target), func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT,enabled BOOLEAN)", "CREATE TABLE children(id INTEGER PRIMARY KEY,event_id INTEGER,name TEXT)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			source := &Source{Name: "Events", Scope: "example.com/entities", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#setting($_ = $route('/events','POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many'))
#define($_ = $Data<[]*EventsView>(output/body))
SELECT e.* FROM events e JOIN children Children ON Children.event_id=e.id`}
			request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: target, Operation: WritePost}}}
			generated, err := NewCompiler().Transcribe(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if generated.Result.Plan.EntitySupport == nil {
				t.Fatal("entity support not emitted")
			}
			factory := `contract:=NewEventsHandler();capturer,ok:=any(contract).(xhandler.InputCapturer[EventsInput]);if !ok{t.Fatal("generated Go capture adapter missing")};captured,err:=capturer.CaptureInput(context.Background(),input)`
			if target == HandlerVelty {
				factory = `contract,err:=NewEventsHandler();if err!=nil{t.Fatal(err)};captured,err:=contract.CaptureInput(context.Background(),input);var _ xhandler.OriginalPresence`
			}
			testSource := strings.ReplaceAll(entitySupportTestSource, "{{CAPTURE}}", factory)
			if err = os.WriteFile(filepath.Join(root, "generated", "entity_support_test.go"), []byte(testSource), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout", "45s", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated %s entity support failed: %v\n%s", target, err, output)
			}
			// A handwritten pointer setter remains authoritative when regenerating.
			custom := `package events
import original "github.com/viant/xdatly/handler"
func(e *EventsView)SetName(value *string){e.Name=value;if e.Has==nil{e.Has=&EventsViewHas{}};e.Has.Name=true}
func(e EventsView)GetName()*string{return e.Name}
func(e *EventsView)SyncPresence(snapshot original.EntitySnapshot[EventsView])error{return snapshot.SyncPresence(e)}
`
			path := filepath.Join(root, "generated", "custom_entity.go")
			if err = os.WriteFile(path, []byte(custom), 0644); err != nil {
				t.Fatal(err)
			}
			if _, err = NewCompiler().Transcribe(ctx, request); err != nil {
				t.Fatal(err)
			}
			preserved, err := os.ReadFile(path)
			if err != nil || string(preserved) != custom {
				t.Fatal("handwritten setter changed")
			}
			command = exec.Command("go", "test", "-mod=mod", "-race", "-timeout", "45s", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("regenerated %s entity support failed: %v\n%s", target, err, output)
			}
		})
	}
}

const entitySupportTestSource = `package events
import("context";"testing";xhandler "github.com/viant/xdatly/handler")
func TestEntityOriginalState(t *testing.T){
 zero,nonzero:=int(0),int(7)
 name:="before"
 child:=&ChildrenView{Name:&name,Has:&ChildrenViewHas{Name:true}}
 supplied:=&EventsView{Id:&zero,Name:&name,Children:[]*ChildrenView{child,nil,child},Has:&EventsViewHas{Id:true,Children:true}}
 sequencedLater:=&EventsView{Id:&nonzero,Children:[]*ChildrenView{},Has:&EventsViewHas{Children:true}}
 missingMarker:=&EventsView{}
 if missingMarker.GetName()!=nil||missingMarker.Has!=nil{t.Fatal("getter changed presence")}
 if supplied.GetId()!=supplied.Id||len(supplied.GetChildren())!=3{t.Fatal("getter changed value/type")}
 input:=&EventsInput{Events:[]*EventsView{supplied,sequencedLater,missingMarker,nil}}
 {{CAPTURE}}
 if err!=nil{t.Fatal(err)}
 original:=captured.(*_newEventsHandlerOriginalInput)
 if len(original.Roots)!=4||original.Roots[3]!=nil{t.Fatal("original collection positions lost")}
 if !original.Roots[0].Has("Id")||!original.Roots[0].Available()||original.Roots[0].keyId!=0{t.Fatal("explicit zero identity lost")}
 if original.Roots[1].Has("Id")||original.Roots[1].keyId!=7{t.Fatal("identity inferred from nonzero value")}
 if original.Roots[2].Available(){t.Fatal("nil original marker was invented")}
 if original.Roots[0].children1[0]==nil||!original.Roots[0].children1[0].Has("Name")||original.Roots[0].children1[1]!=nil{t.Fatal("nested original markers lost")}
 if original.Roots[1].children1==nil||len(original.Roots[1].children1)!=0||original.Roots[2].children1!=nil{t.Fatal("empty/nil relation distinction lost")}
 baseline:=original.Roots[0].original
 if baseline==supplied||baseline.Name==supplied.Name||*baseline.Name!="before"{t.Fatal("business baseline was not detached")}
 if baseline.Name!=baseline.Children[0].Name||baseline.Children[0]!=baseline.Children[2]||baseline.Children[1]!=nil{t.Fatal("business graph aliases/positions lost")}
 if baseline.Has==supplied.Has||!baseline.Has.Id||baseline.Has.Name{t.Fatal("baseline original presence changed")}
 name="after"
 if *baseline.Name!="before"||*baseline.Children[0].Name!="before"{t.Fatal("mutable working values reached processing baseline")}
 *supplied.Id=99;supplied.Has.Id=false;child.Has.Name=false
 supplied.SetName(supplied.Name);missingMarker.SetName(nil)
 off:=false;supplied.SetEnabled(&off);supplied.SetChildren(nil)
 id:=int(101);sequencedLater.SetId(&id)
 if !supplied.Has.Name||!supplied.Has.Enabled||!supplied.Has.Children||!missingMarker.Has.Name||missingMarker.Name!=nil{t.Fatal("typed setters did not mark explicit values")}
 if !original.Roots[0].Has("Id")||original.Roots[0].keyId!=0||original.Roots[0].Has("Name")||!original.Roots[0].children1[0].Has("Name"){t.Fatal("capture aliased working markers or keys")}
 if original.Roots[1].Has("Id")||original.Roots[2].Available(){t.Fatal("later setter changed original identity presence")}
 next,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)};if next.(*_newEventsHandlerOriginalInput)==original{t.Fatal("capture shared invocation state")}
}
func TestPublicEntityRecursiveSync(t *testing.T){
 one,two:=int(1),int(2)
 child:=&ChildrenView{Id:&two,Has:&ChildrenViewHas{Id:true}}
 root:=&EventsView{Id:&one,Children:[]*ChildrenView{child},Has:&EventsViewHas{Id:true}}
 input:=&EventsInput{Events:[]*EventsView{root}}
 {{CAPTURE}}
 if err!=nil{t.Fatal(err)}
 snapshot:=captured.(*_newEventsHandlerOriginalInput)
 rootName,childName:="root mutation","child mutation";root.Name=&rootName;child.Name=&childName
 if err:=root.SyncPresence(snapshot.Roots[0]);err!=nil{t.Fatal(err)}
 if !root.Has.Name||!child.Has.Name{t.Fatal("public recursive synchronization missed mutations")}
 if snapshot.Roots[0].Has("Name")||snapshot.Roots[0].children1[0].Has("Name"){t.Fatal("public sync mutated original presence")}
}
`
