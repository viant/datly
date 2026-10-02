package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/testdata/castmodel"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGeneratedLogicalRequestPresence(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[castmodel.Signals]())); err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "Events", Scope: "example.com/logical", Connector: "main", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#import('domain','github.com/viant/datly/transcribe/testdata/castmodel')
#setting($_ = $route('/events','PATCH'))
#setting($_ = $case_format('lc'))
#define($_ = $Events<[]*EventsView>(body/data))
#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /* SELECT id,name FROM events WHERE id IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end) */)
#define($_ = $Data<[]*EventsView>(output/body))
SELECT e.*,CAST(e.category AS '*string'),CAST(e.labels AS '[]string'),CAST(e.enabled AS bool),CAST(e.count AS int),CAST(e.signals AS '*domain.Signals'),tag(e.category,'sqlx:"-"'),tag(e.labels,'sqlx:"-"'),tag(e.enabled,'sqlx:"-"'),tag(e.count,'sqlx:"-"'),tag(e.signals,'sqlx:"-"'),tag(e.hidden,'sqlx:"-" json:"-"'),tag(e.internal,'sqlx:"-" internal:"true"'),tag(e.hiddenformat,'sqlx:"-" format:"-"')
FROM (SELECT id,name,'' AS category,'' AS labels,0 AS enabled,0 AS count,'' AS signals,'' AS hidden,'' AS internal,'' AS hiddenformat FROM events) e`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Current: "CurrentEvents"}}}
	var first map[string]string
	for i := 0; i < 2; i++ {
		if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{}
		err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files[relative] = string(data)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatal("native generation emitted no Go artifacts")
		}
		if i == 0 {
			first = files
		} else if !reflect.DeepEqual(first, files) {
			t.Fatal("native generated artifacts changed on identical transcription")
		}
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "logical_presence_test.go"), []byte(logicalPresenceRuntimeSource), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "-timeout", "60s", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated logical presence: %v\n%s", err, output)
	}
}

const logicalPresenceRuntimeSource = `package events
import (
 "context"
 "database/sql"
 "reflect"
 "testing"
 "github.com/viant/bindly/provider/body"
 "github.com/viant/datly/sql/dml"
 "github.com/viant/datly/transcribe/testdata/castmodel"
 xhandler "github.com/viant/xdatly/handler"
 _ "github.com/mattn/go-sqlite3"
)
func TestLogicalPresenceBindingCaptureAndPersistence(t *testing.T){
 ctx:=context.Background()
 for _,tc:=range []struct{name,request string;present bool}{
 {"omitted","{\"id\":1}",false},
 {"null","{\"id\":1,\"category\":null,\"labels\":null,\"enabled\":false,\"count\":0,\"signals\":null}",true},
 {"empty","{\"id\":1,\"category\":\"\",\"labels\":[],\"enabled\":false,\"count\":0,\"signals\":{}}",true},
 {"value","{\"id\":1,\"category\":\"a\",\"labels\":[\"b\"],\"enabled\":true,\"count\":2,\"signals\":{\"Label\":\"client\"}}",true},
 }{t.Run(tc.name,func(t *testing.T){
 provider,err:=body.New([]byte(tc.request),"application/json",nil);if err!=nil{t.Fatal(err)}
 value,found,err:=provider.Value(ctx,reflect.TypeOf(EventsView{}),"");if err!=nil||!found{t.Fatalf("binding: %v",err)}
 row:=value.(EventsView)
 if row.Has==nil||row.Has.Category!=tc.present||row.Has.Labels!=tc.present||row.Has.Enabled!=tc.present||row.Has.Count!=tc.present||row.Has.Signals!=tc.present{t.Fatalf("presence: %+v",row.Has)}
 if tc.name=="empty"&&(row.Enabled||row.Count!=0||row.Signals==nil||row.Signals.Label!=""){t.Fatalf("empty scalar values: %+v",row)}
 if tc.name=="null"&&row.Signals!=nil{t.Fatal("null structured scalar was not nil")}
 if tc.name=="value"&&(!row.Enabled||row.Count!=2||row.Signals==nil||row.Signals.Label!="client"){t.Fatalf("bound scalar values: %+v",row)}
 for _,hidden:=range []string{"Hidden","Internal","Hiddenformat"}{if _,ok:=reflect.TypeOf(*row.Has).FieldByName(hidden);ok{t.Fatalf("hidden transient marker exposed: %s",hidden)}}
 input:=&EventsInput{Events:[]*EventsView{&row}}
 capturer:=any(NewEventsHandler()).(xhandler.InputCapturer[EventsInput])
 captured,err:=capturer.CaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
 original:=captured.(*_newEventsHandlerOriginalInput).Roots[0]
 fields:=[]string{"Category","Labels","Enabled","Count","Signals"}
 for _,field:=range fields{if original.Has(field)!=tc.present{t.Fatalf("original %s",field)}}
 row.SetCategory(nil);row.SetLabels([]string{"changed"});row.SetEnabled(false);row.SetCount(0);row.SetSignals(&castmodel.Signals{Label:"changed"})
 if !row.Has.Category||!row.Has.Labels||!row.Has.Enabled||!row.Has.Count||!row.Has.Signals{t.Fatal("setters lost logical presence")}
 for _,field:=range fields{if original.Has(field)!=tc.present{t.Fatalf("working mutation changed original %s",field)}}
 db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close()
 if _,err=db.Exec("CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT);INSERT INTO events VALUES(1,'before')");err!=nil{t.Fatal(err)}
 name:="after";row.SetName(&name)
 data:=dml.NewData(db);if err=data.BeginInvocation();err!=nil{t.Fatal(err)}
 if err=data.Update("events",&row);err!=nil{t.Fatal(err)};if err=data.Complete(ctx,nil);err!=nil{t.Fatal(err)}
 var actual string;if err=db.QueryRow("SELECT name FROM events WHERE id=1").Scan(&actual);err!=nil||actual!="after"{t.Fatalf("physical update: %q %v",actual,err)}
 })}
}
`
