package transcribe

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestNativeReadonlyBodyPresenceSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, auxiliarySchema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/auxfixture"}).Write(t, root)
	source := &Source{Scope: "github.com/viant/datly/auxfixture/generated", Name: "Orders", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#setting($_ = $route('/orders','POST'))
#define($_ = $Orders<[]*OrdersView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentAux<?>(view/CurrentAux).Cardinality('Many') /* SELECT ID,ORDER_ID,NAME FROM (AUXILIARY) */)
#define($_ = $Data<[]*OrdersView>(output/body))
SELECT orders.*, Aux.* FROM ORDERS orders JOIN (SELECT a.* FROM (AUXILIARY) a WHERE a.ID = 7) Aux ON Aux.ORDER_ID = orders.ID`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}}
	generated, err := NewCompiler().Transcribe(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, view := range generated.Result.Plan.Views {
		if view.Name == "OrdersView" {
			for _, marker := range view.SetMarkerFields {
				if marker == "Aux" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("native body auxiliary marker missing")
	}
	compiled, err := NewCompiler().Compile(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.Component.RootView.Relations[0].View.Auxiliary {
		t.Fatal("readonly intent changed")
	}
	componentJSON, err := json.Marshal(compiled.Component)
	if err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/component_fixture.json", string(componentJSON))
	schemaJSON, err := json.Marshal(auxiliarySchema)
	if err != nil {
		t.Fatal(err)
	}
	fixture := strings.NewReplacer("{{AUX_ROOT}}", "false", "{{IMPORTS}}", `writerhandler "github.com/viant/datly/runtime/handler/writer"`, "{{FACTORY}}", `generated,err:=writerhandler.New(artifact.Component,reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}),"post");if err!=nil{t.Fatal(err)}`, "{{SCHEMA}}", string(schemaJSON), "{{METHOD}}", "POST", "{{BODY}}", `{"Data":[{"name":"after","Aux":[{"orderId":999,"name":"business"}]}]}`, "{{OPERATION}}", "post").Replace(auxiliaryRuntimeSource)
	// The native graph reconciles parent links in memory; auxiliary DML remains forbidden.
	fixture = strings.Replace(fixture, "result.Data[0].Aux[0].OrderId!=999", "result.Data[0].Aux[0].OrderId!=2", 1)
	writeSourceFile(t, root, "generated/readonly_runtime_test.go", fixture)
	writeSourceFile(t, root, "generated/readonly_presence_test.go", nativeReadonlyPresenceSource)
	if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-race", "-count=1", "-mod=mod", "-timeout=90s", "./...")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("native readonly body runtime: %v\n%s", err, output)
	}
}

const nativeReadonlyPresenceSource = `package orders
import (
 "context"
 "reflect"
 "testing"
 "github.com/viant/bindly/provider/body"
)
func TestReadonlyRelationPresence(t *testing.T){
 for _,tc:=range []struct{name,wire string;present bool}{
 {"omitted", "{}",false},
 {"null", "{\"Aux\":null}",true},
 {"partial", "{\"Aux\":[{\"name\":\"partial\"}]}",true},
 }{t.Run(tc.name,func(t *testing.T){
 provider,err:=body.New([]byte(tc.wire),"application/json",nil);if err!=nil{t.Fatal(err)}
 value,found,err:=provider.Value(context.Background(),reflect.TypeOf(OrdersView{}),"");if err!=nil||!found{t.Fatalf("binding: %v",err)}
 row:=value.(OrdersView)
 if row.Has==nil||row.Has.Aux!=tc.present{t.Fatalf("Has = %+v",row.Has)}
 if tc.name=="partial"&&(len(row.Aux)!=1||(row.Aux[0].Name==nil||*row.Aux[0].Name!="partial")||row.Aux[0].Has==nil||!row.Aux[0].Has.Name||row.Aux[0].Has.Id||row.Aux[0].Has.OrderId){t.Fatalf("partial relation = %+v",row.Aux)}
 if tc.name!="partial"&&row.Aux!=nil{t.Fatal("nil relation changed")}
 })}
}
`
