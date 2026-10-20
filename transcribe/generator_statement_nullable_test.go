package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestGeneratorBufferedStatementNullableUpdateSQLite(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/nullablefixture"}).Write(t, root)
	const source = `#package('pkg/records')
#import('time','time')
#setting($_ = $route('/records','PATCH'))
#define($_ = $ID<int>(param/ID).Required())
#define($_ = $Count<*int>(param/Count).Optional())
#define($_ = $Amount<*float64>(param/Amount).Optional())
#define($_ = $Label<*string>(param/Label).Optional())
#define($_ = $Enabled<*bool>(param/Enabled).Optional())
#define($_ = $Changed<*time.Time>(param/Changed).Optional())
#define($_ = $Data<any>(output/body).WithTag('json:"-"'))
$dml.Execute("UPDATE records SET count = ?, amount = ?, label = ?, enabled = ?, changed = ? WHERE id = ?", $Input.Count, $Input.Amount, $Input.Label, $Input.Enabled, $Input.Changed, $Input.ID)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/nullablefixture/pkg/records", Connector: "main", Text: source}}
	compiled, err := NewCompiler().Compile(context.Background(), request.Source)
	if err != nil {
		t.Fatal(err)
	}
	component, err := json.Marshal(compiled.Component)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := (Generator{Operation: "patch"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	pkg := filepath.Join(root, "pkg/records")
	before, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (Generator{Operation: "patch"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil || string(before) != string(after) {
		t.Fatal("nullable statement generation drift")
	}
	runtime := strings.ReplaceAll(nullableStatementRuntime, "RecordsInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "RecordsOutput", generated.Result.Plan.Output.Type)
	runtime = strings.ReplaceAll(runtime, "COMPONENT_JSON", strconv.Quote(string(component)))
	if err := os.WriteFile(filepath.Join(pkg, "nullable_runtime_test.go"), []byte(runtime), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated nullable update: %v\n%s", err, output)
	}
}

const nullableStatementRuntime = `package records
import (
 "context"
 "database/sql"
 "encoding/json"
 "reflect"
 "strings"
 "testing"
 "time"

 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/custom"
 "github.com/viant/datly/runtime/handler/engine"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/sql/dml"
)
type capturedNullableStatement struct { args []any }
func(c *capturedNullableStatement)Execute(_ string,args ...any)error { c.args=args;return nil }
func nullableInput(id int,count *int,amount *float64,label *string,enabled *bool,changed *time.Time)*RecordsInput {
 input:=&RecordsInput{};input.SetID(id);input.SetCount(count);input.SetAmount(amount);input.SetLabel(label);input.SetEnabled(enabled);input.SetChanged(changed);return input
}
func TestNullableUpdateGeneratedRuntime(t *testing.T) {
 ctx:=context.Background();db:=sqlite.New(t)
 if err:=db.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,count INTEGER,amount REAL,label TEXT,enabled INTEGER,changed TIMESTAMP)","INSERT INTO records(id,count,amount,label,enabled) VALUES(1,5,5.5,'old',1),(2,8,8.5,'second',1)","CREATE TABLE audit(id INTEGER)","CREATE TRIGGER update_audit AFTER UPDATE ON records BEGIN INSERT INTO audit VALUES(NEW.id); END");err!=nil{t.Fatal(err)}
 var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
 handler:=custom.New(NewRecordsHandler());artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:&component,InputType:reflect.TypeFor[RecordsInput](),OutputType:reflect.TypeFor[RecordsOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
 route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/records"});if !ok{t.Fatal("route missing")}
 run:=func(input *RecordsInput)error {_,err:=engine.New().Execute(ctx,engine.Request{Input:route,BoundInput:input,Handler:handler,DataSource:dml.Source{DB:db.DB}});return err}
 check:=func(count *int,amount *float64,label *string,enabled *bool,changed *time.Time){t.Helper();var n sql.NullInt64;var a sql.NullFloat64;var l sql.NullString;var e sql.NullBool;var when sql.NullTime
 if err:=db.DB.QueryRowContext(ctx,"SELECT count,amount,label,enabled,changed FROM records WHERE id=1").Scan(&n,&a,&l,&e,&when);err!=nil{t.Fatal(err)}
 if n.Valid!=(count!=nil)||count!=nil&&n.Int64!=int64(*count)||a.Valid!=(amount!=nil)||amount!=nil&&a.Float64!=*amount||l.Valid!=(label!=nil)||label!=nil&&l.String!=*label||e.Valid!=(enabled!=nil)||enabled!=nil&&e.Bool!=*enabled||when.Valid!=(changed!=nil)||changed!=nil&&!when.Time.Equal(*changed){t.Fatalf("nullable values changed: %v %v %v %v %v",n,a,l,e,when)}
 }
 if err:=run(nullableInput(1,nil,nil,nil,nil,nil));err!=nil{t.Fatal(err)};check(nil,nil,nil,nil,nil)
 n,a,label,enabled,changed:=7,3.25,"value",true,time.Date(2026,10,19,2,3,4,0,time.UTC)
 input:=nullableInput(1,&n,&a,&label,&enabled,&changed)
 captured:=&capturedNullableStatement{};if err:=NewRecordsHandlerQueueStatement(captured,input,&RecordsOutput{});err!=nil{t.Fatal(err)}
 if len(captured.args)!=6||captured.args[0]!=input.Count||captured.args[1]!=input.Amount||captured.args[2]!=input.Label||captured.args[3]!=input.Enabled||captured.args[4]!=input.Changed{t.Fatal("generated statement dereferenced or copied pointer arguments")}
 if err:=run(input);err!=nil{t.Fatal(err)};check(&n,&a,&label,&enabled,&changed)
 n,a,label,enabled,changed=0,0,"",false,time.Time{}
 if err:=run(nullableInput(1,&n,&a,&label,&enabled,&changed));err!=nil{t.Fatal(err)};check(&n,&a,&label,&enabled,&changed)
 if err:=run(nullableInput(999,nil,nil,nil,nil,nil));err!=nil{t.Fatalf("missing-row UPDATE must remain successful: %v",err)}
 var rows,audits int;if err:=db.DB.QueryRowContext(ctx,"SELECT COUNT(*) FROM records").Scan(&rows);err!=nil||rows!=2{t.Fatalf("missing-row UPDATE inserted or removed rows: %d %v",rows,err)}
 if err:=db.DB.QueryRowContext(ctx,"SELECT COUNT(*) FROM audit").Scan(&audits);err!=nil||audits!=3{t.Fatalf("unexpected UPDATE effects: %d %v",audits,err)};check(&n,&a,&label,&enabled,&changed)
 // Both actual generated calls join one invocation-owned journal. A late SQL
 // failure must roll back its earlier successful nullable UPDATE too.
 if err:=db.ExecStatements(ctx,"CREATE TRIGGER reject_second BEFORE UPDATE ON records WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT,'late nullable update failure'); END");err!=nil{t.Fatal(err)}
 data:=dml.NewData(db.DB);if err:=data.BeginInvocation();err!=nil{t.Fatal(err)}
 value:="rolled back";first:=nullableInput(1,nil,nil,&value,nil,nil);second:=nullableInput(2,nil,nil,&value,nil,nil)
 if err:=NewRecordsHandlerQueueStatement(data,first,&RecordsOutput{});err!=nil{t.Fatal(err)}
 if err:=NewRecordsHandlerQueueStatement(data,second,&RecordsOutput{});err!=nil{t.Fatal(err)}
 if err:=data.Complete(ctx,nil);err==nil||!strings.Contains(err.Error(),"late nullable update failure"){t.Fatalf("late failure missing: %v",err)}
 check(&n,&a,&label,&enabled,&changed)
 if err:=db.DB.QueryRowContext(ctx,"SELECT COUNT(*) FROM audit").Scan(&audits);err!=nil||audits!=3{t.Fatalf("early update escaped rollback: %d %v",audits,err)}
 var secondLabel string;if err:=db.DB.QueryRowContext(ctx,"SELECT label FROM records WHERE id=2").Scan(&secondLabel);err!=nil||secondLabel!="second"{t.Fatalf("late row changed: %q %v",secondLabel,err)}
}
`
