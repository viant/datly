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

func TestGeneratorBufferedStatementExecuteSQLite(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	const source = `#package('pkg/constraints')
#setting($_ = $route('/constraints/{type}', 'PATCH'))
#define($_ = $ConstraintType<int>(path/type))
#define($_ = $Identity<int64>(output/body))
$dml.Execute("UPDATE constraints SET audience_ids = '' WHERE type = ?", $Input.ConstraintType)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Constraints", Scope: "github.com/viant/datly/genfixture/pkg/constraints", Connector: "main", Text: source}}
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
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("buffered Execute acquired a competing writer")
	}
	pkg := filepath.Join(root, "pkg/constraints")
	before, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (Generator{Operation: "patch"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(pkg, "handler.go"))
	if err != nil || string(before) != string(after) {
		t.Fatal("regeneration changed handler")
	}
	runtime := `package constraints
import("context";"encoding/json";"reflect";"testing";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values";"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/internal/testharness/sqlite";"github.com/viant/datly/runtime/handler/custom";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/sql/dml")
func TestBulkReset(t *testing.T){ctx:=context.Background();h:=sqlite.New(t)
if err:=h.ExecStatements(ctx,"CREATE TABLE constraints(id INTEGER PRIMARY KEY,type INTEGER NOT NULL,audience_ids TEXT NOT NULL)","INSERT INTO constraints VALUES(1,2,'A'),(2,1,'B'),(3,2,'C')");err!=nil{t.Fatal(err)}
var component spec.Component;if err:=json.Unmarshal([]byte(COMPONENT_JSON),&component);err!=nil{t.Fatal(err)}
handler:=custom.New(NewConstraintsHandler());artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:&component,InputType:reflect.TypeFor[ConstraintsInput](),OutputType:reflect.TypeFor[ConstraintsOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/constraints/{type}"});if !ok{t.Fatal("route not compiled")}
run:=func()error{_,err:=engine.New().Execute(ctx,engine.Request{Input:route,Handler:handler,DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{values.New("path",map[string]any{"type":"2"})}});return err}
if err:=run();err!=nil{t.Fatal(err)}
type row struct{ID int;Type int;AudienceIDs string}
h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,type,audience_ids FROM constraints ORDER BY id"},[]row{{1,2,""},{2,1,"B"},{3,2,""}})
if err:=h.ExecStatements(ctx,"UPDATE constraints SET audience_ids='A' WHERE id=1","UPDATE constraints SET audience_ids='C' WHERE id=3","CREATE TRIGGER reject_late BEFORE UPDATE ON constraints WHEN OLD.id=3 AND NEW.audience_ids='' BEGIN SELECT RAISE(ABORT,'late reset failure'); END");err!=nil{t.Fatal(err)}
if err:=run();err==nil{t.Fatal("failed queued UPDATE reported success")}
h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,type,audience_ids FROM constraints ORDER BY id"},[]row{{1,2,"A"},{2,1,"B"},{3,2,"C"}})
}`
	runtime = strings.ReplaceAll(runtime, "ConstraintsInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "ConstraintsOutput", generated.Result.Plan.Output.Type)
	runtime = strings.ReplaceAll(runtime, "COMPONENT_JSON", strconv.Quote(string(component)))
	if err := os.WriteFile(filepath.Join(pkg, "execute_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated bulk reset: %v\n%s", err, output)
	}
}
