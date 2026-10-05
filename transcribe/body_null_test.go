package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBodyNullPolicyOverlay(t *testing.T) {
	result := overlayParam(&spec.Parameter{Name: "View", Source: spec.BindSource{Kind: "body"}}, &spec.Parameter{Name: "View", Source: spec.BindSource{Kind: "body"}, BodyNullPolicy: "empty-record"})
	if result.BodyNullPolicy != "empty-record" {
		t.Fatal("overlay dropped policy")
	}
}
func TestGeneratedBodyNullPolicySQLShapeReloadEvolution(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	const module = "github.com/viant/datly/testfixture/nullbody"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','PATCH'))
#setting($_ = $mcp('Records'))
#define($_ = $View<*RecordsView>(body/).Required().WithBodyNullPolicy('empty-record'))
#define($_ = $Data<*RecordsView>(output/body))
SELECT r.* FROM records r`}
	generator := Generator{Operation: "patch"}
	var oldInput string
	for revision := 0; revision < 3; revision++ {
		if revision == 2 {
			if err := db.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN note TEXT"); err != nil {
				t.Fatal(err)
			}
		}
		generated, err := generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		inputFile := filepath.Join(root, "records", generated.Result.Plan.Input.Destination)
		input, err := os.ReadFile(inputFile)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(input), "bodyNullPolicy=empty-record") {
			t.Fatalf("generated input lost policy:\n%s", input)
		}
		if revision == 1 && oldInput != string(input) {
			t.Fatal("stable second pass changed input")
		}
		oldInput = string(input)
		view, err := os.ReadFile(filepath.Join(root, "records", generated.Result.Plan.ViewDest))
		if err != nil {
			t.Fatal(err)
		}
		if revision == 2 && !strings.Contains(string(view), "Note") {
			t.Fatalf("SQL schema change not reflected:\n%s", view)
		}
		holders, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{module + "/records"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(holders) != 1 {
			t.Fatalf("holders=%d", len(holders))
		}
		inputType, found, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Input.Type)
		if err != nil || !found {
			t.Fatalf("input resolution %v %v", found, err)
		}
		outputType, found, err := generated.Types.Resolve(typecatalog.TranscribeAuthority, generated.Package.PkgPath+"."+generated.Result.Plan.Output.Type)
		if err != nil || !found {
			t.Fatalf("output resolution %v %v", found, err)
		}
		resolver, err := typecatalog.NewResolver(generated.Types, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: generated.Package.PkgPath})
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := holders[0].Resolve(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := (bootstrap.ContractResolver{Component: canonical, InputType: inputType, OutputType: outputType, Types: resolver}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(resolved)
		if err != nil {
			t.Fatal(err)
		}
		var component spec.Component
		if err = json.Unmarshal(encoded, &component); err != nil {
			t.Fatal(err)
		}
		opted := 0
		for _, param := range component.Parameters {
			if param.BodyNullPolicy != "" {
				opted++
				if param.Name != "View" || param.BodyNullPolicy != "empty-record" {
					t.Fatalf("param=%+v", param)
				}
			}
		}
		if opted != 1 {
			t.Fatalf("policy count=%d", opted)
		}

		consumer := fmt.Sprintf(bodyNullGeneratedConsumer, generated.Result.Plan.PackageName(), generated.Result.Plan.Input.Type, generated.Result.Plan.Output.Type, module+"/records")
		if err := os.WriteFile(filepath.Join(root, "records", "body_null_runtime_test.go"), []byte(consumer), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-run", "TestGeneratedBodyNullRuntime", "./records")
		command.Dir = root
		command.Env = append(os.Environ(), "GOWORK=off")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("generated consumer: %v\n%s", err, output)
		}
	}
}

const bodyNullGeneratedConsumer = `package %s
import (
 "context"
 "os"
 "path/filepath"
 "reflect"
 "strings"
 "testing"
 "net/http/httptest"
 "github.com/viant/bindly"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/runtime/handler/compiler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
)
func TestGeneratedBodyNullRuntime(t *testing.T) {
 inputType,outputType:=reflect.TypeFor[%s](),reflect.TypeFor[%s]()
 cwd,err:=os.Getwd();if err!=nil {t.Fatal(err)}
 holders,err:=bootstrap.DiscoverComponentsFromPackages(context.Background(),filepath.Dir(cwd),[]string{%q},nil);if err!=nil {t.Fatal(err)}
 component,err:=holders[0].Resolve(inputType,outputType);if err!=nil {t.Fatal(err)}
 viewField,ok:=inputType.FieldByName("View");if !ok {t.Fatal("missing View")}
 bindings,err:=compiler.BuildBindingSpecs(component,reflect.StructOf([]reflect.StructField{viewField}),nil);if err!=nil {t.Fatal(err)}
 injector,_:=bindly.NewInjector();plan,err:=injector.CompilePlan(inputType,bindings...);if err!=nil {t.Fatal(err)}
 projection,err:=plan.Projection();if err!=nil {t.Fatal(err)}
 ref:=spec.RouteRef{Method:"PATCH",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:plan,Bindings:bindings});if err!=nil {t.Fatal(err)}
 route,ok:=contract.ForRoute(ref);if !ok {t.Fatal("missing route")}
 optional,err:=route.WithOptionalBindings("View");if err!=nil {t.Fatal(err)};if optional.Fields()[0].Binding().BodyNullPolicy!="empty-record" {t.Fatal("optional recompile lost policy")}
 for _,text:=range []string{"null","{}",""} {
  request:=httptest.NewRequest("PATCH","/records",strings.NewReader(text));request.Header.Set("Content-Type","application/json")
  provider,err:=requestprovider.New(request);if err!=nil {t.Fatal(err)}
  injector,_:=bindly.NewInjector();scope,err:=injector.ForScope(provider.Providers()...);if err!=nil {t.Fatal(err)}
  input:=reflect.New(inputType);err=scope.Bind(context.Background(),input.Interface(),bindly.WithPlan(route.Plan()));provider.Close()
  if text=="" {if err==nil {t.Fatal("missing body accepted")};continue};if err!=nil {t.Fatal(err)}
  row:=input.Elem().FieldByName("View");if row.IsNil() {t.Fatal("null body stayed nil")}
  marker:=row.Elem().FieldByName("Has");if marker.IsNil() {t.Fatal("null record has unavailable original presence")}
  marker=marker.Elem();for i:=0;i<marker.NumField();i++ {if marker.Field(i).Bool() {t.Fatal("fabricated client field presence")}}
 }
}
`
