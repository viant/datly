package transcribe

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestDeclaredViewLimitNativeRoundTrip(t *testing.T) {
	for _, limit := range []int{0, 1} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/viewlimit"}).Write(t, root)
			source := &Source{Scope: "github.com/viant/datly/testfixture/viewlimit/dql/records", Name: "Read", Text: fmt.Sprintf(`#package('generated')
#setting($_ = $route('/records','GET'))
#define($_ = $Records<?>(view/Records).WithLimit(%d).WithColumnType('ID','int') /* SELECT ID FROM records ORDER BY ID */)
#define($_ = $Data<?>(output/view))
SELECT r.ID,cast(r.ID AS int) FROM records r`, limit)}
			result, err := NewCompiler().Transcribe(t.Context(), Request{Destination: root, Source: source, Generation: GenerationOptions{Operation: "get"}})
			if err != nil {
				t.Fatal(err)
			}

			fixture := strings.NewReplacer("PACKAGE_PATH", strconv.Quote(result.Result.Plan.Package), "HOLDER", result.Result.Plan.HolderName(), "INPUT_TYPE", result.Result.Plan.Input.Type, "OUTPUT_TYPE", result.Result.Plan.Output.Type, "NAMESPACE", strconv.Quote(result.Result.Plan.Resources.Namespace), "EXPECTED", strconv.Itoa(2-limit)).Replace(declaredLimitConsumer)
			if err = os.WriteFile(filepath.Join(root, "limit_runtime_test.go"), []byte(fixture), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "-v", "-run", "^TestReloadDeclaredLimit$", ".")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			output, err := cmd.CombinedOutput()
			t.Logf("native reload:\n%s", output)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(output), "--- PASS: TestReloadDeclaredLimit") {
				t.Fatal("named reload test not executed")
			}
		})
	}
}

const declaredLimitConsumer = `package fixture_test
import (
 "context"
 "reflect"
 "testing"
 "github.com/viant/bindly"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 g PACKAGE_PATH
)
func TestReloadDeclaredLimit(t *testing.T){
 _=reflect.TypeFor[g.HOLDER]();ctx:=context.Background();h:=testharness.NewSQLiteHarness(t)
 if err:=h.ExecStatements(ctx,"CREATE TABLE records(ID INTEGER PRIMARY KEY)","INSERT INTO records VALUES(420),(421)");err!=nil{t.Fatal(err)}
 linked,err:=bootstrap.ReflectPackages([]string{PACKAGE_PATH});if err!=nil{t.Fatal(err)};if len(linked.Components)!=1{t.Fatalf("routes=%v",linked.Components)}
 in:=&g.INPUT_TYPE{};resources:=resource.New();if err=resources.Register(NAMESPACE,g.HOLDER{}.EmbedFS());err!=nil{t.Fatal(err)}
 c,err:=linked.Components[0].Resolve(reflect.TypeFor[g.INPUT_TYPE](),reflect.TypeFor[g.OUTPUT_TYPE]());if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:c,InputType:reflect.TypeFor[g.INPUT_TYPE](),OutputType:reflect.TypeFor[g.OUTPUT_TYPE](),Resources:resources});if err!=nil{t.Fatal(err)}
 provider,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"GET",Path:"/records"});if !ok{t.Fatal("missing canonical route")}
 seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};injector,err:=seed.ForScope(provider);if err!=nil{t.Fatal(err)}
 if err=injector.Bind(ctx,in,bindly.WithPlan(route.Plan()),bindly.WithSource(in));err!=nil{t.Fatal(err)}
 if len(in.Records)!=EXPECTED{t.Fatalf("reloaded Records=%+v expected count EXPECTED",in.Records)}
 t.Logf("reloaded canonical Records count=%d",len(in.Records))
}
`
