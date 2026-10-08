package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func cubeFixture() *spec.Component {
	groupable := true
	return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/reporting", Name: "Spend"}, Name: "Spend", Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/spend", Name: "Spend"}}, Parameters: []*spec.Parameter{{Name: "AccountID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "accountId"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"spend", "account_id"}}}}, {Name: "Rows", TypeExpr: "[]*SpendRow", Source: spec.BindSource{Kind: "output", Name: "view"}, EmitOutput: true}}, RootView: &spec.View{Name: "spend", TypeName: "SpendRow", Groupable: &groupable, Source: &spec.ViewSource{SQL: "SELECT account_id,SUM(amount) AS amount FROM spend GROUP BY account_id"}, Columns: []*spec.Column{{Name: "AccountID", Source: "account_id", Type: spec.TypeRef{Name: "int"}, Groupable: &groupable}, {Name: "Amount", Source: "amount", Type: spec.TypeRef{Name: "float64"}}}}}
}

func TestCubeEmitsLinkedFacade(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := cubeFixture()
	component.RootView.Columns[0].Tag = `json:"AccountID"`
	component.RootView.Columns[1].Tag = `json:"reader_amount"`
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "Hidden", Source: "hidden", Type: spec.TypeRef{Name: "int"}, Groupable: component.RootView.Groupable, Tag: `json:"-"`})
	component.RootView.Source.SQL = "SELECT account_id, hidden, SUM(amount) AS amount FROM spend GROUP BY account_id, hidden"
	periodDefault, optional, cacheable := "month", false, true
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Period", TypeExpr: "string", Source: spec.BindSource{Kind: "form", Name: "period"}, Required: &optional, Value: &periodDefault, Cacheable: &cacheable, Tag: `mcp:"-"`})
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "DaysRemaining", Source: "days_remaining", Type: spec.TypeRef{Name: "int", Pointer: true}, Groupable: &optional, Tag: `sqlx:"-" json:"daysRemaining,omitempty"`})
	component.RootView.Source.SQL = "SELECT account_id, hidden, SUM(amount) AS amount, '' AS days_remaining FROM spend GROUP BY account_id, hidden"
	generated, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(filepath.Join(root, "reporting"))
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Plan.Cubes) != 1 || !generated.Plan.Report.LinkedFacade {
		t.Fatalf("missing linked facade: %+v", generated.Plan.Cubes)
	}
	content, err := os.ReadFile(filepath.Join(root, "reporting", "spend_cube.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"type SpendCubeInput struct", "type SpendCubeOutput =", "type SpendCubeComponent struct", "func NewSpendCube()", "NewLinkedFacade[SpendCubeInput, SpendCubeOutput, Component]", "type SpendCubeFiltersHas struct", "SetAccountID"} {
		if !strings.Contains(string(content), expected) {
			t.Fatalf("missing %s:\n%s", expected, content)
		}
	}
	if component.Settings.Report.LinkedFacade {
		t.Fatal("generation mutated authored component")
	}
	runtimeTest := strings.ReplaceAll(cubeRuntimeTest, "CUBE_PACKAGE", generated.Plan.PackageName())
	if err := os.WriteFile(filepath.Join(root, "reporting", "cube_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated facade failed: %v\n%s", err, output)
	}
}

func TestCubeShapePlanningDoesNotMaterializeLinkedFacades(t *testing.T) {
	generator := New(Input{Component: cubeFixture(), TargetPackage: "example.com/generated/reporting"})
	plan, err := generator.plan(false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Cubes) != 0 || plan.Report.LinkedFacade {
		t.Fatal("dynamic shape planning acquired a static facade")
	}
}

func TestFreshCubeGenerationNormalizesSnakeCaseSelections(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := cubeFixture()
	component.RootView.Columns[0].Name = "campaign_id"
	component.RootView.Columns[0].Source = "campaign_id"
	component.RootView.Columns[0].Tag = `json:"ReaderCampaign"`
	component.RootView.Columns[1].Name = "total_spend"
	component.RootView.Columns[1].Source = "total_spend"
	component.RootView.Columns[1].Tag = `json:"ReaderTotal"`
	component.Parameters[0].Predicates[0].Args[1] = "campaign_id"
	component.RootView.Source.SQL = "SELECT campaign_id, SUM(amount) AS total_spend FROM spend GROUP BY campaign_id"
	generated, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(filepath.Join(root, "reporting"))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ReplaceAll(`package CUBE_PACKAGE
import("encoding/json";"reflect";"strings";"testing")
func TestFreshNormalizedCubeDiscoveryAndBinding(t *testing.T){
 for _,test:=range []struct{section,field,wire string}{{"Dimensions","CampaignId","campaignId"},{"Measures","TotalSpend","totalSpend"}}{
  section,_:=reflect.TypeFor[SpendCubeInput]().FieldByName(test.section);field,ok:=section.Type.FieldByName(test.field)
  if !ok||strings.Split(field.Tag.Get("json"),",")[0]!=test.wire{t.Fatalf("fresh cube discovery used SQL or reader names: %s",field.Tag)}
 }
 var input SpendCubeInput
 if err:=json.Unmarshal([]byte("{\"dimensions\":{\"campaignId\":true},\"measures\":{\"totalSpend\":true}}"),&input);err!=nil{t.Fatal(err)}
 if !input.Dimensions.CampaignId||!input.Measures.TotalSpend{t.Fatal("fresh normalized selections did not bind")}
 if _,err:=NewSpendCube();err!=nil{t.Fatal(err)}
}`, "CUBE_PACKAGE", generated.Plan.PackageName())
	if err := os.WriteFile(filepath.Join(root, "reporting", "wire_runtime_test.go"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fresh cube contract failed: %v\n%s", err, output)
	}
}

const cubeRuntimeTest = `package CUBE_PACKAGE
import (
 "context"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 index "github.com/viant/datly/bootstrap/index"
 "github.com/viant/datly/mcp"
 "github.com/viant/datly/report"
 runtime "github.com/viant/datly/runtime"
 custom "github.com/viant/datly/runtime/handler/custom"
 rhandler "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/tag"
 "github.com/viant/datly/typecatalog"
 "github.com/viant/mcp-protocol/schema"
)
func resolve(t *testing.T,holder,input,output reflect.Type)*spec.Component {
 t.Helper();field:=holder.Field(0);metadata,_,err:=tag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 source:=&bootstrap.RouteSource{PackagePath:holder.PkgPath(),HolderType:holder.Name(),FieldName:field.Name,Tag:metadata,InputType:input.Name(),OutputType:output.Name()}
 component,err:=source.Resolve(input,output);if err!=nil{t.Fatal(err)};return component
}
func TestLinkedCubeRegistrationMCPAndDependency(t *testing.T) {
 ctx:=context.Background()
 dimensions,_:=reflect.TypeFor[SpendCubeInput]().FieldByName("Dimensions")
 account,_:=dimensions.Type.FieldByName("AccountID")
 if strings.Split(account.Tag.Get("json"),",")[0]!="accountID"{t.Fatalf("reader serialization leaked into cube discovery: %s",account.Tag)}
 if _,found:=dimensions.Type.FieldByName("Hidden");found{t.Fatal("json:- selection exposed")}
 measures,_:=reflect.TypeFor[SpendCubeInput]().FieldByName("Measures")
 amount,_:=measures.Type.FieldByName("Amount")
 if _,found:=measures.Type.FieldByName("DaysRemaining");found{t.Fatal("transient computed reader output advertised as cube measure")}
 if _,found:=reflect.TypeFor[SpendRow]().FieldByName("DaysRemaining");!found{t.Fatal("transient computed field removed from regular reader output")}
 if strings.Split(amount.Tag.Get("json"),",")[0]!="amount"{t.Fatalf("reader measure serialization leaked into cube: %s",amount.Tag)}
 var bound SpendCubeInput
 if err:=json.Unmarshal([]byte("{\"dimensions\":{\"accountID\":true},\"measures\":{\"amount\":true}}"),&bound);err!=nil{t.Fatal(err)}
 if !bound.Dimensions.AccountID||!bound.Measures.Amount{t.Fatal("lower-camel linked selections did not bind")}
 base:=resolve(t,reflect.TypeFor[Component](),reflect.TypeFor[SpendInput](),reflect.TypeFor[SpendOutput]())
 cube:=resolve(t,reflect.TypeFor[SpendCubeComponent](),reflect.TypeFor[SpendCubeInput](),reflect.TypeFor[SpendCubeOutput]())
 if !base.Settings.Report.LinkedFacade{t.Fatal("source did not suppress dynamic duplicate")}
 snapshot,err:=index.BuildLinked([]string{base.Key.Scope},[]*spec.Component{base,cube});if err!=nil{t.Fatal(err)}
 if len(snapshot.Entries())!=2{t.Fatalf("duplicate cube entries: %d",len(snapshot.Entries()))}
 calls:=0
 wantPeriod:="month"
 sourceHandler:=custom.NewFunc[SpendInput,SpendOutput](func(_ context.Context,input *SpendInput)(*SpendOutput,error){
  calls++;if input.AccountID!=0||input.Has==nil||!input.Has.AccountID{t.Fatalf("explicit zero lost presence: %+v",input)}
  if input.Period!=wantPeriod{t.Fatalf("hidden period default or HTTP proxy changed: got %q want %q",input.Period,wantPeriod)}
  return &SpendOutput{},nil
 })
 cubeHandler,err:=NewSpendCube();if err!=nil{t.Fatal(err)}
 types:=typecatalog.NewCatalog()
 compiled,err:=report.NewProjectCompiler(report.ProjectConfig{Types:types}).CompileArtifacts([]bootstrap.ArtifactInput{
  {Component:base,InputType:reflect.TypeFor[SpendInput](),OutputType:reflect.TypeFor[SpendOutput](),Handler:sourceHandler,HandlerOwnedOutput:true,Types:types},
  {Component:cube,InputType:reflect.TypeFor[SpendCubeInput](),OutputType:reflect.TypeFor[SpendCubeOutput](),Handler:cubeHandler,HandlerOwnedOutput:true,Types:types},
 });if err!=nil{t.Fatal(err)}
 if len(compiled.Artifacts())!=2{t.Fatal("generated cube was derived again")}
 registered,err:=compiled.RuntimeComponents(ctx,report.RuntimeConfigureFunc(func(context.Context,*report.ComponentArtifact)(report.RuntimeCapabilities,error){return report.RuntimeCapabilities{},nil}));if err!=nil{t.Fatal(err)}
 type parentInput struct{ Cube *SpendCubeOutput "parameter:\"Cube,kind=component,in=POST:/spend/cube,required\"" }
 parentSpec:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:base.Key.Scope,Name:"Parent"},Routes:[]*spec.Route{{Method:"POST",Path:"/parent"}}}
 parentHandler:=custom.NewFunc[parentInput,SpendCubeOutput](func(_ context.Context,input *parentInput)(*SpendCubeOutput,error){return input.Cube,nil})
 parent,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:parentSpec,InputType:reflect.TypeFor[parentInput](),OutputType:reflect.TypeFor[SpendCubeOutput](),Handler:parentHandler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
 reg,err:=parent.Registration(registry.RegisteredComponent{});if err!=nil{t.Fatal(err)};registered=append(registered,reg)
 rt,err:=runtime.NewRuntime(registered);if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 for _,path:=range []string{"/spend/cube","/parent"} {
  req:=httptest.NewRequest("POST",path,strings.NewReader("{\"dimensions\":{\"accountID\":true},\"filters\":{\"accountID\":0}}"));req.Header.Set("Content-Type","application/json")
  scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)}
  _,err=rt.ExecuteRoute(ctx,"POST",path,scope);scope.Close();if err!=nil{t.Fatalf("%s: %v",path,err)}
 }
 service,err:=mcp.New(mcp.Config{Components:registered,Invoker:rt});if err!=nil{t.Fatal(err)}
 tool,ok:=service.Registry().ToolRegistry.Get("SpendCube");if !ok{t.Fatal("linked MCP cube missing")}
 toolPlan,_:=service.Catalog().Tool("SpendCube")
 wire,_:=json.Marshal(toolPlan.Metadata().InputSchema)
 if strings.Contains(string(wire),"\"period\":"){t.Fatalf("mcp:- nested cube filter entered discovery: %s",wire)}
 if !strings.Contains(string(wire),"\"accountID\":")||!strings.Contains(string(wire),"\"amount\":")||strings.Contains(string(wire),"\"AccountID\":")||strings.Contains(string(wire),"\"reader_amount\":")||strings.Contains(string(wire),"\"Hidden\":"){t.Fatalf("MCP cube discovery changed selection names or exposed a hidden column: %s",wire)}
 if strings.Contains(string(wire),"\"Has\"")||strings.Contains(string(wire),"\"has\""){t.Fatalf("presence marker exposed: %s",wire)}
 result,protocolErr:=tool.Handler(ctx,&schema.CallToolRequest{Params:schema.CallToolRequestParams{Name:"SpendCube",Arguments:map[string]any{"dimensions":map[string]any{"accountID":true},"filters":map[string]any{"accountID":0}}}})
 if protocolErr!=nil||result==nil||result.IsError!=nil&&*result.IsError{t.Fatalf("MCP: %+v %v",result,protocolErr)}
 if calls!=3{t.Fatalf("source call count %d",calls)}
 wantPeriod="quarter"
 req:=httptest.NewRequest("POST","/spend/cube",strings.NewReader("{\"dimensions\":{\"accountID\":true},\"filters\":{\"accountID\":0,\"period\":\"quarter\"}}"));req.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)}
 _,err=rt.ExecuteRoute(ctx,"POST","/spend/cube",scope);scope.Close();if err!=nil{t.Fatal(err)}
 if calls!=4{t.Fatalf("HTTP hidden filter did not execute source: %d",calls)}
 var _ rhandler.TypedHandler=cubeHandler
}
`

func TestCubeRouteNamesPreventPackageCollisions(t *testing.T) {
	component := cubeFixture()
	component.Routes = []*spec.Route{{Method: "GET", Path: "/spend/one", Name: "One"}, {Method: "GET", Path: "/spend/two", Name: "Two"}}
	plan, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Cubes) != 2 || plan.Cubes[0].Name == plan.Cubes[1].Name || plan.Cubes[0].InputName == plan.Cubes[1].InputName {
		t.Fatalf("colliding routes: %+v", plan.Cubes)
	}
	component.Routes[1].Name = "One"
	if _, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Plan(); err == nil {
		t.Fatal("ambiguous cube identity accepted")
	}
}

func TestCubeInputCannotCollideWithOwnedView(t *testing.T) {
	component := cubeFixture()
	component.RootView.TypeName = "SpendCubeInput"
	component.Parameters[1].TypeExpr = "[]*SpendCubeInput"
	if _, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Plan(); err == nil {
		t.Fatal("cube input shadowed an owned Go view")
	}
}
