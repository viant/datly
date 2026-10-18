package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/report"
)

func TestCubeSelectorAliasOwnership(t *testing.T) {
	section := reflect.StructField{Name: "Measures", Type: reflect.TypeOf(struct {
		Metric25 bool `json:"metric25"`
		Other    bool `json:"legacy"`
	}{})}
	for _, tc := range []struct {
		name       string
		selections []report.SelectionDefinition
		want       map[string]string
	}{
		{"known", []report.SelectionDefinition{{Field: "Measures.Metric25", Name: "metric_25"}}, map[string]string{"metric_25": "metric25"}},
		{"canonical owner", []report.SelectionDefinition{{Field: "Measures.Metric25", Name: "LEGACY"}}, map[string]string{}},
		{"multiple owners", []report.SelectionDefinition{{Field: "Measures.Metric25", Name: "metric_25"}, {Field: "Measures.Other", Name: "metric_25"}}, map[string]string{}},
		{"case-only redundant", []report.SelectionDefinition{{Field: "Measures.Metric25", Name: "METRIC25"}}, map[string]string{}},
		{"filters excluded", []report.SelectionDefinition{{Field: "Filters.Metric25", Name: "metric_25"}}, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := cubeSelectorAliases(section, report.Definition{Measures: tc.selections})
			if !reflect.DeepEqual(actual, tc.want) {
				t.Fatalf("aliases=%v want %v", actual, tc.want)
			}
		})
	}
}

func TestCubeEmitsSelectorAliasBinding(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := cubeFixture()
	component.RootView.Columns[1].Name = "metric_25"
	component.RootView.Columns[1].Source = "metric_25"
	component.RootView.Columns[1].Tag = `json:"metric_25"`
	component.RootView.Source.SQL = "SELECT account_id,SUM(amount) AS metric_25 FROM spend GROUP BY account_id"
	generated, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(filepath.Join(root, "reporting"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(root, "reporting", "spend_cube.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"func (v *SpendCubeMeasures) UnmarshalJSON", `"metric_25": "metric25"`} {
		if !strings.Contains(string(content), fragment) {
			t.Fatalf("missing %s:\n%s", fragment, content)
		}
	}
	if strings.Contains(string(content), "func (v *SpendCubeFilters) UnmarshalJSON") || strings.Contains(string(content), "func (v *SpendCubeInput) UnmarshalJSON") {
		t.Fatal("decoder acquired filter or input ownership")
	}
	runtimeTest := strings.ReplaceAll(cubeAliasRuntimeTest, "CUBE_PACKAGE", generated.Plan.PackageName())
	if err = os.WriteFile(filepath.Join(root, "reporting", "alias_runtime_test.go"), []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated alias binding: %v\n%s", err, output)
	}
}

const cubeAliasRuntimeTest = `package CUBE_PACKAGE
import (
 "context"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/exec"
 "github.com/viant/datly/runtime/handler/engine"
 rh "github.com/viant/datly/runtime/handler"
 hp "github.com/viant/datly/runtime/handler/provider"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/tag"
 xh "github.com/viant/xdatly/handler"
 xs "github.com/viant/xdatly/state"
)
type aliasInvoker struct{t *testing.T;calls int}
func (v *aliasInvoker) InvokeComponent(ctx context.Context,request exec.ComponentRequest)(any,error){
 v.calls++
 found:=false
 for _,provider:=range request.Providers {
  if provider.Kind()!=string(xh.SelectorsKey){continue}
  value,ok,err:=provider.Locate(nil).Value(ctx,reflect.TypeFor[xs.Selectors](),"")
  if err!=nil||!ok{v.t.Fatalf("selectors: %v %v",ok,err)}
  selectors:=value.(xs.Selectors)
  if len(selectors)!=1||!reflect.DeepEqual(selectors[0].Fields,[]string{"metric_25"}){v.t.Fatalf("SQL identities=%+v",selectors)}
  found=true
 }
 if !found{v.t.Fatal("selector provider absent")}
 return &SpendOutput{},nil
}
func TestGeneratedSelectorAliasRequestAndHandler(t *testing.T){
 field:=reflect.TypeFor[SpendCubeComponent]().Field(0)
 metadata,_,err:=tag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 source:=&bootstrap.RouteSource{PackagePath:reflect.TypeFor[SpendCubeComponent]().PkgPath(),HolderType:"SpendCubeComponent",FieldName:field.Name,Tag:metadata,InputType:"SpendCubeInput",OutputType:"SpendCubeOutput"}
 component,err:=source.Resolve(reflect.TypeFor[SpendCubeInput](),reflect.TypeFor[SpendCubeOutput]());if err!=nil{t.Fatal(err)}
 handler,err:=NewSpendCube();if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[SpendCubeInput](),OutputType:reflect.TypeFor[SpendCubeOutput](),Handler:handler,HandlerOwnedOutput:true});if err!=nil{t.Fatal(err)}
 route,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/spend/cube"});if !ok{t.Fatal("cube route absent")}
 for _,key:=range []string{"metric25","metric_25"}{
  req:=httptest.NewRequest("POST","/spend/cube",strings.NewReader("{\"measures\":{\""+key+"\":true},\"filters\":{\"accountID\":0}}"));req.Header.Set("Content-Type","application/json")
  scope,err:=requestprovider.New(req);if err!=nil{t.Fatal(err)}
  invoker:=&aliasInvoker{t:t}
  wrapper:=rh.HandlerFunc(func(ctx context.Context,inv rh.Invocation)(any,error){
   input:=inv.Input.(*SpendCubeInput)
   if input.Filters.AccountID==nil||*input.Filters.AccountID!=0||input.Filters.Has==nil||!input.Filters.Has.AccountID{t.Fatalf("filter zero/presence lost: %+v",input.Filters)}
   return handler.Execute(ctx,inv)
  })
  _,err=engine.New().Execute(context.Background(),engine.Request{Input:route,Scope:scope,Handler:wrapper,OutputType:reflect.TypeFor[SpendCubeOutput](),ComponentInvoker:hp.Static(xh.ComponentInvokerKey,invoker)})
  scope.Close();if err!=nil{t.Fatal(err)}
  if invoker.calls!=1{t.Fatal("report handler did not delegate")}
 }
 var selection SpendCubeMeasures
 if err=json.Unmarshal([]byte("{\"metric25\":false,\"metric_25\":true,\"metric25\":null}"),&selection);err!=nil||!selection.Metric25{t.Fatalf("ordered aliases/null: %+v %v",selection,err)}
 wire,err:=json.Marshal(selection);if err!=nil||strings.Contains(string(wire),"metric_25")||!strings.Contains(string(wire),"metric25"){t.Fatalf("canonical wire: %s %v",wire,err)}
 rowWire,err:=json.Marshal(SpendRow{});if err!=nil||!strings.Contains(string(rowWire),"metric_25"){t.Fatalf("reader wire changed: %s %v",rowWire,err)}
}
`
