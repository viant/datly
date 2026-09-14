package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	gotarget "github.com/viant/datly/transcribe/handler/golang"
	veltytarget "github.com/viant/datly/transcribe/handler/velty"
)

func TestGeneratedWriteHooksGoVeltySQLiteParity(t *testing.T) {
	for _, conversion := range []plan.LinkConversion{plan.LinkDirect, plan.LinkAddress, plan.LinkDereference} {
		for _, pointer := range []bool{false, true} {
			name := "values"
			if pointer {
				name = "pointers"
			}
			t.Run(string(conversion)+"/"+name, func(t *testing.T) {
				key := plan.KeyPart{Field: "ID", Type: spec.TypeRef{Name: "int64"}}
				child := &plan.RecordPlan{Identity: "Children", InputPath: plan.FieldPath{"Input", "Records", "Children"}, Table: "CHILDREN", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{key}, Write: plan.WritePolicy{Missing: plan.ActionInsert, Existing: plan.ActionUpdate}}
				root := &plan.RecordPlan{Identity: "Records", InputPath: plan.FieldPath{"Input", "Records"}, Table: "RECORDS", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{key}, Write: plan.WritePolicy{Missing: plan.ActionInsert, Existing: plan.ActionUpdate}, Relations: []*plan.RelationPlan{{Identity: "Children", FieldPath: plan.FieldPath{"Children"}, Cardinality: spec.CardinalityMany, Links: []plan.KeyLink{{Parent: key, Child: plan.KeyPart{Field: "RecordID", Type: spec.TypeRef{Name: "int64"}}, Conversion: plan.LinkDirect}}, Child: child}}}
				root.Write.ValuePath = root.InputPath
				root.Relations[0].Links[0].Conversion = conversion
				child.Write.ValuePath = child.InputPath
				child.Write.Order = 1
				root.Write.Existing = ""
				child.Write.Existing = ""
				root.Write.Allowed = []plan.Action{plan.ActionInsert}
				child.Write.Allowed = []plan.Action{plan.ActionInsert}
				semantic := &plan.Plan{Operation: plan.OperationPost, Root: root, Output: &plan.ContractRef{Path: plan.FieldPath{"Output", "Data"}}}
				parentType, childType := "[]Record", "[]Child"
				if pointer {
					parentType, childType = "[]*Record", "[]*Child"
				}
				asset, err := gotarget.Lower(semantic, gotarget.Config{Package: "hookfixture", Factory: "NewGeneratedHandler", Handler: "GeneratedHandler", InputType: "Input", OutputType: "Output", Records: []gotarget.RecordType{{Identity: "Records", Path: root.InputPath, Value: parentType}, {Identity: "Children", Path: child.InputPath, Value: childType}}})
				if err != nil {
					t.Fatal(err)
				}
				goSource, err := asset.Source()
				if err != nil {
					t.Fatal(err)
				}
				template, err := veltytarget.Render(semantic)
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/hooks"}).Write(t, dir)
				if err = os.WriteFile(filepath.Join(dir, "handler.go"), goSource, 0644); err != nil {
					t.Fatal(err)
				}
				parentValues, childValues := "[]Record{*parent}", "[]Child{*child}"
				if pointer {
					parentValues, childValues = "[]*Record{nil,parent}", "[]*Child{nil,child}"
				}
				parentKeyType, childKeyType, parentKey, childKey := "int64", "int64", "int64(7)", "c.RecordID"
				if conversion == plan.LinkAddress {
					childKeyType, childKey = "*int64", "*c.RecordID"
				}
				if conversion == plan.LinkDereference {
					parentKeyType, parentKey = "*int64", "&parentID"
				}
				source := strings.NewReplacer("{{PARENT_TYPE}}", parentType, "{{CHILD_TYPE}}", childType, "{{PARENT_VALUES}}", parentValues, "{{CHILD_VALUES}}", childValues, "{{TEMPLATE}}", strconv.Quote(template), "{{PARENT_KEY_TYPE}}", parentKeyType, "{{CHILD_KEY_TYPE}}", childKeyType, "{{PARENT_KEY}}", parentKey, "{{CHILD_KEY}}", childKey).Replace(writeHookParitySource)
				if err = os.WriteFile(filepath.Join(dir, "hooks_test.go"), []byte(source), 0644); err != nil {
					t.Fatal(err)
				}
				command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout", "45s", "./...")
				command.Dir = dir
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("generated %s hook parity failed: %v\n%s\nTemplate:\n%s", name, err, output, template)
				}
			})
		}
	}
}

const writeHookParitySource = `package hookfixture
import (
 "context"
 "errors"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 rhandler "github.com/viant/datly/runtime/handler"
 custom "github.com/viant/datly/runtime/handler/custom"
 veltyhandler "github.com/viant/datly/runtime/handler/velty"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/exec"
 "github.com/viant/datly/sql/dml"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
type Input struct{Records {{PARENT_TYPE}}}
type Output struct{Data {{PARENT_TYPE}}}
type Record struct{ID {{PARENT_KEY_TYPE}} ` + "`sqlx:\"ID,primaryKey\"`" + `;Name string ` + "`sqlx:\"NAME\"`" + `;Children {{CHILD_TYPE}} ` + "`sqlx:\"-\"`" + `}
type Child struct{ID int64 ` + "`sqlx:\"ID,primaryKey\"`" + `;RecordID {{CHILD_KEY_TYPE}} ` + "`sqlx:\"RECORD_ID\"`" + `;Name string ` + "`sqlx:\"NAME\"`" + `}
type logKey struct{}
func note(ctx context.Context,value string){log:=ctx.Value(logKey{}).(*[]string);*log=append(*log,value)}
func(r *Record)InitWrite(ctx context.Context)error{note(ctx,"record:init");r.Name+=":init";return nil}
func(r *Record)ValidateWrite(ctx context.Context)error{note(ctx,"record:validate");if strings.Contains(r.Name,"fail-parent"){return errors.New("parent rejected")};return nil}
func(r *Record)BeforeChildrenWrite(ctx context.Context,children {{CHILD_TYPE}})error{note(ctx,"record:children");r.Name+=":after-queue";if strings.Contains(r.Name,"fail-relation"){return errors.New("relation rejected")};return nil}
func(c *Child)InitWrite(ctx context.Context)error{note(ctx,"child:init");if {{CHILD_KEY}}!=7{return errors.New("relation key missing before child hook")};c.Name+=":init";return nil}
func(c *Child)ValidateWrite(ctx context.Context)error{note(ctx,"child:validate");if strings.Contains(c.Name,"fail-child"){return errors.New("child rejected")};return nil}
func TestHookParity(t *testing.T){
 for _,target:=range []string{"go","velty"}{for _,failure:=range []string{"","fail-parent","fail-relation","fail-child"}{t.Run(target+"/"+failure,func(t *testing.T){
  h:=sqlite.New(t);ctx:=context.Background();if err:=h.ExecStatements(ctx,"CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY,NAME TEXT)","CREATE TABLE CHILDREN(ID INTEGER PRIMARY KEY,RECORD_ID INTEGER,NAME TEXT)");err!=nil{t.Fatal(err)}
  component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Name:"Records"},Routes:[]*spec.Route{{Method:"POST",Path:"/records"}}}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(Input{}),OutputType:reflect.TypeOf(Output{})});if err!=nil{t.Fatal(err)}
  var handler rhandler.Handler
  if target=="go"{handler=custom.New[Input,Output](NewGeneratedHandler())}else{handler,err=veltyhandler.New[Input,Output](veltyhandler.Config{Template:{{TEMPLATE}}});if err!=nil{t.Fatal(err)}}
  runtime,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(Output{}),Handler:handler,DataSource:dml.Source{DB:h.DB}}});if err!=nil{t.Fatal(err)}
  parentID:=int64(7);_ = parentID;parent:=&Record{ID:{{PARENT_KEY}},Name:"record"};child:=&Child{ID:70,Name:"child"};if failure=="fail-child"{child.Name=failure}else if failure!=""{parent.Name=failure};parent.Children={{CHILD_VALUES}}
  input:=&Input{Records:{{PARENT_VALUES}}};var log []string;ctx=context.WithValue(ctx,logKey{},&log)
  result,err:=runtime.InvokeComponent(ctx,exec.ComponentRequest{Target:exec.ComponentTarget{Component:component.Key,Route:spec.RouteRef{Method:"POST",Path:"/records"}},Input:input})
  if failure!=""{if err==nil||!strings.Contains(err.Error(),"rejected"){t.Fatalf("hook error %v",err)};expected:=[]string{"record:init","record:validate","record:children","child:init","child:validate"};if failure=="fail-parent"{expected=expected[:2]}else if failure=="fail-relation"{expected=expected[:3]};if !reflect.DeepEqual(log,expected){t.Fatalf("hooks continued after error: got %v expected %v",log,expected)};type count struct{Count int};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS Count FROM RECORDS"},[]count{{0}});h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS Count FROM CHILDREN"},[]count{{0}});return}
  if err!=nil{t.Fatal(err)};if result==nil{t.Fatal("missing output")}
  expected:=[]string{"record:init","record:validate","record:children","child:init","child:validate"};if !reflect.DeepEqual(log,expected){t.Fatalf("hook order %v expected %v",log,expected)}
  type recordRow struct{ID int64;Name string};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,NAME FROM RECORDS"},[]recordRow{{7,"record:init:after-queue"}})
  type childRow struct{ID,RecordID int64;Name string};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT ID,RECORD_ID AS RecordID,NAME FROM CHILDREN"},[]childRow{{70,7,"child:init"}})
  recordIndex:=0;if len(input.Records)>1{recordIndex=1};if input.Records[recordIndex].Name!="record:init:after-queue"{t.Fatal("pointer-receiver source mutation lost")}
 })}}
}
`
