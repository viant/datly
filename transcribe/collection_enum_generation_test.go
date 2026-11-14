package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/tagly/choice"
)

func TestCollectionEnumNativeGeneration(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,channels TEXT)"))
	const module = "github.com/viant/datly/testfixture/collectionenum"
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('records.query'))
#define($_ = $State<[]string>(query/state).Optional().WithPredicate(0,'in','r','channels').WithTag('mcpEnum:"true" enumAccept:"literal" choice:"in_use" choice:"shared" choice:"not_shared"'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*,type(r,'Row'),CAST(r.channels AS '[]string'),tag(r.channels,'mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO" sqlx:"channels,enc=JSON"') FROM records r`}
	generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	queryFound := false
	for _, field := range generated.Result.Plan.Input.Fields {
		if field.Name == "State" {
			queryFound = true
			values, err := choice.Tags(reflect.StructTag(field.Tag))
			require.NoError(t, err)
			require.Equal(t, []string{"in_use", "shared", "not_shared"}, values, "query collection choices must survive canonical lowering")
		}
	}
	require.True(t, queryFound)
	found := false
	for _, view := range generated.Result.Plan.Views {
		for _, field := range view.Fields {
			if field.Name == "Channels" {
				found = true
				enum, err := choice.ParseCollectionEnum(reflect.StructTag(field.Tag), reflect.TypeFor[[]string]())
				require.NoError(t, err)
				require.NotNil(t, enum)
				require.Equal(t, []interface{}{"DISPLAY", "VIDEO"}, enum.CollectionItems().WireValues())
			}
		}
	}
	require.True(t, found)
	consumer := fmt.Sprintf(`package %s
import("encoding/json";"reflect";"testing";"github.com/viant/tagly/choice";"github.com/viant/mcp-protocol/schema/jsonschema";"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";mtool "github.com/viant/datly/mcp/tool";"github.com/viant/x")
func TestReloadCollectionEnum(t *testing.T) {
 input:=reflect.TypeFor[%s]();state,ok:=input.FieldByName("State");if !ok{t.Fatal("missing State")};choices,e:=choice.Tags(state.Tag);if e!=nil||!reflect.DeepEqual(choices,[]string{"in_use","shared","not_shared"}){t.Fatalf("reloaded query choices %%v: %%v",choices,e)}
 component,e:=(bootstrap.ContractResolver{Component:&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Name:"records"},Routes:[]*spec.Route{{Method:"GET",Path:"/records",MCP:[]*spec.MCPExposure{{Kind:spec.MCPExposureTool,Name:"records.query"}}}}},InputType:x.NewType(input)}).Resolve();if e!=nil{t.Fatal(e)}
 if len(component.Parameters)==0{t.Fatal("missing reloaded parameters")};predicateFound:=false;for _,parameter:=range component.Parameters{if parameter.Name=="State"{predicateFound=len(parameter.Predicates)==1}};if !predicateFound{t.Fatal("query predicate lost on reload")}
 artifact,e:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:input});if e!=nil{t.Fatal(e)};contract,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"GET",Path:"/records"});if !ok{t.Fatal("missing route contract")};plan,e:=mtool.NewCompiler().Compile(mtool.Input{Component:component.Key,Exposure:component.Routes[0].MCP[0],Contract:contract});if e!=nil{t.Fatal(e)};stateSchema:=plan.Metadata().InputSchema.Properties["state"];if stateSchema==nil{t.Fatal("missing state schema")};want:=[]any{"in_use","shared","not_shared"};if !reflect.DeepEqual(stateSchema["items"].(map[string]any)["enum"],want){t.Fatalf("actual MCP input schema %%#v",stateSchema)}
 typ:=reflect.TypeFor[%s]();data,ok:=typ.FieldByName("Data");if !ok{t.Fatal("missing Data")};row:=data.Type;for row.Kind()==reflect.Pointer||row.Kind()==reflect.Slice{row=row.Elem()};field,ok:=row.FieldByName("Channels");if !ok{t.Fatal("missing Channels")}
 enum,e:=choice.ParseCollectionEnum(field.Tag,field.Type);if e!=nil||enum==nil{t.Fatalf("linked collection: %%v",e)}
 for _,raw:=range []string{"null","[]", "[\"DISPLAY\",\"VIDEO\"]"}{if _,e:=enum.DecodeJSON([]byte(raw));e!=nil{t.Fatalf("%%s: %%v",raw,e)}}
 for _,raw:=range []string{"[\"unsupported\"]","[null]","[1]"}{if _,e:=enum.DecodeJSON([]byte(raw));e==nil{t.Fatalf("accepted %%s",raw)}}
 schema,e:=(jsonschema.Reflector{}).Compile(jsonschema.Request{Type:row});if e!=nil{t.Fatal(e)};props:=schema["properties"].(map[string]any);property:=props["channels"];if property==nil{property=props["Channels"]};b,_:=json.Marshal(property);if string(b)=="null"{t.Fatal("missing collection schema")};items:=property.(map[string]any)["items"].(map[string]any);b,_=json.Marshal(items["enum"]);if string(b)!="[\"DISPLAY\",\"VIDEO\"]"{t.Fatalf("item enum %%s",b)}
}`, generated.Result.Plan.PackageName(), generated.Result.Plan.Input.Type, generated.Result.Plan.Output.Type)
	require.NoError(t, os.WriteFile(filepath.Join(root, "records", "collection_reload_test.go"), []byte(consumer), 0600))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-run", "TestReloadCollectionEnum", "./records")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}
