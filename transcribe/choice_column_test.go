package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/tagly/choice"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestChoiceColumnMetadataNativeAndGenerated(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(number INTEGER,text TEXT,flag BOOLEAN)", "INSERT INTO records VALUES(4,'outside',1)"); err != nil {
		t.Fatal(err)
	}
	const module = "github.com/viant/datly/testfixture/choicecolumn"
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('github.com/viant/datly/testfixture/choicecolumn/records')
#setting($_ = $route('/records','GET'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'),CAST(r.number AS int),CAST(r.text AS string),CAST(r.flag AS bool),choice(r.number,3,1,2),choice(r.text,'z','a','m'),choice(r.flag,false,true)
FROM records r`}
	compiled, err := NewCompiler().RuntimeContractsInModule(ctx, module, source)
	if err != nil {
		t.Fatal(err)
	}
	data, ok := compiled.OutputType.FieldByName("Data")
	if !ok {
		t.Fatal("missingData")
	}
	row := data.Type
	for row.Kind() == reflect.Pointer || row.Kind() == reflect.Slice {
		row = row.Elem()
	}
	assertChoiceRow(t, row)
	built, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: compiled.InputType, OutputType: compiled.OutputType, Types: compiled.Types, Resources: compiled.Resources})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &dsql.SQLComponent{DB: db.DB}
	if err = runtime.RegisterConnector("main", db.DB); err != nil {
		t.Fatal(err)
	}
	execution, err := built.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: runtime})
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.Read(ctx, reflect.New(compiled.InputType).Interface(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	if !strings.Contains(string(raw), `"number":4`) && !strings.Contains(string(raw), `"Number":4`) {
		t.Fatalf("output choice must not filter rows: %s", raw)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	for _, view := range generated.Result.Plan.Views {
		for _, field := range view.Fields {
			if field.Name == "Number" {
				c, e := choice.Parse(reflect.StructTag(field.Tag), reflect.TypeFor[int]())
				if e != nil {
					t.Fatal(e)
				}
				b, _ := json.Marshal(c.Values)
				if string(b) != "[3,1,2]" {
					t.Fatal(string(b))
				}
				checked = true
			}
		}
	}
	if !checked {
		t.Fatal("generated output choices missing")
	}
	// Generated catalog entries are intentionally synthetic descriptors, not
	// fabricated reflect.Types. Compile the emitted Go and reload its real linked
	// shape in the consumer process instead of weakening the roundtrip assertion.
	consumer := fmt.Sprintf(`package %s
import("encoding/json";"reflect";"testing";"github.com/viant/datly/bootstrap";"github.com/viant/mcp-protocol/schema/jsonschema";"github.com/viant/x")
func TestReloadGeneratedChoiceOutput(t *testing.T){
 typ:=reflect.TypeFor[%s]();data,ok:=typ.FieldByName("Data");if !ok{t.Fatal("missingData")};row:=data.Type;for row.Kind()==reflect.Pointer||row.Kind()==reflect.Slice{row=row.Elem()}
 linked,e:=(bootstrap.ContractResolver{OutputType:x.NewType(typ)}).Resolve();if e!=nil||linked.RootView==nil{t.Fatalf("linked output reload: %%v",e)}
 schema,e:=(jsonschema.Reflector{}).Compile(jsonschema.Request{Type:row});if e!=nil{t.Fatal(e)};props:=schema["properties"].(map[string]any)
 for name,want:=range map[string]string{"Number":"[3,1,2]","Text":"[\"z\",\"a\",\"m\"]","Flag":"[false,true]"}{field:=props[name];if field==nil{lower:=string(name[0]+32)+name[1:];field=props[lower]};if field==nil{t.Fatal(name)};b,_:=json.Marshal(field.(map[string]any)["enum"]);if string(b)!=want{t.Fatalf("%%s ordered enum %%s want%%s",name,b,want)}}
}`, generated.Result.Plan.PackageName(), generated.Result.Plan.Output.Type)
	if err = os.WriteFile(filepath.Join(root, "records", "choice_reload_test.go"), []byte(consumer), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-run", "TestReloadGeneratedChoiceOutput", "./records")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, e := command.CombinedOutput(); e != nil {
		t.Fatalf("actual generated linked output consumer: %v\n%s", e, output)
	}
}
func assertChoiceRow(t *testing.T, row reflect.Type) {
	t.Helper()
	schema, e := (jsonschema.Reflector{}).Compile(jsonschema.Request{Type: row})
	if e != nil {
		t.Fatal(e)
	}
	properties := schema["properties"].(map[string]any)
	for key, want := range map[string]string{"number": "[3,1,2]", "text": `["z","a","m"]`, "flag": "[false,true]"} {
		var property map[string]any
		for name, p := range properties {
			if strings.EqualFold(name, key) {
				property = p.(map[string]any)
			}
		}
		if property == nil {
			t.Fatal(key, properties)
		}
		b, _ := json.Marshal(property["enum"])
		if string(b) != want {
			t.Fatalf("%s %s want%s", key, b, want)
		}
	}
}
func TestChoiceColumnRejectsInvalidMetadata(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if e := db.ExecStatements(ctx, "CREATE TABLE records(number INTEGER)"); e != nil {
		t.Fatal(e)
	}
	for _, annotation := range []string{"choice(r.number)", "choice(r.number,null)", "choice(r.number,3,3)", "choice(r.number,1.5)", "choice(r.number,3) AS bad"} {
		t.Run(annotation, func(t *testing.T) {
			source := &Source{Name: "Records", Scope: "example.com/choice/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#package('example.com/choice/records')\nSELECT r.*,CAST(r.number AS int)," + annotation + " FROM records r"}
			if _, e := NewCompiler().RuntimeContractsInModule(ctx, "example.com/choice", source); e == nil {
				t.Fatal("invalid annotation accepted")
			}
		})
	}
}

func TestChoiceColumnNegativeLiterals(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if e := db.ExecStatements(ctx, "CREATE TABLE records(number INTEGER)"); e != nil {
		t.Fatal(e)
	}
	s := &Source{Name: "Records", Scope: "example.com/negative/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#package('example.com/negative/records')\n#setting($_ = $route('/negative','GET'))\n#define($_ = $Data<[]*Row>(output/view))\nSELECT r.*,type(r,'Row'),CAST(r.number AS int),choice(r.number,-3,1,-2) FROM records r"}
	compiled, e := NewCompiler().RuntimeContractsInModule(ctx, "example.com/negative", s)
	if e != nil {
		t.Fatal(e)
	}
	data, _ := compiled.OutputType.FieldByName("Data")
	row := data.Type
	for row.Kind() == reflect.Pointer || row.Kind() == reflect.Slice {
		row = row.Elem()
	}
	field, _ := row.FieldByName("Number")
	c, e := choice.Parse(field.Tag, field.Type)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(c.WireValues())
	if string(b) != "[-3,1,-2]" {
		t.Fatal(string(b))
	}
}
