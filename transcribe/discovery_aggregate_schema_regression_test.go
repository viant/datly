package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
)

func TestSchemaDiscoveryPreservesInferredAggregateHelper(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), `CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY, NAME TEXT)`); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	files := map[string]string{
		"generated/router.go": "package generated\nimport \"github.com/viant/xdatly\"\ntype Output struct{Data []*Event `parameter:\"Data,kind=output,in=body\"`}\ntype PatchComponent struct { Contract xdatly.Component[Input,Output] `component:\"patch,path=/events,method=PATCH,connector=main,view=patch\"` }\n",
		"go.mod":              "module example.com/aggregate\n\ngo 1.24\n",
		"generated/input.go":  "package generated\ntype Input struct { Events []*Event; CurrentIDs CurrentIDsHelper `parameter:\"CurrentIDs,kind=param,in=Events\" codec:\"structql\"` }\ntype Event struct { Id int; Name *string }\ntype CurrentIDsHelper struct { Values []int }\n",
		"dql/patch.dql":       "#package('example.com/aggregate/generated')\n#setting($_ = $input_type('example.com/aggregate/generated.Input'))\n#setting($_ = $output_type('example.com/aggregate/generated.Output'))\n#setting($_ = $route('/events','PATCH'))\n#define($_ = $Events<[]*Event>(body/Data).Cardinality('Many'))\n#define($_ = $CurrentIDs<?>(param/Events) /* SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1 */)\n#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many').WithTypeName('CurrentEvent') /* SELECT v.*,selector_no_limit(v,true) FROM (SELECT ID, NAME FROM EVENTS WHERE $criteria.In(\"ID\", $CurrentIDs.Values)) v */)\n#define($_ = $Data<[]*Event>(output/body))\nSELECT e.*,type(e,'Event') FROM EVENTS e\n",
	}
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/aggregate/dql"}, SourceFile: "patch.dql", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB})}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("components = %d; want 1", len(project.Components))
	}
	compiled := project.Components[0]
	inputType, err := (&discoveryInputCompiler{component: compiled.Component, declarations: compiled.Declarations, viewBindings: compiled.ViewBindings, resolver: compiled.TypeResolver, source: compiled.Source, views: compiled.Views}).inputType()
	if err != nil {
		t.Fatal(err)
	}
	helper, ok := indirectInputType(inputType).FieldByName("CurrentIDs")
	if !ok || indirectInputType(helper.Type).Kind() != reflect.Struct {
		t.Fatalf("CurrentIDs must be a concrete helper struct; got %v", helper.Type)
	}
	values, ok := indirectInputType(helper.Type).FieldByName("Values")
	if !ok || values.Type != reflect.TypeOf([]*int{}) {
		t.Fatalf("CurrentIDs.Values = %v; want []*int from the schema-inferred EVENTS.ID", values.Type)
	}
}
