package generate

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestReportMCPGenerationTwoPass(t *testing.T) {
	strict := false
	component := cubeFixture()
	component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "get_spend"}}
	component.Settings.Report.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aggregate_spend", Description: "Quoted, \"text\"\nsecond line", CaseFormat: "lc", StrictArguments: &strict}
	root := t.TempDir()
	first, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(root)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			files[entry.Name()], err = os.ReadFile(filepath.Join(root, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err = New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(root)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range files {
		actual, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !bytes.Equal(actual, want) {
			t.Fatalf("second pass changed %s: %v", name, err)
		}
	}
	selected := first.Plan.Cubes[0].Component.Routes[0].MCP[0]
	if !reflect.DeepEqual(selected, component.Settings.Report.MCP) || !reflect.DeepEqual(first.Plan.Cubes[0].Definition.MCP, selected) {
		t.Fatal("generated cube metadata diverged")
	}
	sourceTag, err := first.Plan.componentTag(first.Plan.Routes[0]).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	restored, ok, err := tag.ParseComponent(reflect.StructTag(sourceTag))
	if err != nil || !ok || !reflect.DeepEqual(restored.ReportMCP, selected) {
		t.Fatalf("source reload lost metadata: %+v %v", restored.ReportMCP, err)
	}
	if !bytes.Contains(files["spend_cube.go"], []byte("aggregate_spend")) || !bytes.Contains(files["spend_cube.go"], []byte("type SpendCubeComponent struct")) {
		t.Fatal("generated component exposure or identity missing")
	}
}
