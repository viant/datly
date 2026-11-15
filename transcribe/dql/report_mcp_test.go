package dql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReportTrailingMCPExposure(t *testing.T) {
	for _, name := range []string{"report", "cube"} {
		prefix := []string{"''", "''", "''", "''", "''", "''", "''", "true"}
		old, err := parseReportSettings(name, prefix)
		if err != nil || old.MCP != nil {
			t.Fatalf("legacy: %+v %v", old, err)
		}
		zero, err := parseReportSettings(name, nil)
		if err != nil || !reflect.DeepEqual(zero, &spec.ReportSettings{Enabled: true}) {
			t.Fatalf("zero: %+v %v", zero, err)
		}
		args := append(append([]string(nil), prefix...), "'aggregate_records'", "'Quoted, description'", "'docs/cube.md'", "'lc'", "false")
		actual, err := parseReportSettings(name, args)
		if err != nil {
			t.Fatal(err)
		}
		if actual.MCP.Name != "aggregate_records" || actual.MCP.Description != "Quoted, description" || actual.MCP.DescriptionPath != "docs/cube.md" || actual.MCP.CaseFormat != "lc" || actual.MCP.StrictArguments == nil || *actual.MCP.StrictArguments {
			t.Fatalf("exposure %+v", actual.MCP)
		}
		for _, bad := range [][]string{{"''"}, {"'valid'", "''", "''", "'bogus'"}, {"'valid'", "''", "''", "'lc'", "maybe"}} {
			if _, err := parseReportSettings(name, append(append([]string(nil), prefix...), bad...)); err == nil || !strings.Contains(err.Error(), name+" MCP exposure") {
				t.Fatalf("missing directive cause: %v", err)
			}
		}
	}
}

func TestReportMCPNativeDirectiveParsing(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('get_records','Source description'))
#setting($_ = $cube('', '', '', '', '', '', '', true, 'aggregate_records', 'Cube, description', 'docs/cube.md', 'lc', false))
SELECT id FROM records`
	component, err := parseComponentSource("example.com/records", "Records", source)
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings.Report.MCP.Name != "aggregate_records" || component.Routes[0].MCP[0].Name != "get_records" {
		t.Fatalf("independent exposures lost %+v", component.Settings.Report)
	}
	_, err = parseComponentSource("example.com/records", "Records", strings.Replace(source, "'aggregate_records'", "''", 1))
	if err == nil || !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("invalid directive error %v", err)
	}
}

func TestReportMCPInvalidExposureDiagnosticSpan(t *testing.T) {
	prefix := "#package('example.com/records')\n#setting($_ = $route('/records','GET'))\n"
	directive := "#setting($_ = $cube('', '', '', '', '', '', '', true, ''))"
	prepared := PrepareSource(prefix + directive + "\nSELECT id FROM records")
	found := false
	for _, diagnostic := range prepared.Diagnostics {
		if strings.Contains(diagnostic.Message, "empty name") {
			found = true
			if diagnostic.Offset != len(prefix) || diagnostic.End <= diagnostic.Offset {
				t.Fatalf("wrong directive span %+v", diagnostic)
			}
		}
	}
	if !found {
		t.Fatalf("missing location-aware error: %+v", prepared.Diagnostics)
	}
}
