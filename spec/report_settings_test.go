package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReportSettingsOwnsLinkedInputType(t *testing.T) {
	payload, err := json.Marshal(&ReportSettings{
		LinkedInputType: "reporting.CubeInput",
		InputLayout:     &ReportInputLayout{Dimensions: "Groups", Measures: "Metrics"},
	})
	if err != nil {
		t.Fatal(err)
	}
	actual := string(payload)
	if !strings.Contains(actual, `"linkedInputType":"reporting.CubeInput"`) {
		t.Fatalf("report settings JSON = %s", actual)
	}
	if strings.Contains(actual, `"input"`) {
		t.Fatalf("report settings JSON contains obsolete input field: %s", actual)
	}
	if !strings.Contains(actual, `"inputLayout":{"dimensions":"Groups","measures":"Metrics"}`) {
		t.Fatalf("report settings JSON does not own a nested input layout: %s", actual)
	}
	decoded := map[string]json.RawMessage{}
	if err = json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, flat := range []string{"dimensions", "measures", "filters", "orderBy", "limit", "offset"} {
		if _, ok := decoded[flat]; ok {
			t.Fatalf("report settings JSON contains flat input layout field %q: %s", flat, actual)
		}
	}
}

func TestSettingsCloneIsolatesReportInputLayout(t *testing.T) {
	source := &Settings{Report: &ReportSettings{InputLayout: &ReportInputLayout{Dimensions: "Groups"}}}
	clone := source.Clone()
	clone.Report.InputLayout.Dimensions = "Changed"
	if source.Report.InputLayout.Dimensions != "Groups" {
		t.Fatalf("source report input layout was mutated: %+v", source.Report.InputLayout)
	}
}
