package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestValidateUsageAndFailureReport(t *testing.T) {
	for _, test := range []struct {
		args   []string
		status int
		json   bool
	}{
		{nil, 2, false},
		{[]string{"validate"}, 2, false},
		{[]string{"validate", "-help"}, 0, false},
		{[]string{"validate", "-format", "yaml", "example.com/app"}, 2, false},
		{[]string{"validate", "-dir", t.TempDir(), "-format", "json", "example.com/app"}, 1, true},
	} {
		var output, diagnostics bytes.Buffer
		if status := run(context.Background(), test.args, &output, &diagnostics); status != test.status {
			t.Fatalf("args=%v status=%d stdout=%s stderr=%s", test.args, status, &output, &diagnostics)
		}
		if test.json && !json.Valid(output.Bytes()) {
			t.Fatalf("invalid JSON report: %s", &output)
		}
	}
}

func TestValidateRealProject(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			status := run(context.Background(), []string{"validate", "-dir", "testdata/project", "-format", format, "example.com/validation/records"}, &output, &diagnostics)
			if status != 0 {
				t.Fatalf("status=%d stdout=%s stderr=%s", status, &output, &diagnostics)
			}
			if format == "json" {
				var report struct {
					Valid               bool
					Components, Skipped []string
				}
				if err := json.Unmarshal(output.Bytes(), &report); err != nil || !report.Valid || len(report.Components) != 1 || len(report.Skipped) == 0 {
					t.Fatalf("report=%+v err=%v", report, err)
				}
			} else if !strings.Contains(output.String(), "Static validation passed: true (1 components)") || !strings.Contains(output.String(), "Not checked:") {
				t.Fatal(output.String())
			}
			if _, err := os.Stat("testdata/project/generated"); !os.IsNotExist(err) {
				t.Fatalf("generated output changed: %v", err)
			}
		})
	}
}

func TestValidateExcludesSelection(t *testing.T) {
	var output, diagnostics bytes.Buffer
	status := run(context.Background(), []string{"validate", "-dir", "testdata/project", "-exclude", "example.com/validation/records", "example.com/validation/..."}, &output, &diagnostics)
	if status != 1 || !strings.Contains(output.String(), "no components found") {
		t.Fatalf("status=%d stdout=%s stderr=%s", status, &output, &diagnostics)
	}
}

func TestValidateSelectedExternalModule(t *testing.T) {
	var output, diagnostics bytes.Buffer
	status := run(context.Background(), []string{"validate", "-dir", "testdata/project", "-module-dir", "../external", "-format", "json", "example.com/external/records"}, &output, &diagnostics)
	var report struct {
		Valid      bool
		Components []string
	}
	err := json.Unmarshal(output.Bytes(), &report)
	if status != 0 || err != nil || !report.Valid || len(report.Components) != 1 || !strings.Contains(report.Components[0], "example.com/external/records") {
		t.Fatalf("status=%d report=%+v err=%v stderr=%s", status, report, err, &diagnostics)
	}
}
