package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerationSettingsOwnsTranscriptionControls(t *testing.T) {
	settings := &Settings{Generation: &GenerationSettings{
		Template: "patch", DescriptionResource: "docs/orders.md", ViewFile: "orders.go",
		InputFile: "orders_input.go", OutputFile: "orders_output.go", RouterFile: "orders_router.go",
	}}

	payload, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	actual := string(payload)
	for _, expected := range []string{`"generation"`, `"template":"patch"`, `"outputFile":"orders_output.go"`} {
		if !strings.Contains(actual, expected) {
			t.Fatalf("settings JSON %s does not contain %s", actual, expected)
		}
	}
	for _, obsolete := range []string{`"templateType"`, `"meta"`, `"dest"`, `"inputDest"`, `"outputDest"`, `"routerDest"`} {
		if strings.Contains(actual, obsolete) {
			t.Fatalf("settings JSON %s contains obsolete flat field %s", actual, obsolete)
		}
	}

	clone := settings.Clone()
	clone.Generation.OutputFile = "changed.go"
	if settings.Generation.OutputFile != "orders_output.go" {
		t.Fatal("settings clone aliases generation settings")
	}
}

func TestGenerationSettingsIsZero(t *testing.T) {
	if !(GenerationSettings{}).IsZero() {
		t.Fatal("empty generation settings reported non-zero")
	}
	if (GenerationSettings{OutputFile: "orders.go"}).IsZero() {
		t.Fatal("configured generation settings reported zero")
	}
}
