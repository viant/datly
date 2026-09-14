package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTypeRefIsZero(t *testing.T) {
	var ref TypeRef
	if !ref.IsZero() {
		t.Fatalf("expected zero typeref")
	}
	ref = TypeRef{Name: "Input"}
	if ref.IsZero() {
		t.Fatalf("expected non-zero typeref")
	}
}

func TestTypeRefHasNoDeclarativeLifecycleCapability(t *testing.T) {
	payload, err := json.Marshal(TypeRef{Package: "example.com/model", Name: "Input"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "capability") {
		t.Fatalf("type reference contains declarative lifecycle capability: %s", payload)
	}
}

func TestDiagnosticIsError(t *testing.T) {
	d := Diagnostic{Severity: SeverityError}
	if !d.IsError() {
		t.Fatalf("expected error diagnostic")
	}
}
