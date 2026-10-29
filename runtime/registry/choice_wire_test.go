package registry

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestChoiceNumericWireSchema(t *testing.T) {
	if err := validateWireSchema(&spec.WireSchema{Type: "number", Enum: []any{1.25, 2.5}}); err != nil {
		t.Fatal(err)
	}
	if err := validateWireSchema(&spec.WireSchema{Type: "number", Format: "date"}); err == nil {
		t.Fatal("numeric format metadata accepted")
	}
}
