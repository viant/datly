package generate

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestChoiceMetadataRejectsUnresolvedOutputType(t *testing.T) {
	view := &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "mode", Source: "mode", Type: spec.TypeRef{Name: "Unresolved", Package: "example.com/custom"}, Tag: `choice:"3" choice:"1" choice:"2"`}}}
	_, err := resolveScalarViewFieldsWithOmission(&Plan{Package: "example.com/generated"}, view, false, false)
	if err == nil || !strings.Contains(err.Error(), "requires resolved scalar Go type") {
		t.Fatalf("unresolved choice authority silently skipped: %v", err)
	}
}
