package writer

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

// Keep the disabled source-phase descriptor rejected without its old runtime.
func TestFiniteSourcePhaseMetadataCannotBypassOldModeGate(t *testing.T) {
	declaration := &spec.Reconciliation{Mode: "same-parent-root-first", SourcePhases: &spec.ReconciliationSourcePhases{}}
	metadata := &Metadata{Root: &Record{}, Component: &spec.Component{RootView: &spec.View{Reconciliation: declaration}}}
	err := validateReconciliation(metadata, reflect.TypeFor[qcInput](), reflect.TypeFor[qcOutput]())
	if err == nil || !strings.Contains(err.Error(), "SourcePhases requires source-phases") {
		t.Fatal("native direct descriptor bypass", err)
	}
}
