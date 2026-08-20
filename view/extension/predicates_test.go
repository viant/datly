package extension

import (
	"strings"
	"testing"
)

func TestNewDurationPredicate_DoesNotUseLogicalOrInVelty(t *testing.T) {
	predicate := NewDurationPredicate()
	if predicate == nil || predicate.Template == nil {
		t.Fatalf("expected duration predicate template")
	}
	if strings.Contains(predicate.Template.Source, "||") {
		t.Fatalf("expected duration predicate template to avoid logical OR, got:\n%s", predicate.Template.Source)
	}
}
