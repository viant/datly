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

func TestNewNopPredicateProducesEmptyTemplate(t *testing.T) {
	predicate := NewNopPredicate()
	if predicate == nil || predicate.Template == nil {
		t.Fatalf("expected nop predicate template")
	}
	if predicate.Template.Name != PredicateNop || predicate.Template.Source != "" {
		t.Fatalf("unexpected nop predicate: %#v", predicate.Template)
	}
}

func TestDefaultExtensionRegistersNopPredicate(t *testing.T) {
	predicate, err := Config.Predicates.Lookup(PredicateNop)
	if err != nil {
		t.Fatal(err)
	}
	if predicate == nil || predicate.Template == nil || predicate.Template.Source != "" {
		t.Fatalf("unexpected registered nop predicate: %#v", predicate)
	}
}
