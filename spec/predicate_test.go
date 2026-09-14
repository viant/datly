package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPredicateJSONUsesApplyWhenAbsent(t *testing.T) {
	data, err := json.Marshal(Predicate{Name: "tenant", ApplyWhenAbsent: true})
	if err != nil {
		t.Fatalf("marshal predicate: %v", err)
	}
	actual := string(data)
	if !strings.Contains(actual, `"applyWhenAbsent":true`) {
		t.Fatalf("predicate JSON must expose applyWhenAbsent: %s", actual)
	}
	if strings.Contains(actual, `"ensure"`) {
		t.Fatalf("predicate JSON must not expose obsolete ensure vocabulary: %s", actual)
	}
}

func TestPredicateJSONRejectsEnsure(t *testing.T) {
	var predicate Predicate
	err := json.Unmarshal([]byte(`{"name":"tenant","ensure":true}`), &predicate)
	if err == nil || !strings.Contains(err.Error(), "use applyWhenAbsent") {
		t.Fatalf("expected obsolete ensure field to fail, got %v", err)
	}
}
