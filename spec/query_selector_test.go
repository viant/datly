package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQuerySelectorBindingJSONUsesProperty(t *testing.T) {
	data, err := json.Marshal(QuerySelectorBinding{View: "users", Property: SelectorPropertyLimit})
	if err != nil {
		t.Fatalf("marshal query selector binding: %v", err)
	}
	actual := string(data)
	if actual != `{"view":"users","property":"limit"}` {
		t.Fatalf("unexpected query selector JSON: %s", actual)
	}
	if strings.Contains(actual, "control") {
		t.Fatalf("query selector JSON must not expose obsolete control vocabulary: %s", actual)
	}
}

func TestSelectorPropertyForParam(t *testing.T) {
	testCases := map[string]SelectorProperty{
		" fields ": SelectorPropertyFields,
		"ORDERBY":  SelectorPropertyOrderBy,
		"offset":   SelectorPropertyOffset,
		"limit":    SelectorPropertyLimit,
		"page":     SelectorPropertyPage,
		"criteria": SelectorPropertyCriteria,
	}
	for input, want := range testCases {
		actual, ok := SelectorPropertyForParam(input)
		if !ok || actual != want {
			t.Errorf("SelectorPropertyForParam(%q) = %q, %v; want %q, true", input, actual, ok, want)
		}
	}
	if actual, ok := SelectorPropertyForParam("unknown"); ok || actual != "" {
		t.Fatalf("unsupported selector property = %q, %v; want empty, false", actual, ok)
	}
}
