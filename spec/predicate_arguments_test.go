package spec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPredicateTypedArgumentsJSON(t *testing.T) {
	original := &Predicate{Name: "handler", Args: []string{"example.Handler"}, HandlerArgs: []any{"alias", "t", "enabled", true, "limit", 7, nil}}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got Predicate
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, &got) {
		t.Fatalf("round trip: %#v", got)
	}
	for _, bad := range []string{`{"handlerArgs":[{}]}`, `{"handlerArgs":[[]]}`, `{"handlerArgs":[1.2]}`} {
		if err = json.Unmarshal([]byte(bad), &got); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	cloned := (&Parameter{Predicates: []*Predicate{original}}).Clone()
	cloned.Predicates[0].HandlerArgs[0] = "changed"
	if original.HandlerArgs[0] != "alias" {
		t.Fatal("shared clone configuration")
	}
}

func TestPredicateUnmarshalJSONRejectsTrailingContent(t *testing.T) {
	// The initial map unmarshal validates the complete byte stream before the
	// UseNumber decoder runs. Preserve strictness even on direct method calls.
	for _, data := range []string{`{"name":"handler"} {}`, `{"name":"handler"} trailing`} {
		var predicate Predicate
		if err := predicate.UnmarshalJSON([]byte(data)); err == nil {
			t.Fatalf("accepted trailing content: %s", data)
		}
	}
}
