package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strconv"
	"testing"
)

func TestTypedHandlerArgumentsRoundTrip(t *testing.T) {
	expected := &spec.Predicate{Name: "handler", Group: 2, Args: []string{"example.Scope"}, HandlerArgs: []any{"alias", "r", "enabled", false, "limit", 12, "quoted,=\"", nil}}
	value, err := PredicateValue(expected)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePredicates(reflect.StructTag(`predicate:` + strconv.Quote(value)))
	if err != nil {
		t.Fatalf("%s: %v", value, err)
	}
	if len(got) != 1 || !reflect.DeepEqual(expected, got[0]) {
		t.Fatalf("%s: %#v", value, got)
	}
}
