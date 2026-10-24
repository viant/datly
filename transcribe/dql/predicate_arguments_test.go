package dql

import (
	"reflect"
	"testing"
)

func TestHandlerArgumentLiteralPreservation(t *testing.T) {
	source := `#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','auth.Scope','alias','t','enabled',true,'limit',7,null))`
	declarations, err := DeclarationOccurrences(source)
	if err != nil {
		t.Fatal(err)
	}
	predicate := declarations[0].Predicates[0].Predicate
	expected := []any{"alias", "t", "enabled", true, "limit", 7, nil}
	if !reflect.DeepEqual(expected, predicate.HandlerArgs) || !reflect.DeepEqual(predicate.Args, []string{"auth.Scope"}) {
		t.Fatalf("%#v", predicate)
	}
	rendered, err := RenderPredicateOption(predicate)
	if err != nil {
		t.Fatal(err)
	}
	source, err = ApplyPatch(source, declarations[0].Predicates[0].Span, rendered)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := DeclarationOccurrences(source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(predicate, roundTrip[0].Predicates[0].Predicate) {
		t.Fatalf("lost arguments: %s", source)
	}
	for _, bad := range []string{`name:'a'`, `$Request`, `{"alias":"t"}`, `[1,2]`, `1.5`} {
		if _, err := DeclarationOccurrences(`#define($_ = $X<int>(query/x).WithPredicate('handler','auth.Scope',` + bad + `))`); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
