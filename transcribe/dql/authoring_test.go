package dql

import (
	"testing"
)

func TestDeclarationOccurrencesRetainRepeatedPredicateSpans(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#define($_ = $IDs<[]int>(query/ids).Optional().WithPredicate(0,'in','r','id').ApplyWhenAbsentPredicate(1,'expr','r.tenant_id = 7'))
SELECT records.* FROM (SELECT r.* FROM records r) records`
	actual, err := DeclarationOccurrences(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != 1 || len(actual[0].Predicates) != 2 {
		t.Fatalf("occurrences=%+v", actual)
	}
	for _, occurrence := range actual[0].Predicates {
		fragment := source[occurrence.Span.Start:occurrence.Span.End]
		if fragment == "" || fragment[0] != '.' {
			t.Fatalf("invalid predicate span %q", fragment)
		}
	}
	if !actual[0].Predicates[1].Predicate.ApplyWhenAbsent || actual[0].Predicates[1].Predicate.Group != 1 {
		t.Fatalf("predicate=%+v", actual[0].Predicates[1].Predicate)
	}
}
