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

func TestDeclarationOccurrencesExposeHeadAndAllOptions(t *testing.T) {
	source := `#define($_ = $Limit<int>(query/limit).Optional().QuerySelector("Records").WithPredicate(0,"less_or_equal","r","id"))
SELECT * FROM records`
	actual, err := DeclarationOccurrences(source)
	if err != nil || len(actual) != 1 {
		t.Fatalf("occurrences=%+v err=%v", actual, err)
	}
	occurrence := actual[0]
	if got := source[occurrence.HeadSpan.Start:occurrence.HeadSpan.End]; got != `$Limit<int>(query/limit)` {
		t.Fatalf("head=%q", got)
	}
	if len(occurrence.Options) != 3 || occurrence.Options[0].Name != "Optional" || occurrence.Options[1].Name != "QuerySelector" || occurrence.Options[2].Name != "WithPredicate" {
		t.Fatalf("options=%+v", occurrence.Options)
	}
	for _, option := range occurrence.Options {
		if source[option.Span.Start] != '.' {
			t.Fatalf("option span=%+v text=%q", option.Span, source[option.Span.Start:option.Span.End])
		}
	}
}
