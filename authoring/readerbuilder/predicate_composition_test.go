package readerbuilder

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func compositionFixture(chain string) string {
	return strings.Replace(baseDQL, "FROM records r)", "FROM records r "+chain+")", 1)
}

func TestInspectPredicateCompositionSourceOrderAndPersistentConnectors(t *testing.T) {
	first := `${predicate.Builder ( ).CombineAnd($predicate.FilterGroup(0, 'AND')).Or().CombineOr($predicate.FilterGroup('1', 'OR'), $predicate.ExpandWith(2, "AND")).Combine($predicate.Expand(3)).And().CombineAnd($predicate.FilterGroup(4, 'OR')).Build ('WHERE')}`
	second := `$predicate.Builder().CombineAnd($predicate.FilterGroup(5,"AND")).Build("AND")`
	source := compositionFixture(first + " /* kept */ " + second)
	views, expansions, err := inspectViewSources(source)
	if err != nil {
		t.Fatal(err)
	}
	got := inspectPredicateCompositions(source, views)
	if len(got) != 2 || len(expansions) != 6 {
		t.Fatalf("compositions=%+v expansions=%+v", got, expansions)
	}
	want := []PredicateCompositionTerm{
		{Operator: "AND", Connector: "AND", Groups: []int{0}},
		{Operator: "OR", Connector: "OR", Groups: []int{1, 2}},
		{Operator: "AND", Connector: "OR", Groups: []int{3}},
		{Operator: "AND", Connector: "AND", Groups: []int{4}},
	}
	if !got[0].Editable || got[0].View != "records" || got[0].Occurrence != 0 || got[1].Occurrence != 1 || !reflect.DeepEqual(got[0].Terms, want) || got[0].BuildKeyword != "WHERE" || got[1].BuildKeyword != "AND" {
		t.Fatalf("compositions=%+v", got)
	}
	for i, original := range []string{first, second} {
		span := got[i].SourceSpan
		if source[span.Start:span.End] != original {
			t.Fatalf("span=%+v actual=%q", span, source[span.Start:span.End])
		}
	}
}

func TestInspectPredicateCompositionIgnoresProtectedSQL(t *testing.T) {
	chain := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}`
	source := strings.Replace(baseDQL, "r.id,r.name", "r.id,r.name,'"+chain+"' AS literal,`"+chain+"` AS quoted", 1)
	source = strings.Replace(source, "FROM records r)", "FROM records r /* "+chain+" */\n-- "+chain+"\n"+chain+")", 1)
	views, _, err := inspectViewSources(source)
	if err != nil {
		t.Fatal(err)
	}
	got := inspectPredicateCompositions(source, views)
	if len(got) != 1 || !got[0].Editable || source[got[0].SourceSpan.Start:got[0].SourceSpan.End] != chain {
		t.Fatalf("compositions=%+v", got)
	}
}

func TestUpdatePredicateCompositionExactOccurrenceAndCompiledMetadata(t *testing.T) {
	first := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, 'AND'), $predicate.FilterGroup(1, 'OR')).Build('WHERE')}`
	second := `${predicate.Builder().CombineAnd($predicate.FilterGroup(2, "AND")).Build("AND")}`
	source := compositionFixture(first + "\n/* keep boundary */\n" + second)
	mutation := &PredicateCompositionMutation{View: "records", Occurrence: 0, BuildKeyword: "WHERE", Terms: []PredicateCompositionTerm{
		{Operator: "OR", Connector: "AND", Groups: []int{1, 0}},
	}}
	candidate, err := updatePredicateComposition(source, mutation)
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.Replace(source, first, `${predicate.Builder().And().CombineOr($predicate.FilterGroup(1, 'OR'), $predicate.FilterGroup(0, 'AND')).Build("WHERE")}`, 1)
	if candidate != expected {
		t.Fatalf("DQL=%s", candidate)
	}
	mutation.Occurrence = 1
	mutation.BuildKeyword = "AND"
	mutation.Terms = []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{2}}}
	candidate, err = updatePredicateComposition(source, mutation)
	if err != nil || !strings.Contains(candidate, first) {
		t.Fatalf("error=%v DQL=%s", err, candidate)
	}
}

func TestUpdatePredicateCompositionMultiStage(t *testing.T) {
	chain := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND"),$predicate.FilterGroup(1,"OR"),$predicate.FilterGroup(2,"AND")).Build("WHERE")}`
	mutation := &PredicateCompositionMutation{View: "records", BuildKeyword: "WHERE", Terms: []PredicateCompositionTerm{
		{Operator: "OR", Connector: "AND", Groups: []int{0, 1}},
		{Operator: "AND", Connector: "AND", Groups: []int{2}},
	}}
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: compositionFixture(chain), Operation: Operation{Type: OperationUpdatePredicateComposition, PredicateComposition: mutation}})
	if !response.Applied || !reflect.DeepEqual(response.Structure.PredicateCompositions[0].Terms, mutation.Terms) {
		t.Fatalf("response=%+v", response)
	}
}

func TestUpdatePredicateCompositionCompilesAndPreservesOtherView(t *testing.T) {
	chain := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR")).Build("WHERE")}`
	source := strings.Replace(compositionFixture(chain), "SELECT records.*", "SELECT records.*,items.*", 1)
	child := `JOIN (SELECT i.id,i.record_id FROM items i ${predicate.Builder().CombineAnd($predicate.FilterGroup(2,"AND")).Build("WHERE")}) items ON items.record_id=records.id`
	source += "\n" + child
	mutation := &PredicateCompositionMutation{View: "records", BuildKeyword: "WHERE", Terms: []PredicateCompositionTerm{{Operator: "OR", Connector: "AND", Groups: []int{1, 0}}}}
	response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationUpdatePredicateComposition, PredicateComposition: mutation}})
	if !response.Applied || len(response.Diagnostics) != 0 || !strings.HasSuffix(response.DQL, child) || len(response.Structure.PredicateCompositions) != 2 {
		t.Fatalf("response=%+v", response)
	}
	if response.Structure.PredicateCompositions[1].Occurrence != 0 || response.Structure.PredicateCompositions[1].View != "items" {
		t.Fatalf("compositions=%+v", response.Structure.PredicateCompositions)
	}
}

func TestUpdatePredicateCompositionRejectsInvalidTargetsAndGroups(t *testing.T) {
	chain := `${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND"),$predicate.FilterGroup(1,"OR")).Build("WHERE")}`
	source := compositionFixture(chain)
	for _, test := range []struct {
		name     string
		mutation PredicateCompositionMutation
	}{
		{"missing view", PredicateCompositionMutation{View: "unknown", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0, 1}}}}},
		{"missing occurrence", PredicateCompositionMutation{View: "records", Occurrence: 1, Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0, 1}}}}},
		{"missing group", PredicateCompositionMutation{View: "records", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0}}}}},
		{"added group", PredicateCompositionMutation{View: "records", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0, 1, 2}}}}},
		{"duplicate group", PredicateCompositionMutation{View: "records", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0, 1, 1}}}}},
		{"empty term", PredicateCompositionMutation{View: "records", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND"}}}},
		{"invalid operator", PredicateCompositionMutation{View: "records", Terms: []PredicateCompositionTerm{{Operator: "XOR", Connector: "AND", Groups: []int{0, 1}}}}},
		{"invalid prefix", PredicateCompositionMutation{View: "records", BuildKeyword: "OR", Terms: []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{0, 1}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := New(Config{Name: "Records"}).Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationUpdatePredicateComposition, PredicateComposition: &test.mutation}})
			if response.Applied || response.DQL != source || len(response.Diagnostics) == 0 {
				t.Fatalf("response=%+v", response)
			}
		})
	}
}

func TestPredicateCompositionAmbiguousAndNestedChainsAreNotRewritten(t *testing.T) {
	for _, chain := range []string{
		`${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND"),$predicate.FilterGroup(0,"OR")).Build("WHERE")}`,
		`${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND"),$predicate.Builder().CombineOr($predicate.FilterGroup(1,"OR")).Build("")).Build("WHERE")}`,
	} {
		source := compositionFixture(chain)
		views, _, err := inspectViewSources(source)
		if err != nil {
			t.Fatal(err)
		}
		got := inspectPredicateCompositions(source, views)
		if len(got) != 1 || source[got[0].SourceSpan.Start:got[0].SourceSpan.End] != chain {
			t.Fatalf("compositions=%+v", got)
		}
		_, err = updatePredicateComposition(source, &PredicateCompositionMutation{View: "records", BuildKeyword: "WHERE", Terms: got[0].Terms})
		if err == nil {
			t.Fatal("ambiguous or nested composition was accepted")
		}
	}
}

func TestPredicateCompositionProtectsAuthorizationAcrossEveryTerm(t *testing.T) {
	chain := `${predicate.Builder().CombineAnd($predicate.FilterGroup(99,"AND"),$predicate.FilterGroup(1,"OR"),$predicate.FilterGroup(2,"AND")).Build("WHERE")}`
	source := compositionFixture(chain)
	for _, terms := range [][]PredicateCompositionTerm{
		{{Operator: "OR", Connector: "AND", Groups: []int{99, 1, 2}}},
		{{Operator: "AND", Connector: "AND", Groups: []int{99}}, {Operator: "AND", Connector: "OR", Groups: []int{1, 2}}},
		{{Operator: "AND", Connector: "AND", Groups: []int{1}}, {Operator: "AND", Connector: "OR", Groups: []int{2}}, {Operator: "AND", Connector: "AND", Groups: []int{99}}},
	} {
		if _, err := updatePredicateComposition(source, &PredicateCompositionMutation{View: "records", BuildKeyword: "WHERE", Terms: terms}); err == nil {
			t.Fatal("authorization bypass accepted")
		}
	}
	terms := []PredicateCompositionTerm{{Operator: "AND", Connector: "AND", Groups: []int{99}}, {Operator: "OR", Connector: "AND", Groups: []int{1, 2}}}
	if _, err := updatePredicateComposition(source, &PredicateCompositionMutation{View: "records", BuildKeyword: "WHERE", Terms: terms}); err != nil {
		t.Fatal(err)
	}
}
