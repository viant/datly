package tag

import (
	"fmt"
	"testing"
)

func TestMutationPredicateRoundTrip(t *testing.T) {
	for _, group := range []int{0, 7} {
		original, err := ParseView(fmt.Sprintf("records,mutationPredicate=%d", group))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := original.Value()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := ParseView(encoded)
		if err != nil || actual.MutationPredicateGroup == nil || *actual.MutationPredicateGroup != group {
			t.Fatalf("round trip=%+v error=%v", actual, err)
		}
	}
	for _, input := range []string{"records,mutationPredicate=-1", "records,mutationPredicate=1.5", "records,mutationPredicate=7,mutationPredicate=8"} {
		if _, err := ParseView(input); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	negative := -1
	if _, err := (View{MutationPredicateGroup: &negative}).Value(); err == nil {
		t.Fatal("formatted negative group")
	}
}
