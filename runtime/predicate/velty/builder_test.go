package velty

import (
	"strings"
	"testing"
)

func TestBuilderPreservesMixedOperatorOrder(t *testing.T) {
	actual := (&Builder{}).
		Combine("a").
		Or().Combine("b").
		And().Combine("c").
		Build("WHERE")
	actual = strings.Join(strings.Fields(actual), " ")
	want := "WHERE ( ( a ) ) OR ( ( b ) ) AND ( ( c ) )"
	if actual != want {
		t.Fatalf("unexpected mixed builder expression: got %q want %q", actual, want)
	}
}

func TestBuilderPreservesGroupOperator(t *testing.T) {
	actual := (&Builder{}).CombineOr("a", "b").Build("")
	actual = strings.Join(strings.Fields(actual), " ")
	if actual != "( ( a ) OR ( b ) )" {
		t.Fatalf("unexpected OR group: %q", actual)
	}
}
