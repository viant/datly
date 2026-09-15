package velty

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuilderGroupComposition(t *testing.T) {
	empty, err := (&Context{}).FilterGroup(99, "OR")
	require.NoError(t, err)
	for _, tc := range []struct {
		name          string
		builder       *Builder
		keyword, want string
	}{
		{"empty WHERE", (&Builder{}).CombineAnd(empty, " "), "WHERE", ""},
		{"empty AND", (&Builder{}).CombineOr(empty), "AND", ""},
		{"empty raw", (&Builder{}).Combine(empty), "", ""},
		{"skip empty between predicates", (&Builder{}).CombineAnd("scope", empty, "bounds"), "WHERE", "WHERE ( ( scope ) AND ( bounds ) )"},
		{"OR inside one call", (&Builder{}).CombineOr("name", "reference"), "", "( ( name ) OR ( reference ) )"},
		{"CombineOr still appends with default AND", (&Builder{}).CombineAnd("scope").CombineOr("name", "reference"), "", "( ( scope ) ) AND ( ( name ) OR ( reference ) )"},
		{"Or joins successive calls", (&Builder{}).CombineAnd("a").Or().CombineAnd("b"), "", "( ( a ) ) OR ( ( b ) )"},
		{"Or persists through empty calls", (&Builder{}).Combine("a").Or().Combine(empty).Combine("b").Combine("c"), "", "( ( a ) ) OR ( ( b ) ) OR ( ( c ) )"},
		{"And resets connector", (&Builder{}).Combine("a").Or().Combine("b").And().Combine("c"), "", "( ( a ) ) OR ( ( b ) ) AND ( ( c ) )"},
		{"explicit nested OR under scope", (&Builder{}).CombineAnd("scope", (&Builder{}).CombineOr("name", "reference").Build("")), "AND", "AND ( ( scope ) AND ( ( ( name ) OR ( reference ) ) ) )"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, strings.Join(strings.Fields(tc.builder.Build(tc.keyword)), " "))
		})
	}
}
