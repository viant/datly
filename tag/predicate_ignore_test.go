package tag

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandalonePredicateIgnoreMarker(t *testing.T) {
	for _, value := range []reflect.StructTag{`predicate:"-"`, `predicate:" - "`, `format:"dateFormat=YYYY-MM-DD" predicate:"-"`} {
		predicates, err := ParsePredicates(value)
		require.NoError(t, err)
		require.Empty(t, predicates)
	}
	// An ignored occurrence cannot erase another predicate, including security metadata.
	predicates, err := ParsePredicates(`predicate:"-" predicate:"handler,example.com/access.Scope,auth=true,applyWhenAbsent=true"`)
	require.NoError(t, err)
	require.Len(t, predicates, 1)
	require.Equal(t, "handler", predicates[0].Name)
	require.True(t, predicates[0].Auth)
	require.True(t, predicates[0].ApplyWhenAbsent)
	for _, value := range []reflect.StructTag{`predicate:"missing_predicate"`, `predicate:"-,group=1"`} {
		predicates, err = ParsePredicates(value)
		require.NoError(t, err)
		require.Len(t, predicates, 1, "only a standalone ignore marker may be omitted")
	}
}
