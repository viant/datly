package tag

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestPredicateAuthOccurrenceRoundTrip(t *testing.T) {
	original := &spec.Predicate{Name: "handler", Group: 2, Auth: true, Args: []string{"example.com/auth.Scope"}}
	value, err := PredicateValue(original)
	require.NoError(t, err)
	parsed, err := ParsePredicates(reflect.StructTag("predicate:" + strconv.Quote(value) + ` predicate:"equal,r,ordinary"`))
	require.NoError(t, err)
	require.Len(t, parsed, 2)
	require.Equal(t, original, parsed[0])
	require.False(t, parsed[1].Auth)
	_, err = ParsePredicates(`predicate:"equal,r,id,auth=perhaps"`)
	require.Error(t, err)
}
