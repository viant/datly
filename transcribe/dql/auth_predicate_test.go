package dql

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/tag"
)

func TestWithAuthPredicatePreservesParameterizedOccurrences(t *testing.T) {
	for _, method := range []string{"WithAuthPredicate", "ApplyWhenAbsentAuthPredicate"} {
		t.Run(method, func(t *testing.T) {
			source := `#setting($_ = $route('/records','GET'))
#define($_ = $Scope<string>(header/Authorization).Required().` + method + `(2,'handler','security.Scope','Column','tenant_id','Alias','f','enabled',true,'limit',7,null).WithPredicate(3,'equal','f','label'))
SELECT 1 AS id`
			component, err := parseComponentSource("example.com/test", "Records", source)
			require.NoError(t, err)
			require.Len(t, component.Parameters, 1)
			predicates := component.Parameters[0].Predicates
			require.Len(t, predicates, 2)
			require.True(t, predicates[0].Auth)
			require.False(t, predicates[1].Auth)
			require.Equal(t, method == "ApplyWhenAbsentAuthPredicate", predicates[0].ApplyWhenAbsent)
			require.Equal(t, []any{"Column", "tenant_id", "Alias", "f", "enabled", true, "limit", 7, nil}, predicates[0].HandlerArgs)
			occurrences, err := DeclarationOccurrences(source)
			require.NoError(t, err)
			require.Equal(t, predicates[0], occurrences[0].Predicates[0].Predicate)
			rendered, err := RenderPredicateOption(predicates[0])
			require.NoError(t, err)
			require.Contains(t, rendered, "."+method+"(")
			patched, err := ApplyPatch(source, occurrences[0].Predicates[0].Span, rendered)
			require.NoError(t, err)
			again, err := parseComponentSource("example.com/test", "Records", patched)
			require.NoError(t, err)
			require.Equal(t, predicates, again.Parameters[0].Predicates)
			encoded, err := tag.PredicateValue(predicates[0])
			require.NoError(t, err)
			require.Contains(t, encoded, "auth=true")
			decoded, err := tag.ParsePredicates(reflect.StructTag("predicate:" + strconv.Quote(encoded)))
			require.NoError(t, err)
			require.Equal(t, predicates[0], decoded[0])
		})
	}
}
