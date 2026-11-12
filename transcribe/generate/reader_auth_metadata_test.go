package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/tag"
)

func TestReaderAuthorizationPredicateTagRoundTrip(t *testing.T) {
	source := `#setting($_ = $route('/records/{key}', 'GET'))
#define($_ = $Key<[]int>(path/key).WithPredicate(0,'in','r','id'))
#define($_ = $Tenant<int>(query/tenant).Tag('predicate:"equal,group=1,r,tenant,auth=true" predicate:"equal,group=2,r,ordinary"'))
#define($_ = $Data<[]*Row>(output/view).Tag('onEmpty:"empty"'))
SELECT id FROM records r`
	component, err := parseTestComponentSource("example.com/records", "Records", source)
	require.NoError(t, err)
	require.Len(t, component.Parameters[1].Predicates, 2)
	require.True(t, component.Parameters[1].Predicates[0].Auth)
	require.False(t, component.Parameters[1].Predicates[1].Auth)
	plan := testPlan(t, component)
	var found bool
	for _, field := range plan.Input.Fields {
		if field.Name != "Tenant" {
			continue
		}
		found = true
		predicates, err := tag.ParsePredicates(reflect.StructTag(field.Tag))
		require.NoError(t, err)
		require.Len(t, predicates, 2)
		require.True(t, predicates[0].Auth)
		require.False(t, predicates[1].Auth)
		require.Equal(t, []string{"r", "tenant"}, predicates[0].Args)
	}
	require.True(t, found)
	require.Equal(t, "empty", reflect.StructTag(plan.Output.Fields[0].Tag).Get("onEmpty"))
}
