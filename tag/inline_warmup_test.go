package tag

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestViewInlineWarmupsRoundtrip(t *testing.T) {
	zero, max := 0, 5
	source := View{Name: "items", Cache: "shared", CacheWarmup: "fallback", InlineWarmups: []*spec.CacheWarmupSettings{
		nil, {Name: "by-order", IndexColumn: "order_id", IndexParameter: "OrderID", Connector: "prewarm", Priority: 2, IndexMeta: true, Limit: &zero, FieldNames: []string{"id", "order_id"}, CaseRefs: []string{"recent"}, Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Period", Values: []string{"yesterday", "last week's"}, ExcludeDefault: true}}}}},
		{Name: "all", MaxCases: &max},
	}}
	value, err := source.Value()
	require.NoError(t, err)
	parsed, err := ParseView(value)
	require.NoError(t, err)
	require.Equal(t, source.InlineWarmups, parsed.InlineWarmups)
	require.Equal(t, "fallback", parsed.CacheWarmup)
	require.Equal(t, "shared", parsed.Cache)
	parsed.InlineWarmups[1].Cases[0].Set[0].Values[0] = "changed"
	require.Equal(t, "yesterday", source.InlineWarmups[1].Cases[0].Set[0].Values[0])
	for _, invalid := range []string{"items,inlineWarmups=***", "items,inlineWarmups=e30"} {
		_, err := ParseView(invalid)
		require.Error(t, err)
	}
}
