package compile

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestTargetedInlineWarmupUsesComponentOptionsAndMerge(t *testing.T) {
	root, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, i.*, p.*,
 use_cache(o,'shared'), cache_warmup(o,'startup'), cache_warmup(o,'','Limit=0'),
 cache_warmup(i,'order_id','IndexParameter=OrderId','Connector=prewarm','FieldNames=id,order_id','IndexMeta=true','Priority=2','Limit=0','Period=today','ExcludeDefault=Period'),
 cache_warmup(i,'order_id','IndexParameter=OrderId','IndexMeta=true','Period=yesterday'),
 cache_warmup(i,'id','Name=by-id','MaxCases=2','CaseRefs=recent'),
 cache_warmup(p,'id','Limit=0')
 FROM orders o JOIN items i ON i.order_id=o.id JOIN products p ON p.id=i.product_id`})
	require.NoError(t, err)
	require.Equal(t, "startup", root.Source.Bindings.CacheWarmup)
	require.Equal(t, "shared", root.Source.Bindings.CacheName)
	require.Len(t, root.Source.Bindings.InlineWarmups, 1)
	require.Empty(t, root.Source.Bindings.InlineWarmups[0].IndexColumn)
	require.NotNil(t, root.Source.Bindings.InlineWarmups[0].Limit)
	require.Zero(t, *root.Source.Bindings.InlineWarmups[0].Limit)
	child := root.Relations[0].View
	require.Len(t, child.Source.Bindings.InlineWarmups, 2)
	policy := child.Source.Bindings.InlineWarmups[0]
	require.Equal(t, "prewarm", policy.Connector)
	require.True(t, policy.IndexMeta)
	require.Equal(t, 2, policy.Priority)
	require.Equal(t, []string{"id", "order_id"}, policy.FieldNames)
	require.Nil(t, policy.MaxCases)
	require.NotNil(t, policy.Limit)
	require.Zero(t, *policy.Limit)
	require.Len(t, policy.Cases, 2)
	require.Equal(t, []string{"today"}, policy.Cases[0].Set[0].Values)
	require.True(t, policy.Cases[0].Set[0].ExcludeDefault)
	require.Equal(t, []string{"yesterday"}, policy.Cases[1].Set[0].Values)
	require.Equal(t, []string{"recent"}, child.Source.Bindings.InlineWarmups[1].CaseRefs)
	require.Len(t, child.Relations[0].View.Source.Bindings.InlineWarmups, 1)
	clone := root.Clone()
	clone.Relations[0].View.Source.Bindings.InlineWarmups[0].Cases[0].Set[0].Values[0] = "mutated"
	*clone.Source.Bindings.InlineWarmups[0].Limit = 7
	require.Equal(t, "today", policy.Cases[0].Set[0].Values[0])
	require.Zero(t, *root.Source.Bindings.InlineWarmups[0].Limit)
}

func TestTargetedInlineWarmupRejectsMalformedOptions(t *testing.T) {
	for _, control := range []string{
		`cache_warmup(o,'id','Limit=-1')`, `cache_warmup(o,'id','MaxCases=bad')`,
		`cache_warmup(o,'id','ExcludeDefault=Missing')`, `cache_warmup(o,'id',true)`,
		`cache_warmup(o,'')`, `cache_warmup(o,'id','not-an-option')`,
	} {
		t.Run(control, func(t *testing.T) {
			_, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Orders", Source: &spec.ViewSource{}}, SQL: `SELECT o.*, cache_warmup(o,'startup'), ` + control + ` FROM orders o`})
			require.Error(t, err)
		})
	}
}
