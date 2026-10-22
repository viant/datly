package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/builder"
	xstate "github.com/viant/xdatly/state"
)

func TestCanonicalWarmupIdentityIgnoresRequestOrdering(t *testing.T) {
	grouped := true
	view := &data.View{Spec: spec.View{Name: "cube", Groupable: &grouped, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}, Source: &spec.ViewSource{SQL: "SELECT ao.ad_order_id,ao.channel_id,SUM(ao.bids) AS bids FROM records ao GROUP BY ao.ad_order_id,ao.channel_id"}}}
	fields := []string{"ad_order_id", "bids"}
	var canonical string
	for _, order := range []string{"channel_id ASC", "channel_id DESC", "bids DESC", "2 ASC"} {
		requested := &xstate.Selector{Fields: []string{"ad_order_id", "channel_id", "bids"}, Columns: []string{"ad_order_id", "channel_id", "bids"}, OrderBy: order, Limit: 2, Offset: 1}
		before := requested.Clone()
		actual, err := builder.NewBuilder().Build(context.Background(), builder.WithBuilderView(view), builder.WithBuilderSelector(requested), builder.WithBuilderProjection(requested.Fields))
		require.NoError(t, err)
		require.Contains(t, actual.SQL, "ORDER BY")
		selector := canonicalWarmupSelector(requested, fields)
		identity, err := builder.NewBuilder().CacheSQL(context.Background(), builder.WithBuilderView(view), builder.WithBuilderSelector(selector), builder.WithBuilderProjection(selector.Fields))
		require.NoError(t, err)
		if canonical == "" {
			canonical = identity.SQL
		}
		require.Equal(t, canonical, identity.SQL)
		require.Equal(t, 2, identity.Limit)
		require.Equal(t, 1, identity.Offset)
		require.Equal(t, before, requested)
		require.False(t, warmupOrderingReplayable(requested))
		selector.Fields[0] = "changed"
		require.Equal(t, "ad_order_id", fields[0])
	}
	require.True(t, warmupOrderingReplayable(nil))
	require.True(t, warmupOrderingReplayable(&xstate.Selector{}))
}

func TestCanonicalWarmupSelectorPreservesAuthoredSourceOrderAndLimit(t *testing.T) {
	const source = "SELECT p.id FROM (SELECT id FROM records ORDER BY id DESC LIMIT 3) p"
	view := &data.View{Spec: spec.View{Name: "records", Source: &spec.ViewSource{SQL: source}, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true}}}
	selector := canonicalWarmupSelector(&xstate.Selector{OrderBy: "id ASC", Limit: 1, Offset: 1}, []string{"id"})
	query, err := builder.NewBuilder().CacheSQL(context.Background(), builder.WithBuilderView(view), builder.WithBuilderSelector(selector), builder.WithBuilderProjection(selector.Fields))
	require.NoError(t, err)
	require.Contains(t, query.SQL, "ORDER BY id DESC LIMIT 3")
	require.Equal(t, source, view.Spec.Source.SQL)
}
