package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func newWarmupsMatcherView(t *testing.T, cache *view.Cache) (*view.View, *state.Parameter, *state.Parameter) {
	t.Helper()
	advertiserParam := state.NewParameter("AdvertiserIds", state.NewQueryLocation("advertiser_id"), state.WithParameterType(reflect.TypeOf([]int{})))
	campaignParam := state.NewParameter("CampaignIds", state.NewQueryLocation("campaign_id"), state.WithParameterType(reflect.TypeOf([]int{})))
	aView := view.NewView("performance", "performance",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "advertiser_id", DataType: "int"},
			{Name: "campaign_id", DataType: "int"},
			{Name: "spend", DataType: "float"},
		}),
		view.WithTemplate(view.NewTemplate("", view.WithTemplateParameters(advertiserParam, campaignParam))),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = cache
	return aView, advertiserParam, campaignParam
}

func TestSelectTopLevelWarmup_MostSpecificByParameterPresence(t *testing.T) {
	cache := &view.Cache{
		Warmups: []*view.Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView, _, campaignParam := newWarmupsMatcherView(t, cache)
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, campaignParam.Set(statelet.Template, []int{101, 102}))

	candidate, err := selectTopLevelWarmup(aView, statelet)

	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Same(t, cache.Warmups[1], candidate.warmup)
	require.Equal(t, "CampaignIds", candidate.parameter.Name)
	require.Equal(t, []interface{}{101, 102}, candidate.values)
}

func TestSelectTopLevelWarmup_LaterDeclarationBreaksTies(t *testing.T) {
	cache := &view.Cache{
		Warmups: []*view.Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView, advertiserParam, campaignParam := newWarmupsMatcherView(t, cache)
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, advertiserParam.Set(statelet.Template, []int{7}))
	require.NoError(t, campaignParam.Set(statelet.Template, []int{101}))

	candidate, err := selectTopLevelWarmup(aView, statelet)

	require.NoError(t, err)
	require.NotNil(t, candidate)
	// No explicit priority: broad-to-specific declaration order makes the later,
	// more restrictive applicable warmup win deterministically.
	require.Same(t, cache.Warmups[1], candidate.warmup)
	require.Equal(t, []interface{}{101}, candidate.values)
}

func TestSelectTopLevelWarmup_ExplicitPriorityWins(t *testing.T) {
	cache := &view.Cache{
		Warmups: []*view.Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds", Priority: 10},
		},
	}
	aView, advertiserParam, campaignParam := newWarmupsMatcherView(t, cache)
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, advertiserParam.Set(statelet.Template, []int{7}))
	require.NoError(t, campaignParam.Set(statelet.Template, []int{101}))

	candidate, err := selectTopLevelWarmup(aView, statelet)

	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Same(t, cache.Warmups[1], candidate.warmup)
	require.Equal(t, []interface{}{101}, candidate.values)
}

func TestSelectTopLevelWarmup_PluralOverridesSingularAtEqualPriority(t *testing.T) {
	cache := &view.Cache{
		Warmup: &view.Warmup{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*view.Warmup{
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView, advertiserParam, campaignParam := newWarmupsMatcherView(t, cache)
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, advertiserParam.Set(statelet.Template, []int{7}))
	require.NoError(t, campaignParam.Set(statelet.Template, []int{101}))

	candidate, err := selectTopLevelWarmup(aView, statelet)

	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Same(t, cache.Warmups[0], candidate.warmup)
}

func TestSelectTopLevelWarmup_NoApplicableWarmup(t *testing.T) {
	cache := &view.Cache{
		Warmups: []*view.Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView, _, _ := newWarmupsMatcherView(t, cache)
	statelet := view.NewStatelet()
	statelet.Init(aView)

	candidate, err := selectTopLevelWarmup(aView, statelet)

	require.NoError(t, err)
	require.Nil(t, candidate)
}

func TestSelectRelationWarmup_PicksMatchingIndexColumn(t *testing.T) {
	cache := &view.Cache{
		Warmup: &view.Warmup{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*view.Warmup{
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView := &view.View{Cache: cache}
	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.JoinOn(view.WithLink("CampaignId", "campaign_id")),
		},
	}
	batchData := &view.BatchData{
		ColumnNames: []string{"campaign_id"},
		ValuesBatch: []interface{}{101},
	}

	selected := selectRelationWarmup(aView, relation, batchData)

	require.NotNil(t, selected)
	require.Same(t, cache.Warmups[0], selected)
}

func TestSelectRelationWarmup_NoMatch(t *testing.T) {
	cache := &view.Cache{
		Warmups: []*view.Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		},
	}
	aView := &view.View{Cache: cache}
	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.JoinOn(view.WithLink("CampaignId", "campaign_id")),
		},
	}
	batchData := &view.BatchData{
		ColumnNames: []string{"campaign_id"},
		ValuesBatch: []interface{}{101},
	}

	require.Nil(t, selectRelationWarmup(aView, relation, batchData))
}
