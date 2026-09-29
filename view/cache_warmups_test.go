package view

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
)

func TestCacheEffectiveWarmups_Ordering(t *testing.T) {
	singular := &Warmup{IndexColumn: "advertiser_id"}
	campaign := &Warmup{IndexColumn: "campaign_id"}
	order := &Warmup{IndexColumn: "order_id"}

	var nilCache *Cache
	assert.Nil(t, nilCache.EffectiveWarmups())
	assert.False(t, nilCache.HasWarmup())

	singularOnly := &Cache{Warmup: singular}
	require.Equal(t, []*Warmup{singular}, singularOnly.EffectiveWarmups())
	assert.True(t, singularOnly.HasWarmup())

	pluralOnly := &Cache{Warmups: []*Warmup{campaign, nil, order}}
	require.Equal(t, []*Warmup{campaign, order}, pluralOnly.EffectiveWarmups())
	assert.True(t, pluralOnly.HasWarmup())

	mixed := &Cache{Warmup: singular, Warmups: []*Warmup{campaign, order}}
	require.Equal(t, []*Warmup{singular, campaign, order}, mixed.EffectiveWarmups())

	//an empty plural collection does not disable a valid singular warmup
	withEmptyPlural := &Cache{Warmup: singular, Warmups: []*Warmup{}}
	require.Equal(t, []*Warmup{singular}, withEmptyPlural.EffectiveWarmups())
	assert.True(t, withEmptyPlural.HasWarmup())

	//defensive copy: mutating the returned slice does not affect the cache
	effective := mixed.EffectiveWarmups()
	effective[0] = nil
	require.Equal(t, singular, mixed.Warmup)
}

func TestWarmupEffectiveName(t *testing.T) {
	assert.Equal(t, "campaign", (&Warmup{Name: "campaign", IndexParameter: "CampaignIds", IndexColumn: "campaign_id"}).EffectiveName())
	assert.Equal(t, "CampaignIds", (&Warmup{IndexParameter: "CampaignIds", IndexColumn: "campaign_id"}).EffectiveName())
	assert.Equal(t, "campaign_id", (&Warmup{IndexColumn: "campaign_id"}).EffectiveName())
	var nilWarmup *Warmup
	assert.Equal(t, "", nilWarmup.EffectiveName())
}

func newWarmupTestView(t *testing.T, cache *Cache, columns []*Column, parameters ...*state.Parameter) *View {
	aView := &View{
		Name:     "performance",
		Columns:  columns,
		Template: NewTemplate("", WithTemplateParameters(parameters...)),
		Cache:    cache,
	}
	aView.Template._parametersIndex = aView.Template.Parameters.Index()
	aView.indexColumns()
	aView.Cache.owner = aView
	return aView
}

func TestInitWarmup_RejectsDuplicateIdentity(t *testing.T) {
	param := state.NewParameter("AdvertiserIds", state.NewQueryLocation("advertiser_id"))
	cache := &Cache{
		Warmup:  &Warmup{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*Warmup{{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"}},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate warmup")
}

func TestInitWarmup_RejectsDuplicateIndexWithDifferentNames(t *testing.T) {
	param := state.NewParameter("AdvertiserIds", state.NewQueryLocation("advertiser_id"))
	cache := &Cache{
		Warmup:  &Warmup{Name: "broad", IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
		Warmups: []*Warmup{{Name: "duplicate", IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"}},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate warmup index identity")
}

func TestInitWarmup_InitializesEveryPluralWarmup(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	cache := &Cache{
		Warmups: []*Warmup{
			{IndexColumn: "advertiser_id", IndexParameter: "AdvertiserIds"},
			{IndexColumn: "campaign_id", IndexParameter: "CampaignIds"},
		},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}, {Name: "campaign_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	require.NoError(t, err)
	//each warmup owns its generated optional-parameter case; cases are never shared
	require.Len(t, cache.Warmups[0].Cases, 1)
	require.Len(t, cache.Warmups[1].Cases, 1)
	assert.NotSame(t, cache.Warmups[0].Cases[0], cache.Warmups[1].Cases[0])
}

func TestInitWarmup_RejectsUnknownPluralIndexColumn(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	cache := &Cache{
		Warmup:  &Warmup{IndexColumn: "advertiser_id"},
		Warmups: []*Warmup{{IndexColumn: "missing_column"}},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	require.Error(t, err)
}

func TestInitWarmup_ExpandsCaseRefsPerWarmup(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	recent := []*CacheParameters{
		{Set: []*ParamValue{{Name: "From", Values: []interface{}{"2026-01-01"}, ExcludeDefault: true}}},
	}
	cache := &Cache{
		SharedCases: map[string][]*CacheParameters{"recent": recent},
		Warmup: &Warmup{
			IndexColumn: "advertiser_id",
			CaseRefs:    []string{"recent"},
			Cases: []*CacheParameters{
				{Set: []*ParamValue{{Name: "From", Values: []interface{}{"2026-02-01"}, ExcludeDefault: true}}},
			},
		},
		Warmups: []*Warmup{
			{IndexColumn: "campaign_id", CaseRefs: []string{"recent"}},
		},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}, {Name: "campaign_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())
	require.NoError(t, err)

	//referenced cases come first, inline cases follow; each warmup gets its own clone
	require.Len(t, cache.Warmup.Cases, 2)
	assert.Equal(t, []interface{}{"2026-01-01"}, cache.Warmup.Cases[0].Set[0].Values)
	assert.Equal(t, []interface{}{"2026-02-01"}, cache.Warmup.Cases[1].Set[0].Values)
	require.Len(t, cache.Warmups[0].Cases, 1)
	assert.Equal(t, []interface{}{"2026-01-01"}, cache.Warmups[0].Cases[0].Set[0].Values)
	assert.NotSame(t, recent[0], cache.Warmup.Cases[0])
	assert.NotSame(t, cache.Warmup.Cases[0], cache.Warmups[0].Cases[0])

	//repeated expansion is idempotent thanks to per-warmup case dedup
	require.NoError(t, cache.expandWarmupCaseRefs(cache.Warmup))
	require.Len(t, cache.Warmup.Cases, 2)
}

func TestInitWarmup_RejectsUnknownCaseRef(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	cache := &Cache{
		Warmup: &Warmup{IndexColumn: "advertiser_id", CaseRefs: []string{"unknown"}},
	}
	aView := newWarmupTestView(t, cache, []*Column{{Name: "advertiser_id"}}, param)

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown")
}

func TestCacheCloneForInheritance_DeepCopiesPluralWarmups(t *testing.T) {
	source := &Cache{
		Warmup: &Warmup{IndexColumn: "advertiser_id", FieldNames: []string{"advertiser_id"}},
		Warmups: []*Warmup{
			{
				Name:        "campaign",
				Priority:    2,
				IndexColumn: "campaign_id",
				CaseRefs:    []string{"recent"},
				FieldNames:  []string{"campaign_id"},
				Cases: []*CacheParameters{
					{Set: []*ParamValue{{Name: "Period", Values: []interface{}{"today"}}}},
				},
			},
		},
		SharedCases: map[string][]*CacheParameters{
			"recent": {{Set: []*ParamValue{{Name: "Period", Values: []interface{}{"today"}}}}},
		},
	}

	cloned := source.cloneForInheritance()

	require.Len(t, cloned.Warmups, 1)
	require.NotSame(t, source.Warmups[0], cloned.Warmups[0])
	assert.Equal(t, "campaign", cloned.Warmups[0].Name)
	assert.Equal(t, 2, cloned.Warmups[0].Priority)
	assert.Equal(t, []string{"recent"}, cloned.Warmups[0].CaseRefs)
	require.NotSame(t, source.Warmups[0].Cases[0], cloned.Warmups[0].Cases[0])
	require.NotSame(t, source.SharedCases["recent"][0], cloned.SharedCases["recent"][0])

	cloned.Warmups[0].FieldNames[0] = "mutated"
	cloned.Warmups[0].Cases[0].Set[0].Values[0] = "mutated"
	cloned.SharedCases["recent"][0].Set[0].Values[0] = "mutated"

	assert.Equal(t, "campaign_id", source.Warmups[0].FieldNames[0])
	assert.Equal(t, "today", source.Warmups[0].Cases[0].Set[0].Values[0])
	assert.Equal(t, "today", source.SharedCases["recent"][0].Set[0].Values[0])
}

func TestCacheInheritIfNeeded_InheritsPluralWarmups(t *testing.T) {
	resource := EmptyResource()
	provider := &Cache{
		Name:         "shared",
		Location:     "cache_location",
		TimeToLiveMs: 1000,
		Warmup:       &Warmup{IndexColumn: "advertiser_id"},
		Warmups:      []*Warmup{{IndexColumn: "campaign_id"}},
		SharedCases: map[string][]*CacheParameters{
			"recent": {{Set: []*ParamValue{{Name: "Period", Values: []interface{}{"today"}}}}},
		},
	}
	resource.CacheProviders = append(resource.CacheProviders, provider)
	resource.indexProviders()

	inherited := NewRefCache("shared")
	require.NoError(t, inherited.inheritIfNeeded(context.Background(), resource, nil))

	require.NotNil(t, inherited.Warmup)
	require.NotSame(t, provider.Warmup, inherited.Warmup)
	assert.Equal(t, "advertiser_id", inherited.Warmup.IndexColumn)
	require.Len(t, inherited.Warmups, 1)
	require.NotSame(t, provider.Warmups[0], inherited.Warmups[0])
	assert.Equal(t, "campaign_id", inherited.Warmups[0].IndexColumn)
	require.NotSame(t, provider.SharedCases["recent"][0], inherited.SharedCases["recent"][0])
}

func TestCacheInheritIfNeeded_LocalWarmupsSuppressInheritance(t *testing.T) {
	resource := EmptyResource()
	provider := &Cache{
		Name:         "shared",
		Location:     "cache_location",
		TimeToLiveMs: 1000,
		Warmup:       &Warmup{IndexColumn: "advertiser_id"},
	}
	resource.CacheProviders = append(resource.CacheProviders, provider)
	resource.indexProviders()

	inherited := NewRefCache("shared")
	inherited.Warmups = []*Warmup{{IndexColumn: "campaign_id"}}
	require.NoError(t, inherited.inheritIfNeeded(context.Background(), resource, nil))

	assert.Nil(t, inherited.Warmup)
	require.Len(t, inherited.Warmups, 1)
	assert.Equal(t, "campaign_id", inherited.Warmups[0].IndexColumn)
}
