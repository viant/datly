package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
)

type warmupCloneInputHas struct {
	AdOrderID bool
}

type warmupCloneInput struct {
	AdOrderID int
	Has       *warmupCloneInputHas `setMarker:"true"`
}

func TestCloneStructologyState_DeepCopiesValueAndMarker(t *testing.T) {
	stateType := structology.NewStateType(reflect.TypeOf(warmupCloneInput{}))
	original := stateType.NewState()

	original.EnsureMarker()
	require.NoError(t, original.SetValue("AdOrderID", 2653813))

	origSelector, err := original.Selector("AdOrderID")
	require.NoError(t, err)
	require.Equal(t, 2653813, origSelector.Value(original.Pointer()))
	require.True(t, origSelector.Has(original.Pointer()))

	cloned := cloneStructologyState(original)
	require.NotNil(t, cloned)

	cloneSelector, err := cloned.Selector("AdOrderID")
	require.NoError(t, err)
	require.Equal(t, 2653813, cloneSelector.Value(cloned.Pointer()))

	require.NoError(t, cloneSelector.SetValue(cloned.Pointer(), 0))
	cloned.EnsureMarker()
	marker := cloned.Type().Marker()
	require.NotNil(t, marker)
	idx := marker.Index("AdOrderID")
	require.NotEqual(t, -1, idx)
	require.NoError(t, marker.Set(cloned.Pointer(), idx, false))

	require.Equal(t, 2653813, origSelector.Value(original.Pointer()))
	require.True(t, origSelector.Has(original.Pointer()))
	require.Equal(t, 0, cloneSelector.Value(cloned.Pointer()))
	require.False(t, cloneSelector.Has(cloned.Pointer()))
}

func TestRelationWarmupMatcherRequiresExactRelationKey(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "order_id"},
		},
	}
	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.JoinOn(view.WithLink("CampaignId", "campaign_id")),
		},
	}
	batchData := &view.BatchData{
		ColumnNames: []string{"campaign_id"},
		ValuesBatch: []interface{}{
			101,
		},
	}

	matcher, err := (&Service{}).relationWarmupMatcher(context.Background(), aView, view.NewStatelet(), batchData, relation)

	require.NoError(t, err)
	require.Nil(t, matcher)
}

func TestMatchesWarmupIndexColumnUsesReferenceColumn(t *testing.T) {
	link := view.WithLink("OrderId", "p.order_id")

	matched := matchesWarmupIndexColumn("order_id", link, "p.order_id")

	require.True(t, matched)
}

func TestMatchesWarmupIndexColumnRejectsFieldOnlyMatch(t *testing.T) {
	link := view.WithLink("OrderId", "campaign_id")

	matched := matchesWarmupIndexColumn("order_id", link, "campaign_id")

	require.False(t, matched)
}

func TestMatchesWarmupIndexColumnRejectsCollapsedIdentifier(t *testing.T) {
	link := view.WithLink("OrderId", "p.orderid")

	matched := matchesWarmupIndexColumn("order_id", link, "p.orderid")

	require.False(t, matched)
}

func TestWarmupMarkerColumnPrefersBatchColumnToken(t *testing.T) {
	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.JoinOn(view.WithLink("CampaignId", "t.campaign_id")),
		},
	}
	batchData := &view.BatchData{
		ColumnNames: []string{"Campaign_Id"},
	}

	actual := warmupMarkerColumn("CampaignId", relation, batchData)

	require.Equal(t, "CampaignId", actual)
}

func TestWarmupMarkerColumnFallsBackToRelationColumnToken(t *testing.T) {
	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.JoinOn(view.WithLink("CampaignId", "t.campaign_id")),
		},
	}

	actual := warmupMarkerColumn("CampaignId", relation, nil)

	require.Equal(t, "CampaignId", actual)
}

func TestWarmupMarkerColumnFallsBackToConfiguredIndexColumn(t *testing.T) {
	actual := warmupMarkerColumn("t.campaign_id", nil, nil)

	require.Equal(t, "campaign_id", actual)
}

func TestMatchesWarmupIndexAcceptsWarmupAliasForRelationField(t *testing.T) {
	aView := &view.View{}
	link := view.WithLink("CampaignId", "ID")

	matched := matchesWarmupIndex(aView, "CAMPAIGN_ID", link, "ID")

	require.True(t, matched)
}

func TestWarmupIndexParameterUsesExplicitParameter(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "order_id", IndexParameter: "OrderId"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("OrderId", state.NewQueryLocation("order_id"))),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.NotNil(t, parameter)
	require.Equal(t, "OrderId", parameter.Name)
}

func TestWarmupIndexParameterMatchesCanonicalQueryLocation(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "ad_order_id", IndexParameter: "order_id"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("OrderIds", state.NewQueryLocation("order_id"))),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.NotNil(t, parameter)
	require.Equal(t, "OrderIds", parameter.Name)
}

func TestWarmupIndexParameterMatchesFieldAliasPlural(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "ad_order_id", IndexParameter: "order_id"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("OrderIds", nil)),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.NotNil(t, parameter)
	require.Equal(t, "OrderIds", parameter.Name)
}

func TestWarmupIndexParameterDoesNotMatchUnrelatedParameter(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "ad_order_id", IndexParameter: "order_id"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("CampaignIds", state.NewQueryLocation("campaign_id"))),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.Nil(t, parameter)
}

func TestApplyRequestedFieldsPopulatesMatcherProjection(t *testing.T) {
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "order_id", DataType: "int", Tag: `json:"orderId"`, Groupable: true},
			{Name: "bids", DataType: "int", Aggregate: true},
			{Name: "impressions", DataType: "int", Aggregate: true},
		}),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	statelet := view.NewStatelet()
	statelet.SetColumns([]string{"impressions", "order_id"})
	matcher := &cache.ParmetrizedQuery{}

	require.NoError(t, applyRequestedFields(aView, statelet, matcher))

	require.Len(t, matcher.RequestedFields, 2)
	require.Equal(t, "impressions", matcher.RequestedFields[0].Name)
	require.Equal(t, "impressions", matcher.RequestedFields[0].MeasureKey)
	require.Equal(t, "order_id", matcher.RequestedFields[1].Name)
	require.Equal(t, "order_id", matcher.RequestedFields[1].DimensionKey)
}

func TestApplyRequestedFieldsIgnoresUnmappedProjection(t *testing.T) {
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "order_id", DataType: "int"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	statelet := view.NewStatelet()
	statelet.SetColumns([]string{"missing"})
	matcher := &cache.ParmetrizedQuery{}

	require.Error(t, applyRequestedFields(aView, statelet, matcher))

	require.Empty(t, matcher.RequestedFields)
}

func TestApplyRequestedFieldsUsesFullProjectionWhenRequestHasNoProjection(t *testing.T) {
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Groupable: true},
			{Name: "spend", DataType: "float", Aggregate: true},
		}),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	matcher := &cache.ParmetrizedQuery{}

	require.NoError(t, applyRequestedFields(aView, view.NewStatelet(), matcher))

	require.Len(t, matcher.RequestedFields, 2)
	require.Equal(t, "audience_id", matcher.RequestedFields[0].DimensionKey)
	require.Equal(t, "spend", matcher.RequestedFields[1].MeasureKey)
}

func TestApplyWarmupIdentityProjectionUsesWarmupFieldNames(t *testing.T) {
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Tag: `json:"audienceId"`, Groupable: true},
			{Name: "bids", DataType: "int", Aggregate: true},
			{Name: "spend", DataType: "float", Aggregate: true},
			{Name: "period_ecpm", DataType: "float", Aggregate: true},
		}),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{
		Warmup: &view.Warmup{
			FieldNames: []string{"audience_id", "bids", "spend", "period_ecpm"},
		},
	}
	statelet := view.NewStatelet()
	statelet.SetColumns([]string{"audience_id", "spend", "period_ecpm"})
	statelet.Fields = []string{"AudienceId", "Spend", "PeriodEcpm"}

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, []string{"audience_id", "bids", "spend", "period_ecpm"}, statelet.Columns)
	require.Equal(t, []string{"audience_id", "bids", "spend", "period_ecpm"}, statelet.Fields)
}

func TestApplyWarmupIdentityProjectionUsesMatchingCaseFieldNames(t *testing.T) {
	periodParam := state.NewParameter("Period", state.NewQueryLocation("period"), state.WithParameterType(reflect.TypeOf("")))
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Groupable: true},
			{Name: "bids", DataType: "int", Aggregate: true},
			{Name: "spend", DataType: "float", Aggregate: true},
			{Name: "period_ecpm", DataType: "float", Aggregate: true},
		}),
		view.WithTemplate(view.NewTemplate("", view.WithTemplateParameters(periodParam))),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{
		Warmup: &view.Warmup{
			FieldNames: []string{"audience_id", "bids"},
			Cases: []*view.CacheParameters{
				{
					Set: []*view.ParamValue{
						{Name: "Period", Values: []interface{}{"today"}},
					},
					FieldNames: []string{"audience_id", "spend", "period_ecpm"},
				},
			},
		},
	}
	statelet := view.NewStatelet()
	statelet.Init(aView)
	require.NoError(t, periodParam.Set(statelet.Template, "today"))

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, []string{"audience_id", "spend", "period_ecpm"}, statelet.Columns)
}

func TestApplyWarmupIdentityProjectionMatchesDefaultOptionalCase(t *testing.T) {
	periodParam := state.NewParameter("Period", state.NewQueryLocation("period"), state.WithParameterType(reflect.TypeOf("")))
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Groupable: true},
			{Name: "bids", DataType: "int", Aggregate: true},
			{Name: "spend", DataType: "float", Aggregate: true},
		}),
		view.WithTemplate(view.NewTemplate("", view.WithTemplateParameters(periodParam))),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{
		Warmup: &view.Warmup{
			FieldNames: []string{"audience_id", "bids"},
			Cases: []*view.CacheParameters{
				{
					Set: []*view.ParamValue{
						{Name: "Period"},
					},
					FieldNames: []string{"audience_id", "spend"},
				},
			},
		},
	}
	statelet := view.NewStatelet()
	statelet.Init(aView)

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, []string{"audience_id", "spend"}, statelet.Columns)
}

func TestApplyWarmupIdentityProjectionClearsProjectionForAmbiguousCaseFieldNames(t *testing.T) {
	periodParam := state.NewParameter("Period", state.NewQueryLocation("period"), state.WithParameterType(reflect.TypeOf("")))
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Groupable: true},
			{Name: "bids", DataType: "int", Aggregate: true},
			{Name: "spend", DataType: "float", Aggregate: true},
		}),
		view.WithTemplate(view.NewTemplate("", view.WithTemplateParameters(periodParam))),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{
		Warmup: &view.Warmup{
			Cases: []*view.CacheParameters{
				{
					Set: []*view.ParamValue{
						{Name: "Period"},
					},
					FieldNames: []string{"audience_id", "bids"},
				},
				{
					Set: []*view.ParamValue{
						{Name: "Period"},
					},
					FieldNames: []string{"audience_id", "spend"},
				},
			},
		},
	}
	statelet := view.NewStatelet()
	statelet.Init(aView)
	statelet.SetColumns([]string{"audience_id", "spend"})

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.False(t, ok)

	require.Equal(t, []string{"audience_id", "spend"}, statelet.Columns)
	require.Empty(t, statelet.Fields)
}

func TestApplyWarmupIdentityProjectionAllowsEquivalentCaseFieldAliases(t *testing.T) {
	periodParam := state.NewParameter("Period", state.NewQueryLocation("period"), state.WithParameterType(reflect.TypeOf("")))
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int", Tag: `json:"audienceId"`, Groupable: true},
			{Name: "spend", DataType: "float", Aggregate: true},
		}),
		view.WithTemplate(view.NewTemplate("", view.WithTemplateParameters(periodParam))),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{
		Warmup: &view.Warmup{
			Cases: []*view.CacheParameters{
				{
					Set: []*view.ParamValue{
						{Name: "Period"},
					},
					FieldNames: []string{"AudienceId", "Spend"},
				},
				{
					Set: []*view.ParamValue{
						{Name: "Period"},
					},
					FieldNames: []string{"audience_id", "spend"},
				},
			},
		},
	}
	statelet := view.NewStatelet()
	statelet.Init(aView)

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.True(t, ok)

	require.Equal(t, []string{"audience_id", "spend"}, statelet.Columns)
}

func TestApplyWarmupIdentityProjectionClearsProjectionForFullWarmup(t *testing.T) {
	aView := view.NewView("events", "events",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			{Name: "audience_id", DataType: "int"},
			{Name: "spend", DataType: "float"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	aView.Cache = &view.Cache{Warmup: &view.Warmup{}}
	statelet := view.NewStatelet()
	statelet.SetColumns([]string{"audience_id", "spend"})
	statelet.Fields = []string{"AudienceId", "Spend"}

	ok, err := applyWarmupIdentityProjection(aView, statelet)
	require.NoError(t, err)
	require.True(t, ok)

	require.Empty(t, statelet.Columns)
	require.Empty(t, statelet.Fields)
}
