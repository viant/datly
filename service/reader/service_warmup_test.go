package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
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

func TestWarmupIndexParameterDoesNotInferCamelCase(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "order_id"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("OrderId", state.NewQueryLocation("order_id"))),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.Nil(t, parameter)
}

func TestWarmupIndexParameterDoesNotFallbackToMatchingColumnName(t *testing.T) {
	aView := &view.View{
		Cache: &view.Cache{
			Warmup: &view.Warmup{IndexColumn: "order_id"},
		},
		Template: view.NewTemplate("",
			view.WithTemplateParameters(state.NewParameter("order_id", state.NewQueryLocation("order_id"))),
		),
	}

	parameter := warmupIndexParameter(aView)

	require.Nil(t, parameter)
}
