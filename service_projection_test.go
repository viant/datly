package datly

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type fullProjectionOutput struct {
	AccountID   int     `json:"accountId" sqlx:"account_id"`
	CampaignID  int     `json:"campaignId" sqlx:"campaign_id"`
	Impressions int     `json:"impressions"`
	Spend       float64 `json:"spend"`
}

type alternateProjectionOutput struct {
	Campaign int `json:"campaignId"`
	Spend    int `sqlx:"spend"`
}

type sourceProjectionOutput struct {
	AliasValue int `source:"src.alias_value"`
}

type ignoredEmbeddedProjection struct {
	AccountID int `json:"accountId"`
}

func TestProjectionColumnsForOutput_BuildsProjectionFromDestinationType(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output []alternateProjectionOutput

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.NoError(t, err)
	require.Equal(t, []string{"campaign_id", "spend"}, columns)
}

func TestProjectionColumnsForOutput_IgnoresNonSliceOutput(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output struct {
		Status string `json:"status"`
	}

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.NoError(t, err)
	require.Nil(t, columns)
}

func TestProjectionColumnsForOutput_FailsForEmptyProjectionDTO(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output []struct{}

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.Error(t, err)
	require.Nil(t, columns)
}

func TestProjectionColumnsForOutput_UsesSourceAliases(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output []sourceProjectionOutput

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.NoError(t, err)
	require.Equal(t, []string{"alias_value"}, columns)
}

func TestProjectionColumnsForOutput_IgnoresSkippedAnonymousEmbeds(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output []struct {
		ignoredEmbeddedProjection `json:"-"`
		Spend                     int `json:"spend"`
	}

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.NoError(t, err)
	require.Equal(t, []string{"spend"}, columns)
}

func TestProjectionColumnsForOutput_FailsForUnknownField(t *testing.T) {
	aComponent := projectionTestComponent(t, reflect.TypeOf(fullProjectionOutput{}))
	var output []struct {
		Unknown int `json:"unknown"`
	}

	columns, err := view.ProjectionColumnsForOutput(aComponent.View, &output)

	require.Error(t, err)
	require.Nil(t, columns)
}

func TestSessionViewProjectionColumns_NarrowsAfterStatePopulation(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	columns := []string{"account_id", "bids"}
	aSession := session.New(aComponent.View, session.WithViewProjectionColumns(aComponent.View.Name, columns))

	err := aSession.SetViewState(context.Background(), aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.True(t, statelet.Has("account_id"))
	require.True(t, statelet.Has("bids"))
	require.False(t, statelet.Has("campaign_id"))

	query, err := reader.NewBuilder().CacheSQL(context.Background(), aComponent.View, statelet)

	require.NoError(t, err)
	require.Contains(t, query.SQL, "SELECT  account_id,  bids FROM")
	require.Contains(t, query.SQL, "GROUP BY 1")
	require.NotContains(t, query.SQL, "campaign_id")
}

func TestSessionApplyOutputProjectionFromContext_NarrowsChildView(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct {
		AccountID int `json:"accountId"`
		Bids      int `json:"bids"`
	}
	aSession := session.New(aComponent.View)
	ctx := ContextWithOutputProjection(context.Background(), &output)

	err := aSession.ApplyOutputProjection(ctx, aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.Equal(t, []string{"account_id", "bids"}, statelet.Columns)
	require.True(t, statelet.Has("account_id"))
	require.True(t, statelet.Has("bids"))
	require.False(t, statelet.Has("campaign_id"))

	query, err := reader.NewBuilder().Build(context.Background(), reader.WithBuilderView(aComponent.View), reader.WithBuilderStatelet(statelet))

	require.NoError(t, err)
	require.Contains(t, query.SQL, "SELECT  account_id,  bids FROM")
	require.Contains(t, query.SQL, "GROUP BY 1")
	require.NotContains(t, query.SQL, "campaign_id")
}

func TestSessionApplyOutputProjectionFromScopedContext_OnlyAppliesToMatchingView(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct {
		AccountID int `json:"accountId"`
		Bids      int `json:"bids"`
	}
	aSession := session.New(aComponent.View)
	ctx := ContextWithViewOutputProjection(context.Background(), "other_view", &output)

	err := aSession.ApplyOutputProjection(ctx, aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.Empty(t, statelet.Columns)

	ctx = ContextWithViewOutputProjection(context.Background(), aComponent.View.Name, &output)
	err = aSession.ApplyOutputProjection(ctx, aComponent.View)
	require.NoError(t, err)
	require.Equal(t, []string{"account_id", "bids"}, statelet.Columns)
}

func TestSessionApplyOutputProjectionFromOption_NarrowsChildView(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct {
		AccountID int `json:"accountId"`
		Bids      int `json:"bids"`
	}
	aSession := session.New(aComponent.View, session.WithOutputProjection(&output))

	err := aSession.ApplyOutputProjection(context.Background(), aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.Equal(t, []string{"account_id", "bids"}, statelet.Columns)
}

func TestSessionApplyOutputProjectionFromScopedOption_OnlyAppliesToMatchingView(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct {
		AccountID int `json:"accountId"`
		Bids      int `json:"bids"`
	}
	aSession := session.New(aComponent.View, session.WithViewOutputProjection("other_view", &output))

	err := aSession.ApplyOutputProjection(context.Background(), aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.Empty(t, statelet.Columns)

	aSession.Apply(session.WithViewOutputProjection(aComponent.View.Name, &output))
	err = aSession.ApplyOutputProjection(context.Background(), aComponent.View)
	require.NoError(t, err)
	require.Equal(t, []string{"account_id", "bids"}, statelet.Columns)
}

func TestSessionApplyOutputProjectionWithoutHint_LeavesChildViewFullWidth(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	aSession := session.New(aComponent.View)

	err := aSession.ApplyOutputProjection(context.Background(), aComponent.View)
	require.NoError(t, err)
	statelet := aSession.State().Lookup(aComponent.View)
	require.Empty(t, statelet.Columns)

	query, err := reader.NewBuilder().Build(context.Background(), reader.WithBuilderView(aComponent.View), reader.WithBuilderStatelet(statelet))

	require.NoError(t, err)
	require.Contains(t, query.SQL, "SELECT  t.account_id,  t.campaign_id,  t.bids FROM")
	require.Contains(t, query.SQL, "GROUP BY 1, 2")
}

func TestSessionApplyOutputProjectionNonSliceHint_DoesNotClearExistingProjection(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	aSession := session.New(aComponent.View)
	statelet := aSession.State().Lookup(aComponent.View)
	statelet.SetColumns([]string{"account_id", "bids"})
	var output struct {
		Status string `json:"status"`
	}
	ctx := ContextWithOutputProjection(context.Background(), &output)

	err := aSession.ApplyOutputProjection(ctx, aComponent.View)

	require.NoError(t, err)
	require.Equal(t, []string{"account_id", "bids"}, statelet.Columns)
}

func TestSessionApplyOutputProjectionFromContext_FailsForEmptyProjectionDTO(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct{}
	aSession := session.New(aComponent.View)
	ctx := ContextWithOutputProjection(context.Background(), &output)

	err := aSession.ApplyOutputProjection(ctx, aComponent.View)

	require.Error(t, err)
	require.Contains(t, err.Error(), "did not map any columns")
}

func TestSessionApplyOutputProjectionFromContext_FailsForUnknownField(t *testing.T) {
	aComponent := groupableProjectionTestComponent(t)
	var output []struct {
		Unknown int `json:"unknown"`
	}
	aSession := session.New(aComponent.View)
	ctx := ContextWithOutputProjection(context.Background(), &output)

	err := aSession.ApplyOutputProjection(ctx, aComponent.View)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to map output field Unknown")
}

func TestWithOutput_DoesNotEnableProjection(t *testing.T) {
	options := newOperateOptions([]OperateOption{WithOutput(&[]alternateProjectionOutput{})})

	require.NotNil(t, options.output)
}

func projectionTestComponent(t *testing.T, outputType reflect.Type) *repository.Component {
	t.Helper()
	aView := view.NewView("projection", "projection",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "account_id", DataType: "int"},
			&view.Column{Name: "campaign_id", DataType: "int"},
			&view.Column{Name: "impressions", DataType: "int"},
			&view.Column{Name: "spend", DataType: "float"},
			&view.Column{Name: "alias_value", DataType: "int", Tag: `source:"src.alias_value"`},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	output, err := state.NewType(state.WithSchema(state.NewSchema(outputType)))
	require.NoError(t, err)
	return &repository.Component{
		View: aView,
		Contract: contract.Contract{
			Output: contract.Output{
				Type: *output,
			},
		},
	}
}

func groupableProjectionTestComponent(t *testing.T) *repository.Component {
	t.Helper()
	aView := view.NewView("groupable_projection", "(SELECT account_id, campaign_id, SUM(bids) AS bids FROM bids GROUP BY 1, 2)",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "account_id", DataType: "int", Tag: `groupable:"true"`},
			&view.Column{Name: "campaign_id", DataType: "int", Tag: `groupable:"true"`},
			&view.Column{Name: "bids", DataType: "int", Aggregate: true},
		}),
		view.WithGroupable(true),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	output, err := state.NewType(state.WithSchema(state.NewSchema(reflect.TypeOf(fullProjectionOutput{}))))
	require.NoError(t, err)
	return &repository.Component{
		View: aView,
		Contract: contract.Contract{
			Output: contract.Output{
				Type: *output,
			},
		},
	}
}
