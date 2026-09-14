package builder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
)

func TestReviewProbeQualifiedNameDoesNotMatchCollapsedAlias(t *testing.T) {
	_, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("SELECT secret AS bid FROM users"),
		WithBuilderProjection([]string{"b.id"}),
		WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true}),
	)
	require.Error(t, err)
}

func TestReviewProbeOrderQualifiedNameDoesNotMatchCollapsedAlias(t *testing.T) {
	_, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("SELECT secret AS bid FROM users"),
		WithBuilderSelector(&xstate.Selector{OrderBy: "b.id"}),
		WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true}),
	)
	require.Error(t, err)
}

func TestReviewProbeJoinedWildcardRequiresRequestedQualifier(t *testing.T) {
	_, err := NewBuilder().Build(context.Background(),
		WithBuilderView(&data.View{Columns: []*data.Column{
			{Name: "AID", Column: "a.id"},
			{Name: "BID", Column: "b.id"},
		}}),
		WithBuilderSQL("SELECT a.*, b.* FROM users a JOIN users b ON a.id=b.id"),
		WithBuilderProjection([]string{"bid"}),
		WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true}),
	)
	require.Error(t, err)
}
