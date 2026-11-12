package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state/kind/locator"
	hstate "github.com/viant/xdatly/handler/state"
)

func TestRedirectPreservesDelegatedQuerySelector(t *testing.T) {
	selector := &hstate.NamedQuerySelector{
		Name: "ad_cube",
		QuerySelector: hstate.QuerySelector{
			Fields:  []string{"Bids", "Impressions"},
			OrderBy: "Bids DESC",
			Limit:   1,
		},
	}
	stateOptions := hstate.NewOptions(hstate.WithQuerySelector(selector))

	actual := locator.NewOptions(appendRedirectQuerySelectorOptions(nil, stateOptions))
	require.Len(t, actual.QuerySelectors, 1)
	require.Same(t, selector, actual.QuerySelectors[0])
	require.Equal(t, []string{"Bids", "Impressions"}, actual.QuerySelectors[0].Fields)
	require.Equal(t, "Bids DESC", actual.QuerySelectors[0].OrderBy)
	require.Equal(t, 1, actual.QuerySelectors[0].Limit)
}
