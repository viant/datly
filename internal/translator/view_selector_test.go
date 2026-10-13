package translator

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	tparser "github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view"
)

func TestView_buildSelector_MergesDefaultConstraints(t *testing.T) {
	namespace := &Viewlet{
		Name: "vendor",
		Resource: &Resource{
			Declarations: &tparser.Declarations{
				QuerySelectors: map[string]inference.State{},
			},
		},
	}

	aView := &View{
		View: view.View{
			Name: "vendor",
			Selector: &view.Config{
				Constraints: &view.Constraints{
					OrderBy: true,
					OrderByColumn: map[string]string{
						"accountId": "ACCOUNT_ID",
					},
				},
			},
		},
	}

	rule := &Rule{
		Root: "vendor",
		Viewlets: Viewlets{
			registry: map[string]*Viewlet{
				"vendor": {Name: "vendor", View: aView},
			},
			keys: []string{"vendor"},
		},
	}

	aView.buildSelector(namespace, rule)

	require.NotNil(t, aView.Selector)
	require.NotNil(t, aView.Selector.Constraints)
	require.Equal(t, 25, aView.Selector.Limit)
	require.True(t, aView.Selector.Constraints.Criteria)
	require.True(t, aView.Selector.Constraints.Limit)
	require.True(t, aView.Selector.Constraints.Offset)
	require.True(t, aView.Selector.Constraints.Projection)
	require.True(t, aView.Selector.Constraints.OrderBy)
	require.Equal(t, "ACCOUNT_ID", aView.Selector.Constraints.OrderByColumn["accountId"])
	require.Equal(t, []string{"*"}, aView.Selector.Constraints.Filterable)
}
