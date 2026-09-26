package spec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuerySelectorExplicitPolicyWins(t *testing.T) {
	for _, property := range []SelectorProperty{SelectorPropertyFields, SelectorPropertyOrderBy, SelectorPropertyCriteria, SelectorPropertyLimit, SelectorPropertyOffset, SelectorPropertyPage} {
		for _, policy := range []string{"unspecified", "allow", "deny"} {
			t.Run(string(property)+"/"+policy, func(t *testing.T) {
				view := &View{Selector: &Selector{}}
				if policy != "unspecified" {
					require.NoError(t, view.Selector.SetPermission(property, policy == "allow"))
				}
				for i := 0; i < 2; i++ {
					require.NoError(t, view.EnableQuerySelector(property))
				}
				value, err := view.Selector.permissionField(property)
				require.NoError(t, err)
				require.Equal(t, policy != "deny", *value)
				require.Equal(t, policy != "unspecified", view.Selector.PermissionSpecified(property))
				if property == SelectorPropertyCriteria && policy == "deny" {
					require.Empty(t, view.Selector.Filterable)
				}
				encoded, err := json.Marshal(view.Selector)
				require.NoError(t, err)
				var restored Selector
				require.NoError(t, json.Unmarshal(encoded, &restored))
				require.Equal(t, view.Selector, &restored)
				if policy != "unspecified" {
					clone := restored.Clone()
					clone.Specified[0] = "other"
					require.True(t, restored.PermissionSpecified(property))
				}
			})
		}
	}
}
