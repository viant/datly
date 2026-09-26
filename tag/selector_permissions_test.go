package tag

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSelectorExplicitDenialsRoundTrip(t *testing.T) {
	text := "rows,selectorProjection=false,selectorOrderBy=false,selectorCriteria=false,selectorLimit=false,selectorOffset=false,selectorPage=false"
	parsed, err := ParseView(text)
	require.NoError(t, err)
	value, err := parsed.Value()
	require.NoError(t, err)
	require.Equal(t, text, value)
	for _, property := range []spec.SelectorProperty{spec.SelectorPropertyFields, spec.SelectorPropertyOrderBy, spec.SelectorPropertyCriteria, spec.SelectorPropertyLimit, spec.SelectorPropertyOffset, spec.SelectorPropertyPage} {
		require.True(t, parsed.Selector.PermissionSpecified(property))
	}
	unspecified, err := ParseView("rows")
	require.NoError(t, err)
	value, err = unspecified.Value()
	require.NoError(t, err)
	require.Equal(t, "rows", value)
}
