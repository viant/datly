package tag

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestSelectorFieldSelectionRoundTrip(t *testing.T) {
	for _, mode := range []string{"query", "outputOnly"} {
		text := "rows,selectorFieldSelection=" + mode + ",selectorProjection=true"
		parsed, err := ParseView(text)
		require.NoError(t, err)
		require.Equal(t, spec.FieldSelectionMode(mode), parsed.Selector.FieldSelection)
		for i := 0; i < 2; i++ {
			value, err := parsed.Value()
			require.NoError(t, err)
			require.Equal(t, text, value)
			parsed, err = ParseView(value)
			require.NoError(t, err)
		}
	}
	_, err := ParseView("rows,selectorFieldSelection=unknown")
	require.ErrorContains(t, err, "unsupported selector field selection mode")
}
