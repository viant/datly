package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
)

func TestGroupable_Apply(t *testing.T) {
	useCases := []struct {
		description string
		args        []string
		expected    bool
	}{
		{
			description: "defaults to true when flag omitted",
			args:        nil,
			expected:    true,
		},
		{
			description: "supports explicit false",
			args:        []string{"false"},
			expected:    false,
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			aView := &view.View{}
			fn := &groupable{}

			err := fn.Apply(useCase.args, nil, nil, aView)
			require.NoError(t, err)
			require.Equal(t, useCase.expected, aView.Groupable)
		})
	}
}

func TestGroupingEnabledAlias_Apply(t *testing.T) {
	aView := &view.View{}
	fn := &groupingEnabled{}

	err := fn.Apply(nil, nil, nil, aView)
	require.NoError(t, err)
	require.True(t, aView.Groupable)
	require.Equal(t, "grouping_enabled", fn.Name())
}
