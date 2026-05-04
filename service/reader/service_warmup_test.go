package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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
