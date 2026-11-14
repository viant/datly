package compiler

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/tagly/choice"
)

func TestQueryListOwnsCollectionEnumAfterNativeParsing(t *testing.T) {
	base := &queryListTransformer{target: reflect.TypeFor[[]string](), allowEmpty: true}
	enumeration, err := choice.ParseEnum(`mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`, base.target)
	require.NoError(t, err)
	configured, err := base.WithEnum(enumeration)
	require.NoError(t, err)
	selected := configured.(*queryListTransformer)
	require.Nil(t, base.enumeration, "prepared source must remain unchanged")
	for _, raw := range []any{"DISPLAY,VIDEO", []string{"DISPLAY", "VIDEO"}, provider.TypedQueryValue{Value: []string{"DISPLAY", "VIDEO"}}} {
		actual, err := selected.Transform(t.Context(), nil, raw)
		require.NoError(t, err)
		require.Equal(t, []string{"DISPLAY", "VIDEO"}, actual)
	}
	actual, err := selected.Transform(t.Context(), nil, "")
	require.NoError(t, err)
	require.Equal(t, []string{}, actual)
	_, err = selected.Transform(t.Context(), nil, "UNKNOWN")
	require.ErrorContains(t, err, "enum collection")
	cleared, err := selected.WithEnum(nil)
	require.NoError(t, err)
	actual, err = cleared.Transform(t.Context(), nil, "UNKNOWN")
	require.NoError(t, err)
	require.Equal(t, []string{"UNKNOWN"}, actual)
	require.Same(t, enumeration, selected.enumeration, "clearing another policy must not mutate prepared enum")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = selected.Transform(ctx, nil, "UNKNOWN")
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryListNumericEnumRetainsStorageType(t *testing.T) {
	target := reflect.TypeFor[[]int]()
	enumeration, err := choice.ParseEnum(`mcpEnum:"true" enumAccept:"literal" enumStyle:"literal" choice:"7:ALPHA" choice:"3:BETA"`, target)
	require.NoError(t, err)
	base := &queryListTransformer{target: target, allowEmpty: true}
	configured, err := base.WithEnum(enumeration)
	require.NoError(t, err)
	for _, raw := range []any{"3,7,3", []string{"3", "7", "3"}, provider.TypedQueryValue{Value: []int{3, 7, 3}}} {
		actual, err := configured.Transform(t.Context(), nil, raw)
		require.NoError(t, err)
		require.Equal(t, []int{3, 7, 3}, actual)
	}
	actual, err := configured.Transform(t.Context(), nil, "")
	require.NoError(t, err)
	require.Equal(t, []int{}, actual)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = configured.Transform(ctx, nil, provider.TypedQueryValue{Value: []int{3}})
	require.ErrorIs(t, err, context.Canceled)
}
