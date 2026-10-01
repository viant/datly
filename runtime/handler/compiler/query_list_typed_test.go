package compiler

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/runtime/handler/provider"
	"reflect"
	"testing"
)

func TestQueryListTypedInvocationValues(t *testing.T) {
	decoder := &queryListTransformer{target: reflect.TypeFor[[]string]()}
	for _, value := range [][]string{{"a,b", ""}, {}, nil} {
		got, err := decoder.Transform(context.Background(), nil, provider.TypedQueryValue{Value: value})
		require.NoError(t, err)
		require.Equal(t, value, got)
	}
	_, err := decoder.Transform(context.Background(), nil, provider.TypedQueryValue{Value: []int{1}})
	require.ErrorContains(t, err, "typed query value requires")
	got, err := decoder.Transform(context.Background(), nil, []string{"a,b", "c"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, got, "wire query occurrences still parse CSV")
}
