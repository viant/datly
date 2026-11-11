package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSelectorPresentationExactOwnerAndDetachedMetadata(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Kind: "reader", Name: "root"}, Route: spec.RouteRef{Method: "GET", Path: "/root"}}
	fields := []SelectorFieldName{{GoName: "MetricID", PublicName: "metricId", Index: []int{0}}}
	presentation := NewSelectorPresentation(target, func(reflect.Type, string) ([]SelectorFieldName, error) { return fields, nil })
	ctx, err := EnterSelectorPresentation(context.Background(), target, presentation)
	require.NoError(t, err)
	first, err := PresentedSelectorFields(ctx, target.Component, nil, "data")
	require.NoError(t, err)
	first[0].Index[0] = 99
	first[0].PublicName = "changed"
	second, err := PresentedSelectorFields(ctx, target.Component, nil, "data")
	require.NoError(t, err)
	require.Equal(t, fields, second)
	child := target
	child.Component.Name = "child"
	_, err = EnterSelectorPresentation(ctx, child, presentation)
	require.Error(t, err)
	absent, err := PresentedSelectorFields(ctx, child.Component, nil, "data")
	require.NoError(t, err)
	require.Nil(t, absent)
	childCtx, err := EnterSelectorPresentation(ctx, child, nil)
	require.NoError(t, err)
	absent, err = PresentedSelectorFields(childCtx, target.Component, nil, "data")
	require.NoError(t, err)
	require.Nil(t, absent)
	// A reentrant same-target call must also supply its own capability.
	reentrant, err := EnterSelectorPresentation(ctx, target, nil)
	require.NoError(t, err)
	absent, err = PresentedSelectorFields(reentrant, target.Component, nil, "data")
	require.NoError(t, err)
	require.Nil(t, absent)
}
