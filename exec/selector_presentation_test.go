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

func TestSelectorPresentationFieldSelectionBoundary(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Kind: "reader", Name: "root"}, Route: spec.RouteRef{Method: "GET", Path: "/root"}}
	callback := func(reflect.Type, string) ([]SelectorFieldName, error) { return nil, nil }
	ctx, err := EnterSelectorPresentation(t.Context(), target, NewSelectorPresentation(target, callback))
	require.NoError(t, err)
	require.Empty(t, PresentedSelectorFieldSelection(ctx, target.Component))
	ctx, err = EnterSelectorPresentation(ctx, target, NewSelectorPresentation(target, callback).WithFieldSelection(spec.FieldSelectionOutputOnly))
	require.NoError(t, err)
	require.Equal(t, spec.FieldSelectionOutputOnly, PresentedSelectorFieldSelection(ctx, target.Component))
	child := target
	child.Component.Name = "child"
	require.Empty(t, PresentedSelectorFieldSelection(ctx, child.Component))
	wrongRoute := target
	wrongRoute.Route.Path = "/other"
	_, err = EnterSelectorPresentation(ctx, wrongRoute, NewSelectorPresentation(target, callback).WithFieldSelection(spec.FieldSelectionOutputOnly))
	require.Error(t, err)
	for _, boundary := range []ComponentTarget{child, target} {
		cleared, err := EnterSelectorPresentation(ctx, boundary, nil)
		require.NoError(t, err)
		require.Empty(t, PresentedSelectorFieldSelection(cleared, target.Component))
		require.Equal(t, spec.FieldSelectionOutputOnly, PresentedSelectorFieldSelection(ctx, target.Component))
	}
	_, err = EnterSelectorPresentation(ctx, target, NewSelectorPresentation(target, callback).WithFieldSelection("invalid"))
	require.Error(t, err)
}

// The original constructor remains assignable to its pre-extension API type.
var _ func(ComponentTarget, func(reflect.Type, string) ([]SelectorFieldName, error)) *SelectorPresentation = NewSelectorPresentation

func TestSelectorPresentationModeCloneIsImmutable(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Name: "root"}}
	original := NewSelectorPresentation(target, func(reflect.Type, string) ([]SelectorFieldName, error) { return nil, nil })
	selected := original.WithFieldSelection(spec.FieldSelectionOutputOnly)
	require.NotSame(t, original, selected)
	require.Empty(t, original.fieldSelection)
	require.Equal(t, spec.FieldSelectionOutputOnly, selected.fieldSelection)
	require.Equal(t, original.target, selected.target)
	var absent *SelectorPresentation
	require.Nil(t, absent.WithFieldSelection(spec.FieldSelectionOutputOnly))
}
