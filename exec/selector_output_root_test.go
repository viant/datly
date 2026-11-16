package exec

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSelectorOutputRootExactEntryAndDetachedBinding(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Name: "owner"}, Route: spec.RouteRef{Method: "GET", Path: "/root"}}
	row := reflect.TypeFor[struct{ ID int }]()
	root := &SelectorOutputRoot{View: "records", Path: []string{"Public"}, RowType: row, OutputType: reflect.TypeFor[struct{ Public []struct{ ID int } }](), Resolve: func(string) (SelectorFieldName, bool, error) {
		return SelectorFieldName{GoName: "ID", Index: []int{0}}, true, nil
	}}
	original := NewSelectorPresentation(target, func(reflect.Type, string) ([]SelectorFieldName, error) { return nil, nil }).WithFieldSelection(spec.FieldSelectionOutputOnly)
	selected := original.WithOutputRoot(root)
	root.Path[0] = "mutated"
	require.Nil(t, original.outputRoot)
	ctx, err := EnterSelectorPresentation(t.Context(), target, selected)
	require.NoError(t, err)
	first := PresentedSelectorOutputRoot(ctx, target.Component)
	require.Equal(t, []string{"Public"}, first.Path)
	first.Path[0] = "changed"
	require.Equal(t, []string{"Public"}, PresentedSelectorOutputRoot(ctx, target.Component).Path)
	wrong := target
	wrong.Route.Path = "/other"
	_, err = EnterSelectorPresentation(ctx, wrong, selected)
	require.Error(t, err)
	for _, entry := range []ComponentTarget{target, {Component: spec.Key{Name: "child"}}} {
		cleared, err := EnterSelectorPresentation(ctx, entry, nil)
		require.NoError(t, err)
		require.Nil(t, PresentedSelectorOutputRoot(cleared, target.Component))
	}
	_, err = EnterSelectorPresentation(ctx, target, NewSelectorPresentation(target, original.fields).WithOutputRoot(first))
	require.Error(t, err, "query projection cannot acquire public root authority")
}
