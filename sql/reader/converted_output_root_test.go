package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/data"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type convertedRootSelectorBinder struct{ selectors xstate.Selectors }

func (b convertedRootSelectorBinder) Bind(context.Context, any, ...bindly.BindOption) error {
	return nil
}
func (b convertedRootSelectorBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	return b.selectors, key == xhandler.SelectorsKey, nil
}

func TestConvertedOutputRootRejectsChildFieldsKeepsSQLControlsAndResets(t *testing.T) {
	type sqlRow struct{ ID int }
	type publicRow struct {
		Computed string
		ID       int
	}
	type output struct {
		Rows   []sqlRow
		Public []publicRow
	}
	c := &spec.Component{Key: spec.Key{Name: "owner"}}
	rootView := &data.View{Spec: spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true}}}
	childView := &data.View{Spec: spec.View{Name: "children", Selector: &spec.Selector{AllowFields: true}}}
	rootPlan := &ViewPlan{View: rootView, Relations: []*RelationPlan{{Relation: &data.Relation{Holder: "Children"}, Target: &ViewPlan{View: childView}}}}
	index := NewViewIndex(c, rootView)
	index.add("children", childView)
	target := exec.ComponentTarget{Component: c.Key}
	binding := &exec.SelectorOutputRoot{View: "records", Path: []string{"Public"}, OutputType: reflect.TypeFor[output](), RowType: reflect.TypeFor[publicRow](), Resolve: func(name string) (exec.SelectorFieldName, bool, error) {
		if name == "computed" {
			return exec.SelectorFieldName{GoName: "Computed", Index: []int{0}}, true, nil
		}
		return exec.SelectorFieldName{}, false, nil
	}}
	presentation := exec.NewSelectorPresentation(target, func(reflect.Type, string) ([]exec.SelectorFieldName, error) { return nil, nil }).WithFieldSelection(spec.FieldSelectionOutputOnly).WithOutputRoot(binding)
	ctx, err := exec.EnterSelectorPresentation(t.Context(), target, presentation)
	require.NoError(t, err)
	session := &Session{Component: c, OutputType: reflect.TypeFor[output](), Artifact: &Plan{Root: rootPlan, OutputViewField: "Rows", ViewIndex: index}}
	_, err = resolveInvocationSelectors(ctx, session, reflect.ValueOf(&struct{}{}), convertedRootSelectorBinder{selectors: xstate.Selectors{&xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Fields: []string{"id"}}}}})
	require.ErrorContains(t, err, "converted output supports root Fields only")
	selectors, err := resolveInvocationSelectors(ctx, session, reflect.ValueOf(&struct{}{}), convertedRootSelectorBinder{selectors: xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: xstate.Selector{Fields: []string{"computed"}, Columns: []string{"ID"}, Limit: 2, Page: 3}}, &xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Columns: []string{"ID"}, OrderBy: "ID DESC", Limit: 1}}}})
	require.NoError(t, err)
	require.Equal(t, [][]int{{0}}, session.publicOutputIndexes)
	require.Equal(t, []string{"ID"}, selectors[rootView].Columns)
	require.Equal(t, "ID DESC", selectors[childView].OrderBy)
	require.Equal(t, 1, selectors[childView].Limit)
	// Reused sessions acquire no prior request indexes or inherited root binding.
	cleared, err := exec.EnterSelectorPresentation(ctx, target, nil)
	require.NoError(t, err)
	_, err = resolveInvocationSelectors(cleared, session, reflect.ValueOf(&struct{}{}), convertedRootSelectorBinder{})
	require.NoError(t, err)
	require.Nil(t, session.selectorOutputRoot)
	require.Nil(t, session.publicOutputIndexes)
}
