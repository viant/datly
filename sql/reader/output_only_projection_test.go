package reader

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"
)

func TestOutputOnlyProjectionSeparationAndStaticOwnership(t *testing.T) {
	view := &data.View{Spec: spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT v.* FROM (SELECT id,name FROM records) v"}}, Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Name", Column: "name"}}, SelectorFields: []data.SelectorField{{GoName: "Total", PublicName: "computed-total", OutputOnly: true, Index: []int{2}}}}
	plan, err := dsql.NewStaticProjection(view.Spec.Source.SQL, view, nil)
	require.NoError(t, err)
	for _, fields := range [][]string{{"id"}, {"id", "computed-total"}, {"computed-total"}, nil, {"computed-total", "unknown"}} {
		selector := &xstate.Selector{Fields: fields}
		projection := viewProjection(view, selector)
		selection, err := plan.Select(projection)
		if len(fields) == 2 && fields[1] == "unknown" {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		prepared, err := selection.Prepare(view.Spec.Source.SQL, view, view.Spec.Source.SQL)
		require.NoError(t, err)
		require.NotContains(t, prepared.Render(prepared.Source), "computed-total")
		require.Equal(t, fields, selector.Fields, "SQL filtering must not mutate relation/output selection")
		require.Error(t, selection.Validate(view.Spec.Source.SQL, view, []string{"foreign"}))
	}
}
func TestOutputOnlyNameCollisionAndGroupedRejection(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		view := &data.View{Spec: spec.View{Name: "records", Groupable: &grouped}, SelectorFields: []data.SelectorField{{GoName: "Total", PublicName: "value", OutputOnly: true, Index: []int{0}}}}
		if !grouped {
			view.SelectorFields = append(view.SelectorFields, data.SelectorField{GoName: "Mapped", PublicName: "value", Column: "id", Index: []int{1}})
		}
		component := &spec.Component{Name: "Records"}
		plan, err := NewPlan(PlanConfig{RootView: view, ViewIndex: NewViewIndex(component, view)})
		require.NoError(t, err)
		_, err = resolveInvocationSelectors(context.Background(), &Session{Component: component, Artifact: plan}, reflect.ValueOf(&struct{}{}), selectorBinder{selectors: xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: xstate.Selector{Fields: []string{"value"}}}}})
		require.Error(t, err)
		if grouped {
			require.ErrorContains(t, err, "grouped")
		} else {
			require.ErrorContains(t, err, "ambiguous")
		}
	}
}
