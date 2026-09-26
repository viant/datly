package builder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

func TestReportOrderingRetainsValidation(t *testing.T) {
	groupable := true
	policy := &spec.Selector{AllowFields: true, Orderable: []spec.FieldPath{"total"}, OrderAliases: map[string]spec.FieldPath{"revenue": "total"}}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "sales"}}
	view := &data.View{Spec: spec.View{Name: "sales", Groupable: &groupable, Selector: policy}}
	target := exec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/sales"}}
	ctx, err := exec.EnterReportOrdering(context.Background(), target, exec.NewReportOrdering(target, "sales", "total", "country"))
	require.NoError(t, err)
	opts := []BuilderOption{WithBuilderComponent(component), WithBuilderView(view),
		WithBuilderSQL("SELECT country, SUM(amount) AS total FROM sales GROUP BY country"),
		WithBuilderProjection([]string{"total"})}
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		selector  *xstate.Selector
		errorText string
	}{
		{"direct", context.Background(), &xstate.Selector{OrderBy: "total DESC"}, "order by is not allowed"},
		{"ranked", ctx, &xstate.Selector{OrderBy: "total DESC"}, ""},
		{"alias", ctx, &xstate.Selector{OrderBy: "revenue DESC"}, ""},
		{"unknown", ctx, &xstate.Selector{OrderBy: "unknown"}, "not in source projection"},
		{"restricted column", ctx, &xstate.Selector{OrderBy: "country"}, "not allowed"},
		{"injection", ctx, &xstate.Selector{OrderBy: "total DESC; DROP TABLE sales"}, "order by"},
		{"limit remains denied", ctx, &xstate.Selector{OrderBy: "total", Limit: 1}, "limit is not allowed"},
		{"offset remains denied", ctx, &xstate.Selector{OrderBy: "total", Offset: 1}, "offset is not allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBuilder().Build(tc.ctx, append(append([]BuilderOption{}, opts...), WithBuilderSelector(tc.selector))...)
			if tc.errorText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errorText)
				if tc.selector.Limit == 0 && tc.selector.Offset == 0 {
					var failure *response.Error
					require.ErrorAs(t, err, &failure)
					require.Equal(t, 400, failure.Code)
				}
			}
			require.False(t, policy.AllowOrderBy)
		})
	}
	// Removing the allowlist still cannot sort by an unselected grouping key.
	unrestricted := &spec.Selector{AllowFields: true}
	_, err = NewBuilder().Build(ctx, append(opts, WithBuilderSelectorPolicy(unrestricted), WithBuilderSelector(&xstate.Selector{OrderBy: "country"}))...)
	require.ErrorContains(t, err, "not selected in grouped projection")
	var failure *response.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, 400, failure.Code)
	// A grant for another view must not authorize the current view.
	wrong, err := exec.EnterReportOrdering(context.Background(), target, exec.NewReportOrdering(target, "other"))
	require.NoError(t, err)
	_, err = NewBuilder().Build(wrong, append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "total"}))...)
	require.ErrorContains(t, err, "order by is not allowed")
	// Cache construction uses the same policy and must not regress warmup reuse.
	_, err = NewBuilder().CacheSQL(ctx, append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "total"}))...)
	require.NoError(t, err)
	require.False(t, policy.AllowOrderBy)
	for _, fields := range [][]string{nil, {"total"}} {
		scoped, err := exec.EnterReportOrdering(context.Background(), target, exec.NewReportOrdering(target, "sales", fields...))
		require.NoError(t, err)
		// The full SQL projection may retain hidden keys. Ordinals cannot make
		// those keys sortable when they are absent from the report permission.
		_, err = NewBuilder().Build(scoped, append(opts,
			WithBuilderSelectorPolicy(unrestricted),
			WithBuilderProjection([]string{"country", "total"}),
			WithBuilderSelector(&xstate.Selector{OrderBy: "1 DESC"}))...)
		require.ErrorContains(t, err, "order by position 1 is not allowed")
	}
	query, err := NewBuilder().Build(ctx, append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "total"}))...)
	require.NoError(t, err)
	_, err = NewBuilder().QueryMatcher(ctx, query, append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "total"}))...)
	require.NoError(t, err)
}
