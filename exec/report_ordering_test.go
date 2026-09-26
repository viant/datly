package exec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReportOrderingBoundaries(t *testing.T) {
	parent := ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "report"}, Route: spec.RouteRef{Method: "GET", Path: "/report"}}
	child := ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "private"}, Route: spec.RouteRef{Method: "GET", Path: "/private"}}
	base, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	fields := []string{"total"}
	permission := NewReportOrdering(parent, "rows", fields...)
	fields[0] = "secret"
	ctx, err := EnterReportOrdering(base, parent, permission)
	require.NoError(t, err)
	deadline, _ := base.Deadline()
	actualDeadline, _ := ctx.Deadline()
	require.Equal(t, deadline, actualDeadline)
	names := ReportOrderingFields(ctx, parent.Component, "rows")
	require.Equal(t, []string{"total"}, names)
	names[0] = "secret"
	require.Equal(t, []string{"total"}, ReportOrderingFields(ctx, parent.Component, "rows"))
	require.True(t, AllowsReportOrdering(ctx, parent.Component, "rows"))
	require.False(t, AllowsReportOrdering(ctx, parent.Component, "other"))
	require.False(t, AllowsReportOrdering(ctx, child.Component, "rows"))
	cleared, err := EnterReportOrdering(ctx, parent, nil)
	require.NoError(t, err)
	require.False(t, AllowsReportOrdering(cleared, parent.Component, "rows"))
	require.Nil(t, ForwardReportOrdering(cleared, child, "rows"))
	permission = ForwardReportOrdering(ctx, child, "childRows")
	_, err = EnterReportOrdering(ctx, parent, permission)
	require.ErrorContains(t, err, "does not match")
	delegated, err := EnterReportOrdering(ctx, child, permission)
	require.NoError(t, err)
	require.True(t, AllowsReportOrdering(delegated, child.Component, "childRows"))
	require.False(t, AllowsReportOrdering(delegated, parent.Component, "rows"))
	require.True(t, AllowsReportOrdering(ctx, parent.Component, "rows"))
	require.Equal(t, []string{"total"}, ReportOrderingFields(delegated, child.Component, "childRows"))
	wrongRoute := child
	wrongRoute.Route.Path = "/other"
	_, err = EnterReportOrdering(ctx, wrongRoute, permission)
	require.Error(t, err)
	cancel()
	require.ErrorIs(t, delegated.Err(), context.Canceled)
}
