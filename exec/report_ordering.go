package exec

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// ReportOrdering is a trusted, invocation-local permission, not a selector or
// transport input. It changes only the ordering gate; SQL validation still runs.
type ReportOrdering struct {
	target ComponentTarget
	view   string
	fields []string
}

type reportOrderingKey struct{}

// NewReportOrdering is for server-side report adapters targeting an exact route.
// Fields are selected scalar identities, not hidden relation dependencies. An
// empty set permits no ordering columns. SQL resolves and validates these names.
func NewReportOrdering(target ComponentTarget, view string, fields ...string) *ReportOrdering {
	target.Route.Method = strings.ToUpper(strings.TrimSpace(target.Route.Method))
	target.Route.Path = strings.TrimSpace(target.Route.Path)
	return &ReportOrdering{target: target, view: strings.TrimSpace(view), fields: append([]string{}, fields...)}
}

// EnterReportOrdering replaces, rather than inherits, the parent's permission.
// Runtime calls it at every component boundary, including input dependencies.
func EnterReportOrdering(ctx context.Context, target ComponentTarget, permission *ReportOrdering) (context.Context, error) {
	if permission != nil && (permission.target != target || permission.view == "") {
		return ctx, fmt.Errorf("report ordering permission does not match target %s", target.String())
	}
	var value ReportOrdering
	if permission != nil {
		value = *permission
	}
	return context.WithValue(ctx, reportOrderingKey{}, value), nil
}

// ForwardReportOrdering explicitly delegates the current report permission to
// a handler's designated reader. Ordinary handler calls have nothing to forward.
// Supplying this value on another target's request is rejected by runtime.
func ForwardReportOrdering(ctx context.Context, target ComponentTarget, view string) *ReportOrdering {
	current, _ := ctx.Value(reportOrderingKey{}).(ReportOrdering)
	if current.view == "" {
		return nil
	}
	return NewReportOrdering(target, view, current.fields...)
}

// AllowsReportOrdering checks the exact component and canonical view. Runtime
// has already checked the route when entering this invocation.
func AllowsReportOrdering(ctx context.Context, component spec.Key, view string) bool {
	permission, _ := ctx.Value(reportOrderingKey{}).(ReportOrdering)
	return permission.view != "" && permission.target.Component == component && permission.view == view
}

// ReportOrderingFields returns a detached set of selected scalar identities.
// Hidden relation keys do not become sortable merely by entering SQL projection.
func ReportOrderingFields(ctx context.Context, component spec.Key, view string) []string {
	if !AllowsReportOrdering(ctx, component, view) {
		return nil
	}
	permission := ctx.Value(reportOrderingKey{}).(ReportOrdering)
	return append([]string{}, permission.fields...)
}
