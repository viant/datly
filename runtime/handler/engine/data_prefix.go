package engine

import (
	"context"
	"strings"

	"github.com/viant/datly/internal/drainowner"
)

// flushAuthority is captured once for one Engine.Execute activity. A retained
// capability cannot borrow a newer activity on the same database owner.
type flushAuthority struct {
	scope      *dataScope
	invocation *drainowner.Invocation
	frame      *drainowner.Frame
	activity   drainowner.Activity
	lifetime   context.Context
	tables     map[string]bool
}

func newFlushAuthority(scope *dataScope, activity drainowner.Activity, tables []string) *flushAuthority {
	root := scope
	if scope.root != nil {
		root = scope.root
	}
	allowed := make(map[string]bool, len(tables))
	for _, table := range tables {
		allowed[table] = true
	}
	return &flushAuthority{scope: scope, invocation: root.nativeInvocation, frame: scope.journalFrame, activity: activity, lifetime: root.invocationContext, tables: allowed}
}

func (a *flushAuthority) flush(ctx context.Context, table string) error {
	for i := 0; i < len(table); i++ {
		if table[i] > 127 {
			drainowner.FailProtected(a.invocation, drainowner.ErrPrefix)
			return drainowner.ErrPrefix
		}
	}
	if table == "" || table != strings.TrimSpace(table) || !a.tables[strings.ToLower(table)] {
		drainowner.FailProtected(a.invocation, drainowner.ErrPrefix)
		return drainowner.ErrPrefix
	}
	unit := a.scope.unit
	if unit == nil {
		unit = a.scope
	}
	return unit.nativeHandle.CallPrefix(ctx, unit.data, a.invocation, a.frame, a.activity, a.scope.data, strings.ToLower(table), a.lifetime)
}
