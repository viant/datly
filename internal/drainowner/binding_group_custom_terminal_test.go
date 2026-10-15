package drainowner

import (
	"context"
	"database/sql/driver"
	"errors"
	dexec "github.com/viant/datly/exec"
	sqlxread "github.com/viant/sqlx/io/read"
	"testing"
)

func TestScopedGroupTerminalDescendantAndNoEarlyParentOutcome(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, driver.ErrBadConn, &sqlxread.CleanupError{Cause: errors.New("close rows")}, dexec.NewPanicError("child", "panic")} {
		t.Run(cause.Error(), func(t *testing.T) {
			f := newScopedGroupFixture(t)
			ctx, l, d := f.reserve(t)
			a := f.admit(t, ctx)
			if e := FinishActivity(f.inv, a, cause); e != nil {
				t.Fatal(e)
			}
			if bindingContext(f.ctx).finished || bindingContext(f.ctx).failure != nil {
				t.Fatal("child published parent outcome")
			}
			if _, e := f.group.Enter(t.Context(), "b"); e == nil {
				t.Fatal("terminal child permitted sibling")
			}
			if e := d.Complete(cause); e != nil {
				t.Fatal(e)
			}
			if e := l.Close(); e != nil {
				t.Fatal(e)
			}
			if e := FinishActivity(f.inv, f.activity, cause); e != nil {
				t.Fatal(e)
			}
			if _, e := RecordBindingGroupFailure(f.group, "a", cause, true); e != nil {
				t.Fatal(e)
			}
			if e := f.group.Close(); !errors.Is(e, cause) {
				t.Fatal(e)
			}
			if e := CloseActivities(f.inv); !errors.Is(e, cause) {
				t.Fatal(e)
			}
		})
	}
}
