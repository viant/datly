package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
)

type retained49NativeBusiness struct {
	*writer.Handler
	calls *atomic.Int32
}

func (h *retained49NativeBusiness) Execute(ctx context.Context, in rh.Invocation) (any, error) {
	h.calls.Add(1)
	return h.Handler.Execute(ctx, in)
}

// Genuine ordinary native Flush reaches a real SQL driver callback. A new
// buffered child tries to enroll that same canonical root, fails and is caught.
// After drain return, a retained ordinary invoker must preserve the sticky root
// even when its caller replaces context. No ledger token or permit is fabricated.
func TestNativeRetained49DesiredStickyOrdinaryInvoker(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprintf("replaced=%t", replaced), func(t *testing.T) {
			db := activity49DB(t)
			var retained dexec.ComponentInvoker
			var once sync.Once
			var enrollment, drainError, ordinaryError, bufferedError, capturedError error
			var ordinaryValue any
			var ordinaryCalls, bufferedCalls, capturedCalls atomic.Int32
			buffered := activity49Parent(t, "StickyBuffered", "/sticky-buffered", "buffered", db, rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) {
				bufferedCalls.Add(1)
				return &bufferedCallOutput{}, nil
			}))
			ordinary := activity49Parent(t, "StickyOrdinary", "/sticky-ordinary", "", nil, rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) {
				ordinaryCalls.Add(1)
				return &bufferedCallOutput{}, nil
			}))
			captured := activity49Native(t, "StickyCaptured", "/sticky-captured", "", db)
			captured.Handler = &retained49NativeBusiness{Handler: captured.Handler.(*writer.Handler), calls: &capturedCalls}
			faultDB := db.FaultDB(t, func(ctx context.Context, c sqlfault.Call) error {
				if c.Phase == "prepare" && strings.HasPrefix(c.SQL, "INSERT INTO records") {
					once.Do(func() { _, enrollment = retained.InvokeComponent(ctx, activity49Request(buffered, 0)) })
				}
				return nil
			})
			buffered.DataSource = dml.Source{DB: faultDB}
			captured.DataSource = dml.Source{DB: faultDB}
			parent := activity49Parent(t, "StickyParent", "/sticky-parent", "", nil, rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				var e error
				retained, e = activity49Invoker(ctx, in.Binder)
				if e != nil {
					return nil, e
				}
				value, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !ok {
					return nil, e
				}
				if e = value.(xh.Data).Execute("INSERT INTO records VALUES(1,'ordinary-prefix')"); e != nil {
					return nil, e
				}
				flusher, ok, e := in.Binder.Lookup(ctx, xh.FlusherKey)
				if e != nil || !ok {
					return nil, e
				}
				drainError = flusher.(xh.Flusher).Flush(ctx, "")
				if enrollment == nil {
					return nil, errors.New("intended same-root callback enrollment was not rejected")
				}
				call := ctx
				if replaced {
					call = context.Background()
				}
				ordinaryValue, ordinaryError = retained.InvokeComponent(call, activity49Request(ordinary, 0))
				_, bufferedError = retained.InvokeComponent(call, activity49Request(buffered, 0))
				capturedContext, cancel := context.WithTimeout(call, time.Second)
				defer cancel()
				_, capturedError = retained.InvokeComponent(capturedContext, activity49Request(captured, 2))
				// All component failures are deliberately caught; root completion must
				// still retain the original overlap failure and truthful physical rollback.
				return &bufferedCallOutput{}, nil
			}))
			parent.DataSource = dml.Source{DB: faultDB}
			r := activity49Runtime(t, parent, ordinary, buffered, captured)
			result, rootError := r.ExecuteRoute(context.Background(), "POST", "/sticky-parent", nil)
			if !errors.Is(enrollment, drainowner.ErrDrainOverlap) || drainError != nil {
				t.Fatalf("intended ordinary callback stage enrollment=%v drain=%v", enrollment, drainError)
			}
			if !errors.Is(rootError, drainowner.ErrDrainOverlap) || result != nil {
				t.Errorf("DESIRED_STICKY_ROOT_GAP root=%v result=%T", rootError, result)
			}
			if ordinaryError == nil || ordinaryCalls.Load() != 0 || ordinaryValue != nil {
				t.Errorf("DESIRED_ORDINARY_RETAINED_GAP replaced=%t error=%v business=%d result=%T", replaced, ordinaryError, ordinaryCalls.Load(), ordinaryValue)
			}
			if bufferedError == nil || bufferedCalls.Load() != 0 {
				t.Errorf("DESIRED_BUFFERED_RETAINED_GAP replaced=%t error=%v business=%d", replaced, bufferedError, bufferedCalls.Load())
			}
			if capturedError == nil || capturedCalls.Load() != 0 {
				t.Errorf("DESIRED_CAPTURED_RETAINED_GAP replaced=%t error=%v business=%d", replaced, capturedError, capturedCalls.Load())
			}
			rows, e := activity49Rows(context.Background(), db.DB)
			if e != nil || len(rows) != 0 {
				t.Fatalf("protected root physical rollback rows=%v error=%v", rows, e)
			}
			t.Logf("REAL_CAUGHT_OVERLAP replaced=%t enrollment=%v ordinary=%v/%d buffered=%v/%d captured=%v/%d root=%v rows=%v", replaced, enrollment, ordinaryError, ordinaryCalls.Load(), bufferedError, bufferedCalls.Load(), capturedError, capturedCalls.Load(), rootError, rows)
		})
	}
}

// Ordinary caught errors retired before any protected enrollment remain local.
// Original and replaced contexts preserve existing ordinary transaction policy.
func TestNativeRetained49NeverEnrolledCaughtFailureControl(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(fmt.Sprintf("replaced=%t", replaced), func(t *testing.T) {
			db := activity49DB(t)
			sentinel := errors.New("ordinary caught business failure")
			var calls atomic.Int32
			bad := activity49Parent(t, "OrdinaryCaught", "/ordinary-caught", "", nil, rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return nil, sentinel }))
			next := activity49Parent(t, "OrdinaryNext", "/ordinary-next", "", db, rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				calls.Add(1)
				v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !ok {
					return nil, e
				}
				return &bufferedCallOutput{}, v.(xh.Data).Execute("INSERT INTO records VALUES(3,'ordinary-next')")
			}))
			parent := activity49Parent(t, "OrdinaryControl", "/ordinary-control", "", db, rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !ok {
					return nil, e
				}
				if e = v.(xh.Data).Execute("INSERT INTO records VALUES(1,'ordinary-parent')"); e != nil {
					return nil, e
				}
				invoker, e := activity49Invoker(ctx, in.Binder)
				if e != nil {
					return nil, e
				}
				_, e = invoker.InvokeComponent(ctx, activity49Request(bad, 0))
				if !errors.Is(e, sentinel) {
					return nil, fmt.Errorf("ordinary cause lost: %v", e)
				}
				call := ctx
				if replaced {
					call = context.Background()
				}
				return invoker.InvokeComponent(call, activity49Request(next, 0))
			}))
			r := activity49Runtime(t, parent, bad, next)
			result, e := r.ExecuteRoute(context.Background(), "POST", "/ordinary-control", nil)
			if e != nil || result == nil || calls.Load() != 1 {
				t.Fatalf("ordinary caught failure became retroactive result=%T error=%v business=%d", result, e, calls.Load())
			}
			rows, e := activity49Rows(context.Background(), db.DB)
			want := []int{1, 3}
			if replaced {
				want = []int{3, 1}
			}
			if e != nil || !reflect.DeepEqual(rows, want) {
				t.Fatalf("ordinary physical order=%v want=%v error=%v", rows, want, e)
			}
			// Both source-owned units have completed, so a new direct query confirms
			// real committed values; no inferred activity or ownership count is used.
			var n int
			if e = db.DB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM records").Scan(&n); e != nil || n != 2 {
				t.Fatalf("ordinary actual physical rows=%d error=%v", n, e)
			}
			t.Logf("REAL_NEVER_ENROLLED_CONTROL replaced=%t order=%v business=%d", replaced, rows, calls.Load())
		})
	}
}
