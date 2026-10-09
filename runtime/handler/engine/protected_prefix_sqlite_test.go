//go:build sqlite_trace

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	sqlite3 "github.com/mattn/go-sqlite3"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

func TestProtectedPrefixPhysicalOrdering(t *testing.T) {
	for _, lateFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion", true: "late product failure"}[lateFailure], func(t *testing.T) {
			trace := &orderedTrace{}
			fail := ""
			if lateFailure {
				fail = "late"
			}
			a, b := orderedDB(t, trace, "A", fail, false), orderedDB(t, trace, "B", "", false)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			rootActivity, err := root.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err = root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			main := orderedData(t, root)
			orderedInsert(t, main, 1, "product")
			orderedInsert(t, main, 2, "late")
			prior := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			priorActivity, err := prior.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			orderedInsert(t, orderedData(t, prior), 1, "prior")
			prior.seal()
			if err = prior.finishActivity(priorActivity, nil); err != nil {
				t.Fatal(err)
			}
			child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			activity, err := child.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data := orderedData(t, child)
			orderedInsert(t, data, 2, "selected")
			flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, activity, []string{"records", "events"})}
			trace.active = true
			if err = flusher.Flush(t.Context(), "records"); err != nil {
				t.Fatal(err)
			}
			if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"B:prior", "B:selected"}) {
				t.Fatalf("prefix=%v", got)
			}
			// Repeated/no-match calls are true no-ops, not a drain of other owners.
			if err = flusher.Flush(t.Context(), "records"); err != nil {
				t.Fatal(err)
			}
			if err = flusher.Flush(t.Context(), "events"); err != nil {
				t.Fatal(err)
			}
			if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"B:prior", "B:selected"}) {
				t.Fatalf("repeated prefix=%v", got)
			}
			orderedInsert(t, data, 3, "suffix")
			child.seal()
			if err = child.finishActivity(activity, nil); err != nil {
				t.Fatal(err)
			}
			if err = root.finishActivity(rootActivity, nil); err != nil {
				t.Fatal(err)
			}
			err = root.complete(t.Context(), nil)
			if lateFailure {
				if err == nil {
					t.Fatal("late SQL failure lost")
				}
				if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
					t.Fatal("root rollback lost ownership")
				}
				if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"B:prior", "B:selected", "A:product", "A:late"}) {
					t.Fatalf("late order=%v", got)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if orderedCount(t, a, "records") != 2 || orderedCount(t, b, "records") != 3 || orderedCount(t, b, "effects") != 3 {
					t.Fatal("selected writes replayed or disappeared")
				}
				if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"B:prior", "B:selected", "A:product", "A:late", "B:suffix"}) {
					t.Fatalf("completion order=%v", got)
				}
			}
		})
	}
}

func TestProtectedPrefixPartialFailureRetainsRootRollback(t *testing.T) {
	trace := &orderedTrace{}
	a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
	if _, err := b.Exec("CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'prefix event failure');END"); err != nil {
		t.Fatal(err)
	}
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
	rootActivity, err := root.admitActivity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	orderedInsert(t, orderedData(t, root), 1, "product")
	child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
	activity, err := child.admitActivity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data := orderedData(t, child)
	orderedInsert(t, data, 1, "prefix")
	if err = data.Execute("INSERT INTO events VALUES(1,'fail')"); err != nil {
		t.Fatal(err)
	}
	orderedInsert(t, data, 2, "suffix")
	flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, activity, []string{"records"})}
	trace.active = true
	if err = flusher.Flush(t.Context(), "records"); err == nil {
		t.Fatal("partial failure lost")
	}
	child.seal()
	_ = child.finishActivity(activity, nil)
	_ = root.finishActivity(rootActivity, nil)
	if err = root.complete(t.Context(), nil); err == nil {
		t.Fatal("caught partial failure committed")
	}
	if got := physicalLabels(trace.copy()); !reflect.DeepEqual(got, []string{"B:prefix"}) {
		t.Fatalf("unexpected suffix effects: %v", got)
	}
	if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
		t.Fatal("partial prefix escaped rollback")
	}
}

func TestProtectedPrefixCallbackReentryAndCancellation(t *testing.T) {
	for _, mode := range []string{"cross owner append", "same owner flush", "transaction start", "validate guards", "framework validate", "panic", "cancel", "close activities"} {
		t.Run(mode, func(t *testing.T) {
			trace := &orderedTrace{}
			a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			rootActivity, err := root.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err = root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			main := orderedData(t, root)
			child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			activity, err := child.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data := orderedData(t, child)
			orderedInsert(t, data, 1, "selected")
			flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, activity, []string{"records"})}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			guarded := data.(interface {
				RegisterExecutionGuard(func(context.Context) error) error
			})
			if err = guarded.RegisterExecutionGuard(func(call context.Context) error {
				switch mode {
				case "cross owner append":
					_ = main.Insert("records", &orderedRow{ID: 9, Label: "forbidden"})
				case "same owner flush":
					_ = flusher.Flush(call, "records")
				case "transaction start":
					_ = main.(xh.TransactionStarter).Start(call)
				case "validate guards":
					_ = data.(interface{ ValidateExecutionGuards(context.Context) error }).ValidateExecutionGuards(call)
				case "framework validate":
					_, _ = data.(*dml.Data).FrameworkValidator().Validate(call, &orderedRow{ID: 9})
				case "panic":
					panic("prefix callback panic")
				case "cancel":
					cancel()
				case "close activities":
					_ = drainowner.CloseActivities(root.nativeInvocation)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = flusher.Flush(ctx, "records"); err == nil {
				t.Fatal("callback failure swallowed")
			}
			child.seal()
			_ = child.finishActivity(activity, nil)
			_ = root.finishActivity(rootActivity, nil)
			if err = root.complete(t.Context(), nil); err == nil {
				t.Fatal("caught callback failure committed")
			}
			if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
				t.Fatal("callback rejection wrote data")
			}
		})
	}
}

func TestProtectedPrefixRejectedAuthorityIsTerminal(t *testing.T) {
	for _, mode := range []string{"wrong table", "empty table", "stale activity", "sealed frame", "sibling activity", "binding ancestor", "unconfigured"} {
		t.Run(mode, func(t *testing.T) {
			trace := &orderedTrace{}
			a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			rootActivity, err := root.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err = root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			orderedInsert(t, orderedData(t, root), 1, "product")
			relation := ComponentBufferedImperative
			if mode == "binding ancestor" {
				relation = ComponentBinding
			}
			child := orderedChild(t, root, dml.Source{DB: b}, relation, "")
			activity, err := child.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data := orderedData(t, child)
			orderedInsert(t, data, 1, "selected")
			flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, activity, []string{"records"})}
			table := "records"
			var retireSibling func()
			switch mode {
			case "wrong table":
				table = "events"
			case "empty table":
				table = ""
			case "stale activity":
				if err = child.finishActivity(activity, nil); err != nil {
					t.Fatal(err)
				}
			case "sealed frame":
				child.seal()
			case "sibling activity":
				sibling := orderedChild(t, root, dml.Source{DB: a}, ComponentBufferedImperative, "")
				siblingActivity, e := sibling.admitActivity(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				retireSibling = func() { sibling.seal(); _ = sibling.finishActivity(siblingActivity, nil) }
			case "unconfigured":
				flusher.authority = nil
			}
			err = flusher.Flush(t.Context(), table)
			if retireSibling != nil {
				retireSibling()
			}
			if err == nil {
				t.Fatal("unauthorized prefix allowed")
			}
			child.seal()
			if mode != "stale activity" {
				_ = child.finishActivity(activity, nil)
			}
			_ = root.finishActivity(rootActivity, nil)
			if err = root.complete(t.Context(), nil); err == nil {
				t.Fatal("swallowed denial did not reject completion")
			}
			if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
				t.Fatal("denied prefix wrote data")
			}
			if mode == "unconfigured" && !errors.Is(err, drainowner.ErrDrain) {
				t.Fatal(err)
			}
		})
	}
}

func TestProtectedPrefixCompletionJoinsNativeUnwind(t *testing.T) {
	for _, mode := range []string{"closure", "panic", "cancelled completion"} {
		t.Run(mode, func(t *testing.T) {
			trace := &orderedTrace{}
			a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
			root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
			ra, err := root.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err = root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			orderedInsert(t, orderedData(t, root), 1, "product")
			child := orderedChild(t, root, dml.Source{DB: b}, ComponentBufferedImperative, "")
			ca, err := child.admitActivity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			data := orderedData(t, child)
			flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, ca, []string{"records"})}
			orderedInsert(t, data, 1, "prefix")
			if err = flusher.Flush(t.Context(), "records"); err != nil {
				t.Fatal(err)
			}
			orderedInsert(t, data, 2, "suffix")
			entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			if err = data.(interface {
				RegisterExecutionGuard(func(context.Context) error) error
			}).RegisterExecutionGuard(func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				close(cancelled)
				<-release
				if mode == "panic" {
					panic("closure callback")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			flushResult := make(chan error, 1)
			go func() { flushResult <- flusher.Flush(t.Context(), "records") }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("native prefix did not enter")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancelled completion" {
				cancel()
			}
			completeResult := make(chan error, 1)
			go func() { completeResult <- root.complete(ctx, nil) }()
			select {
			case <-cancelled:
			case <-time.After(5 * time.Second):
				t.Fatal("root did not cancel native SQL context")
			}
			select {
			case err := <-completeResult:
				t.Fatalf("completion skipped native unwind: %v", err)
			default:
			}
			if _, err = root.admitActivity(t.Context()); err == nil {
				t.Fatal("completion left activity admission open")
			}
			close(release)
			select {
			case err := <-flushResult:
				if err == nil {
					t.Fatal("closed prefix succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("prefix did not unwind")
			}
			select {
			case err := <-completeResult:
				if err == nil {
					t.Fatal("unfinished handler committed")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("completion did not join and abort")
			}
			_ = child.finishActivity(ca, nil)
			_ = root.finishActivity(ra, nil)
			if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
				t.Fatal("closure did not rollback the prior prefix")
			}
			if err := drainowner.JoinPrefixCompletion(root.nativeInvocation); err == nil {
				t.Fatal("native join discarded sticky failure")
			}
		})
	}
}

func TestProtectedPrefixSingleOwnerAccounting(t *testing.T) {
	trace := &orderedTrace{}
	db := orderedDB(t, trace, "A", "", false)
	root, _ := invocationDataScope(t.Context(), dml.Source{DB: db})
	activity, err := root.admitActivity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	data := orderedData(t, root)
	orderedInsert(t, data, 1, "prefix")
	flusher := flusherCapability{service: data, guard: root.mutationGuard(), authority: newFlushAuthority(root, activity, []string{"records"})}
	shortContext, cancel := context.WithCancel(t.Context())
	if err = flusher.Flush(shortContext, "records"); err != nil {
		t.Fatal(err)
	}
	cancel() // caller lifetime must not own the transaction
	if !root.nativeInvocation.HasPrefixExecution() {
		t.Fatal("native receipt missing")
	}
	orderedInsert(t, data, 2, "suffix")
	if err = root.finishActivity(activity, nil); err != nil {
		t.Fatal(err)
	}
	if err = root.complete(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if orderedCount(t, db, "records") != 2 || orderedCount(t, db, "effects") != 2 {
		t.Fatal("single owner prefix replayed or omitted")
	}
}

func TestProtectedPrefixRejectsNativeBeginCallbackReentry(t *testing.T) {
	for _, operation := range []string{"allocate", "reserve", "exec", "query", "query row"} {
		for _, sameOwner := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sameOwner=%t", operation, sameOwner), func(t *testing.T) {
				var onBegin func()
				var once sync.Once
				driverName := fmt.Sprintf("sqlite_prefix_reentry_%d", orderedDBSerial.Add(1))
				sql.Register(driverName, &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
					return conn.SetTrace(&sqlite3.TraceConfig{EventMask: sqlite3.TraceStmt, Callback: func(info sqlite3.TraceInfo) int {
						if strings.EqualFold(strings.TrimSpace(info.StmtOrTrigger), "BEGIN") && onBegin != nil {
							once.Do(onBegin)
						}
						return 0
					}})
				}})
				db, err := sql.Open(driverName, filepath.Join(t.TempDir(), "native.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err = db.Exec("CREATE TABLE records(ID INTEGER PRIMARY KEY AUTOINCREMENT,LABEL TEXT)"); err != nil {
					t.Fatal(err)
				}
				root, _ := invocationDataScope(t.Context(), dml.Source{DB: db})
				ra, err := root.admitActivity(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if err = root.enrollBufferedScope(); err != nil {
					t.Fatal(err)
				}
				native := orderedData(t, root)
				targetDB := db
				if !sameOwner {
					targetDB = orderedDB(t, &orderedTrace{}, "B", "", false)
				}
				child := orderedChild(t, root, dml.Source{DB: targetDB}, ComponentBufferedImperative, "")
				ca, err := child.admitActivity(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				data := orderedData(t, child)
				orderedInsert(t, data, 7, "queued")
				flusher := flusherCapability{service: data, guard: child.mutationGuard(), authority: newFlushAuthority(child, ca, []string{"records"})}
				callbackResult := make(chan error, 1)
				onBegin = func() { callbackResult <- flusher.Flush(t.Context(), "records") }
				finished := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "allocate":
						err = native.Allocate(t.Context(), "records", []*orderedRow{{Label: "allocate"}}, "ID")
					case "reserve":
						err = native.(interface {
							Reserve(context.Context, string, any, string) error
						}).Reserve(t.Context(), "records", []*orderedRow{{ID: 4, Label: "reserve"}}, "ID")
					case "exec":
						_, err = native.(*dml.Data).ExecContext(t.Context(), "INSERT INTO records VALUES(1,'direct')")
					case "query":
						var rows *sql.Rows
						rows, err = native.(*dml.Data).QueryContext(t.Context(), "SELECT ID FROM records")
						if rows != nil {
							rows.Close()
						}
					case "query row":
						var id int
						err = native.(*dml.Data).QueryRowContext(t.Context(), "SELECT ID FROM records").Scan(&id)
					}
					finished <- err
				}()
				select {
				case err := <-callbackResult:
					if !errors.Is(err, drainowner.ErrPrefix) {
						t.Fatalf("callback admission=%v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("authorized flush deadlocked in native callback")
				}
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("native operation did not unwind")
				}
				child.seal()
				_ = child.finishActivity(ca, nil)
				_ = root.finishActivity(ra, nil)
				if err = root.complete(t.Context(), nil); err == nil {
					t.Fatal("caught callback denial committed")
				}
				if orderedCount(t, db, "records") != 0 || orderedCount(t, targetDB, "records") != 0 {
					t.Fatal("reverse reentry wrote or committed")
				}
			})
		}
	}
}

func TestProtectedPrefixCannotReplaceCancelledRootContext(t *testing.T) {
	for _, mode := range []string{"already cancelled no match", "mid prefix"} {
		t.Run(mode, func(t *testing.T) {
			lifetime, cancel := context.WithCancel(t.Context())
			defer cancel()
			db := orderedDB(t, &orderedTrace{}, "A", "", false)
			root, _ := invocationDataScope(lifetime, dml.Source{DB: db})
			activity, err := root.admitActivity(lifetime)
			if err != nil {
				t.Fatal(err)
			}
			if err = root.enrollBufferedScope(); err != nil {
				t.Fatal(err)
			}
			data := orderedData(t, root)
			flusher := flusherCapability{service: data, guard: root.mutationGuard(), authority: newFlushAuthority(root, activity, []string{"records"})}
			if mode == "already cancelled no match" {
				cancel()
			} else {
				orderedInsert(t, data, 1, "prefix")
				if err = data.(interface {
					RegisterExecutionGuard(func(context.Context) error) error
				}).RegisterExecutionGuard(func(ctx context.Context) error {
					cancel()
					select {
					case <-ctx.Done():
					case <-time.After(5 * time.Second):
						return errors.New("root cancellation did not reach SQL context")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err = flusher.Flush(context.Background(), "records"); !errors.Is(err, context.Canceled) {
				t.Fatalf("replacement context bypassed root cancellation: %v", err)
			}
			_ = root.finishActivity(activity, nil)
			if err = root.complete(context.Background(), nil); err == nil {
				t.Fatal("cancelled prefix committed")
			}
			if orderedCount(t, db, "records") != 0 {
				t.Fatal("cancelled root produced SQL effects")
			}
		})
	}
}

func TestProtectedPrefixRejectsMixedOwnershipWithoutBufferedPolicy(t *testing.T) {
	for _, kind := range []string{"external", "ordinary drain"} {
		for _, late := range []bool{false, true} {
			if kind == "ordinary drain" && late {
				continue
			} // covered at constructor association in drainowner tests
			t.Run(fmt.Sprintf("%s/late=%t", kind, late), func(t *testing.T) {
				trace := &orderedTrace{}
				a, b := orderedDB(t, trace, "A", "", false), orderedDB(t, trace, "B", "", false)
				root, _ := invocationDataScope(t.Context(), dml.Source{DB: a})
				activity, err := root.admitActivity(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				data := orderedData(t, root)
				orderedInsert(t, data, 1, "prefix")
				var tx *sql.Tx
				source := dml.Source{DB: b}
				if kind == "external" {
					tx, err = b.BeginTx(t.Context(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					source.Tx = tx
				}
				var bad *dataScope
				attach := func() error {
					bad = orderedChild(t, root, source, ComponentImperative, "")
					other, e := bad.resolve(t.Context())
					if e != nil {
						return e
					}
					if kind == "ordinary drain" {
						// Attach a genuinely already-drained native owner without rewriting its
						// historical evidence. The drain occurs before protected enrollment.
						orderedInsert(t, other, 7, "ordinary")
						e = other.Flush(t.Context(), "records")
					}
					bad.seal()
					return e
				}
				if !late {
					if err = attach(); err != nil {
						t.Fatal(err)
					}
				}
				if err = drainowner.EnrollActivities(root.nativeInvocation); err != nil {
					t.Fatal(err)
				}
				flusher := flusherCapability{service: data, guard: root.mutationGuard(), authority: newFlushAuthority(root, activity, []string{"records"})}
				flushErr := flusher.Flush(t.Context(), "records")
				if late {
					if flushErr != nil {
						t.Fatal(flushErr)
					}
					if err = attach(); err == nil {
						t.Fatal("external owner accepted after prefix")
					}
				} else if flushErr == nil {
					t.Fatal("mixed ownership entered prefix effects")
				}
				_ = root.finishActivity(activity, nil)
				if err = root.complete(t.Context(), nil); err == nil {
					t.Fatal("mixed ownership committed")
				}
				if orderedCount(t, a, "records") != 0 || orderedCount(t, b, "records") != 0 {
					t.Fatal("ownership denial failed rollback")
				}
			})
		}
	}
}
