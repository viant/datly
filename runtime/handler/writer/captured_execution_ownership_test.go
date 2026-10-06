package writer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func capturedOwnershipFixture(t *testing.T, policy string) (*Handler, rhandler.Invocation, *sqldml.Data) {
	t.Helper()
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[replacementRow]().PkgPath(), Name: "Rows"}, Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: policy, Source: &spec.ViewSource{Table: "items"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
	h, err := New(c, reflect.TypeFor[replacementInput](), reflect.TypeFor[replacementOutput](), "patch")
	if err != nil {
		t.Fatal(err)
	}
	input := &replacementInput{Rows: []*replacementRow{{ID: ptr(1), Name: ptr("new"), Has: &replacementHas{ID: true, Name: true}}}}
	snapshot, err := h.CaptureInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	data := sqldml.NewData(db.DB)
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = data.Complete(context.Background(), errors.New("test cleanup")) })
	return h, rhandler.Invocation{Input: input, Snapshot: snapshot, Binder: &sqBinder{data: data}}, data
}
func registerCapturedOwnership(t *testing.T, h *Handler, in rhandler.Invocation, data *sqldml.Data) {
	t.Helper()
	check, err := h.CapturedExecutionGuard(in)
	if err != nil {
		t.Fatal(err)
	}
	if err = data.EnableCapturedExecutionGuards(); err != nil {
		t.Fatal(err)
	}
	if err = data.RegisterExecutionGuard(check); err != nil {
		t.Fatal(err)
	}
	if err = h.CapturedExecutionGuardRegistered(in); err != nil {
		t.Fatal(err)
	}
}
func TestCapturedExecutionRejectsForeignSnapshotsAndBindings(t *testing.T) {
	for _, mode := range []string{"foreign opted", "foreign policy free", "wrong input", "wrong binder", "nil binder", "finalized"} {
		t.Run(mode, func(t *testing.T) {
			h, in, data := capturedOwnershipFixture(t, "insert-delete")
			registerCapturedOwnership(t, h, in, data)
			switch mode {
			case "foreign opted", "foreign policy free":
				policy := "insert-delete"
				if mode == "foreign policy free" {
					policy = ""
				}
				foreign, other, otherData := capturedOwnershipFixture(t, policy)
				if policy != "" {
					registerCapturedOwnership(t, foreign, other, otherData)
				}
				in.Snapshot = other.Snapshot
			case "wrong input":
				in.Input = &replacementInput{}
			case "wrong binder":
				in.Binder = &sqBinder{data: data}
			case "nil binder":
				in.Binder = nil
			case "finalized":
				if err := h.FinalizeOutcome(context.Background(), in, nil, xhandler.Outcome{Error: errors.New("input initialization failed")}); err != nil {
					t.Fatal(err)
				}
			}
			observer := &capturedExecutionObserver{}
			ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 0, 0)
			_, err := h.Execute(ctx, in)
			if observer.calls.Load() != 0 {
				t.Fatal("invalid snapshot reached execution observation")
			}
			if err == nil || !strings.Contains(err.Error(), "captured writer") {
				t.Fatalf("%s admitted: %v", mode, err)
			}
			if err = data.Complete(context.Background(), err); err == nil {
				t.Fatal("invalid invocation committed")
			}
		})
	}
}
func TestCapturedExecutionGuardLifecycleIsOnceOnly(t *testing.T) {
	h, in, data := capturedOwnershipFixture(t, "insert-delete")
	if err := h.CapturedExecutionGuardRegistered(in); err == nil {
		t.Fatal("ack without issuance")
	}
	check, err := h.CapturedExecutionGuard(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.CapturedExecutionGuard(in); err == nil {
		t.Fatal("double issuance")
	}
	wrong := in
	wrong.Binder = &sqBinder{data: data}
	if err = h.CapturedExecutionGuardRegistered(wrong); err == nil {
		t.Fatal("wrong binding ack")
	}
	if err = data.EnableCapturedExecutionGuards(); err != nil {
		t.Fatal(err)
	}
	if err = data.RegisterExecutionGuard(check); err != nil {
		t.Fatal(err)
	}
	if err = h.CapturedExecutionGuardRegistered(in); err != nil {
		t.Fatal(err)
	}
	if err = h.CapturedExecutionGuardRegistered(in); err == nil {
		t.Fatal("double ack")
	}
	if _, err = h.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Execute(context.Background(), in); err == nil {
		t.Fatal("second execution")
	}
	if err = data.Complete(context.Background(), nil); err == nil {
		t.Fatal("caught duplicate execution admitted completion")
	}
	// A retry obtains a new capture and binder rather than resetting Program state.
	fresh, freshIn, freshData := capturedOwnershipFixture(t, "insert-delete")
	if _, err = fresh.Execute(context.Background(), freshIn); err != nil {
		t.Fatal(err)
	}
	if err = freshData.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
func TestCapturedExecutionConcurrentAdmission(t *testing.T) {
	h, in, data := capturedOwnershipFixture(t, "insert-delete")
	registerCapturedOwnership(t, h, in, data)
	p := in.Snapshot.(*Program)
	var winners atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if p.admitCapturedExecution(in) == nil {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("execution admissions=%d", winners.Load())
	}
}

type failingCapturedRegistrar struct {
	*sqldml.Data
	panicRegistration bool
}

func (s *failingCapturedRegistrar) RegisterExecutionGuard(func(context.Context) error) error {
	if s.panicRegistration {
		panic("registration panic")
	}
	return errors.New("registration failure")
}

type failingCapturedBinder struct{ service *failingCapturedRegistrar }

func (b *failingCapturedBinder) Bind(context.Context, any) error { return nil }
func (b *failingCapturedBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.DMLKey {
		return b.service, true, nil
	}
	return nil, false, nil
}
func TestCapturedExecutionRegistrationFailureCannotBeReused(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			h, in, data := capturedOwnershipFixture(t, "insert-delete")
			in.Binder = &failingCapturedBinder{service: &failingCapturedRegistrar{Data: data, panicRegistration: panics}}
			func() {
				defer func() {
					if recovered := recover(); panics && recovered == nil {
						t.Error("missing registration panic")
					}
				}()
				_, err := h.Execute(context.Background(), in)
				if !panics && err == nil {
					t.Error("registration succeeded")
				}
			}()
			if _, err := h.Execute(context.Background(), in); err == nil || !strings.Contains(err.Error(), "fresh capture") {
				t.Fatalf("failed capture reused: %v", err)
			}
			if _, err := h.CapturedExecutionGuard(in); err == nil {
				t.Fatal("failed capture reissued")
			}
		})
	}
}

type capturedExecutionObserver struct{ calls atomic.Int32 }

func (o *capturedExecutionObserver) ObservePhase(_ context.Context, event xhandler.PhaseEvent) {
	if event.Phase == xhandler.PhaseExecution {
		o.calls.Add(1)
	}
}
func TestCapturedExecutionFailedAttemptRequiresFreshCapture(t *testing.T) {
	h, in, data := capturedOwnershipFixture(t, "insert-delete")
	firstContext, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.Execute(firstContext, in)
	if err == nil {
		t.Fatal("invalid first execution succeeded")
	}
	observer := &capturedExecutionObserver{}
	ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 0, 0)
	if _, err = h.Execute(ctx, in); err == nil || !strings.Contains(err.Error(), "fresh capture") {
		t.Fatalf("failed execution reused: %v", err)
	}
	if observer.calls.Load() != 0 {
		t.Fatal("reused capture reached observer")
	}
	_ = data.Complete(context.Background(), errors.New("rollback failed attempt"))
}

func TestCapturedExecutionRealOutcomeFinalizationIsTerminal(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(map[bool]string{false: "before guard", true: "after guard before execution"}[registered], func(t *testing.T) {
			h, in, data := capturedOwnershipFixture(t, "insert-delete")
			if registered {
				registerCapturedOwnership(t, h, in, data)
			}
			cause := errors.New("input initialization failed")
			if err := h.FinalizeOutcome(context.Background(), in, nil, xhandler.Outcome{Error: cause}); err != nil {
				t.Fatal(err)
			}
			if err := h.FinalizeOutcome(context.Background(), in, nil, xhandler.Outcome{Error: cause}); err != nil {
				t.Fatal(err)
			}
			observer := &capturedExecutionObserver{}
			ctx, _ := rhandler.WithPhaseObserver(context.Background(), observer, 0, 0)
			if _, err := h.Execute(ctx, in); err == nil || !strings.Contains(err.Error(), "finalized") {
				t.Fatalf("finalized capture executed: %v", err)
			}
			if observer.calls.Load() != 0 {
				t.Fatal("finalized capture reached observation")
			}
			if _, err := h.CapturedExecutionGuard(in); err == nil {
				t.Fatal("finalized capture reissued")
			}
		})
	}
}

type directCaughtQueueHooks struct{}

func (*directCaughtQueueHooks) AfterQueue(_ context.Context, row *replacementRow, _ xhandler.LifecycleContext[replacementRow, xhandler.NoParent, replacementOutput]) error {
	switch *row.Name {
	case "panic":
		panic("direct after queue panic")
	case "cancel":
		return context.Canceled
	}
	return errors.New("direct after queue error")
}
func TestCapturedExecutionIgnoredBusinessFailureRollsBack(t *testing.T) {
	for _, policy := range []string{"insert-delete", ""} {
		for _, mode := range []string{"error", "panic", "cancel"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				h, in, _ := capturedOwnershipFixture(t, policy)
				h.metadata.Root.HookType = reflect.TypeFor[directCaughtQueueHooks]()
				input := in.Input.(*replacementInput)
				input.Rows[0].Name = ptr(mode)
				snapshot, err := h.CaptureInput(context.Background(), input)
				if err != nil {
					t.Fatal(err)
				}
				db := sqlite.New(t)
				if err = db.ExecStatements(context.Background(), "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT)", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON items BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
					t.Fatal(err)
				}
				data := sqldml.NewData(db.DB)
				if err = data.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				in.Snapshot = snapshot
				in.Binder = &sqBinder{data: data}
				failed := false
				func() {
					defer func() {
						if r := recover(); r != nil {
							if mode != "panic" || r != "direct after queue panic" {
								t.Fatalf("unexpected panic: %v", r)
							}
							failed = true
						}
					}()
					_, err = h.Execute(context.Background(), in)
					failed = err != nil
				}()
				if !failed {
					t.Fatal("business failure not reached")
				}
				completion := data.Complete(context.Background(), nil)
				var rows, audit int
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM items").Scan(&rows); err != nil {
					t.Fatal(err)
				}
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audit); err != nil {
					t.Fatal(err)
				}
				if policy != "" {
					if completion == nil || rows != 0 || audit != 0 {
						t.Fatalf("ignored protected failure committed: err=%v rows=%d audit=%d", completion, rows, audit)
					}
				} else if completion != nil || rows != 1 || audit != 1 {
					t.Fatalf("ordinary completion changed: err=%v rows=%d audit=%d", completion, rows, audit)
				}
			})
		}
	}
}
