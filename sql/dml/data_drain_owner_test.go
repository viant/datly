package dml

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

type promotedDrainOwner struct{ *Data }
type deeplyPromotedDrainOwner struct{ *promotedDrainOwner }

func TestNativeDrainOwnerRejectsBorrowedAndCopiedReceivers(t *testing.T) {
	native := NewData(nil)
	copied := reflect.New(reflect.TypeOf(native).Elem())
	copied.Elem().Set(reflect.ValueOf(native).Elem())
	transplanted := NewData(nil)
	transplanted.drainOwner = native.drainOwner
	borrowed := native.ComponentData(ComponentImperative, "").(*Data)
	// Test both a normal borrowed frame and a transplanted frame cell. Neither
	// may turn a public component view into the actual transaction owner.
	borrowedTransplant := native.ComponentData(ComponentBinding, "tail").(*Data)
	borrowedTransplant.drainOwner = native.drainOwner
	for _, tc := range []struct {
		name     string
		receiver any
	}{
		{"nil", nil}, {"typed nil", (*Data)(nil)}, {"zero Data", &Data{}},
		{"copied Data", copied.Interface()}, {"promoted owner", &promotedDrainOwner{native}},
		{"transplanted cell", transplanted}, {"borrowed frame", borrowed}, {"borrowed transplanted cell", borrowedTransplant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := drainowner.Claim(tc.receiver, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrOwner) {
				t.Fatalf("claim %v", err)
			}
		})
	}
	invocation := drainowner.NewInvocation()
	handle, err := drainowner.Claim(native, invocation)
	if err != nil {
		t.Fatal(err)
	}
	if !handle.Valid(native, invocation) {
		t.Fatal("exact owner lost")
	}
	for _, issuer := range []*drainowner.Invocation{invocation, drainowner.NewInvocation()} {
		if _, err := drainowner.Claim(native, issuer); !errors.Is(err, drainowner.ErrClaim) {
			t.Fatal("duplicate claim", err)
		}
	}
	for _, receiver := range []any{copied.Interface(), &promotedDrainOwner{native}, transplanted, borrowed, borrowedTransplant} {
		if handle.Valid(receiver, invocation) {
			t.Fatal("claim transferred to borrowed receiver")
		}
	}
	if handle.Valid(native, drainowner.NewInvocation()) || (drainowner.Handle{}).Valid(native, invocation) {
		t.Fatal("zero or foreign invocation authorized")
	}
}
func TestNativeDrainOwnerOneClaimAcrossConcurrentInvocations(t *testing.T) {
	native := NewData(nil)
	var wg sync.WaitGroup
	var claims atomic.Int32
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := drainowner.Claim(native, drainowner.NewInvocation()); err == nil {
				claims.Add(1)
			} else if !errors.Is(err, drainowner.ErrClaim) {
				t.Errorf("claim %v", err)
			}
		}()
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatalf("claims %d", claims.Load())
	}
}
func TestNativeDrainOwnerAttachmentAndRetirement(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary attachment", true: "claim before attachment"}[claimed], func(t *testing.T) {
			native := NewData(nil)
			invocation := drainowner.NewInvocation()
			var handle drainowner.Handle
			if claimed {
				var err error
				handle, err = drainowner.Claim(native, invocation)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := native.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if _, err := drainowner.Claim(native, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrClaim) {
				t.Fatalf("attached owner re-claimed %v", err)
			}
			if claimed && !handle.Valid(native, invocation) {
				t.Fatal("native attachment dropped prior claim")
			}
			cause := errors.New("abort before DB access")
			if err := native.Complete(context.Background(), cause); !errors.Is(err, cause) {
				t.Fatalf("complete %v", err)
			}
			if handle.Valid(native, invocation) {
				t.Fatal("retired claim remains valid")
			}
			if _, err := drainowner.Claim(native, invocation); !errors.Is(err, drainowner.ErrClaim) {
				t.Fatalf("retired claim %v", err)
			}
			out := native.TransactionOutcome()
			if out.State != xhandler.TransactionNone || out.Error != nil {
				t.Fatalf("no-transaction native outcome changed: %+v", out)
			}
		})
	}
}
func TestNativeDrainOwnerRejectsZeroInvocationWithoutConsumingClaim(t *testing.T) {
	native := NewData(nil)
	for _, invocation := range []*drainowner.Invocation{nil, {}} {
		if _, err := drainowner.Claim(native, invocation); !errors.Is(err, drainowner.ErrClaim) {
			t.Fatalf("zero invocation %v", err)
		}
	}
	if _, err := drainowner.Claim(native, drainowner.NewInvocation()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeDrainOwnerRootSealAndBorrowedSeal(t *testing.T) {
	native := NewData(nil)
	child := native.ComponentData(ComponentImperative, "").(*Data)
	nested := child.ComponentData(ComponentImperative, "").(*Data)
	child.SealComponent()
	if _, err := drainowner.Claim(nested, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrOwner) {
		t.Fatal(err)
	}
	invocation := drainowner.NewInvocation()
	handle, err := drainowner.Claim(native, invocation)
	if err != nil {
		t.Fatal("child seal closed owner claim", err)
	}
	native.SealComponent()
	if !handle.Valid(native, invocation) {
		t.Fatal("seal prematurely retired identity")
	}
	sealed := NewData(nil)
	sealed.SealComponent()
	if _, err := drainowner.Claim(sealed, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrClaim) {
		t.Fatal("sealed root remains claimable", err)
	}
	closed := NewData(nil)
	if err := closed.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := closed.CloseMutationAdmission(); err != nil {
		t.Fatal(err)
	}
	rejected := closed.ComponentData(ComponentImperative, "")
	if _, err := drainowner.Claim(rejected, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrOwner) {
		t.Fatal("rejected borrower claim", err)
	}
}
func TestNativeDrainOwnerClaimLifecycleRaces(t *testing.T) {
	for _, complete := range []bool{false, true} {
		for range 64 {
			native := NewData(nil)
			invocation := drainowner.NewInvocation()
			start := make(chan struct{})
			var wg sync.WaitGroup
			var handle drainowner.Handle
			var claimErr, lifecycleErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; handle, claimErr = drainowner.Claim(native, invocation) }()
			go func() {
				defer wg.Done()
				<-start
				if complete {
					lifecycleErr = native.Complete(context.Background(), nil)
				} else {
					lifecycleErr = native.BeginInvocation()
				}
			}()
			close(start)
			wg.Wait()
			if lifecycleErr != nil {
				t.Fatal(lifecycleErr)
			}
			if claimErr != nil && !errors.Is(claimErr, drainowner.ErrClaim) {
				t.Fatal(claimErr)
			}
			if _, err := drainowner.Claim(native, invocation); !errors.Is(err, drainowner.ErrClaim) {
				t.Fatal("late race claim", err)
			}
			if complete && handle.Valid(native, invocation) {
				t.Fatal("race retirement lost")
			}
			if !complete && claimErr == nil && !handle.Valid(native, invocation) {
				t.Fatal("winning identity claim lost")
			}
		}
	}
}
func TestNativeDrainOwnerRetirementPreservesPhysicalOutcomes(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "flush failure", "commit unknown", "rollback unknown", "callback panic", "caller pending"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
				t.Fatal(err)
			}
			var opts []Option
			if mode == "callback panic" {
				opts = append(opts, WithCommitObserver(func(context.Context) { panic("native observer panic") }))
			}
			var callerRollback func() error
			if mode == "caller pending" {
				tx, err := h.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				opts = append(opts, WithTx(tx))
				callerRollback = tx.Rollback
				defer tx.Rollback()
			}
			native := NewData(h.DB, opts...)
			invocation := drainowner.NewInvocation()
			handle, err := drainowner.Claim(native, invocation)
			if err != nil {
				t.Fatal(err)
			}
			copyHandle := handle
			if err = native.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			var cause error
			want := xhandler.TransactionCommitted
			rows := 1
			switch mode {
			case "flush failure":
				if err = native.Execute("INSERT INTO absent_table VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				want = xhandler.TransactionRolledBack
				rows = 0
			case "commit unknown":
				if err = native.Execute("INSERT INTO child VALUES(99)"); err != nil {
					t.Fatal(err)
				}
				want = xhandler.TransactionCommitUnknown
				rows = 0
			default:
				if err = native.Execute("INSERT INTO parent VALUES(1)"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "rollback" || mode == "rollback unknown" {
				if err = native.PrepareCompletion(ctx); err != nil {
					t.Fatal(err)
				}
				cause = errors.New("native failure")
				want = xhandler.TransactionRolledBack
				rows = 0
				if mode == "rollback unknown" {
					if err = native.tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					want = xhandler.TransactionRollbackUnknown
				}
			}
			if mode == "caller pending" {
				want = xhandler.TransactionCallerPending
				rows = 0
			}
			err = native.Complete(ctx, cause)
			failure := mode != "commit" && mode != "caller pending"
			if failure != (err != nil) {
				t.Fatalf("completion %v", err)
			}
			if handle.Valid(native, invocation) || copyHandle.Valid(native, invocation) {
				t.Fatal("retired original/copied handle")
			}
			first := native.TransactionOutcome()
			if first.State != want {
				t.Fatalf("actual outcome %+v want %s", first, want)
			}
			if !errors.Is(native.Complete(ctx, nil), ErrInvocationCompleted) {
				t.Fatal("repeated complete")
			}
			second := native.TransactionOutcome()
			if first.State != second.State || first.Error != second.Error {
				t.Fatal("completed outcome rewritten")
			}
			if callerRollback != nil {
				if _, err = native.tx.ExecContext(ctx, "INSERT INTO parent VALUES(2)"); err != nil {
					t.Fatal("caller tx closed", err)
				}
				if err = callerRollback(); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM parent").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != rows {
				t.Fatalf("physical rows %d want %d", count, rows)
			}
		})
	}
}

func TestNativeDrainOwnerNilPromotedChains(t *testing.T) {
	for _, receiver := range []any{&promotedDrainOwner{}, &deeplyPromotedDrainOwner{}, &deeplyPromotedDrainOwner{&promotedDrainOwner{}}} {
		if _, err := drainowner.Claim(receiver, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrOwner) {
			t.Fatalf("unsupported nil owner %v", err)
		}
		if (drainowner.Handle{}).Valid(receiver, drainowner.NewInvocation()) {
			t.Fatal("nil chain became owner")
		}
		if err := (drainowner.Handle{}).Attach(receiver, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrOwner) {
			t.Fatal("nil chain attachment", err)
		}
		if (drainowner.Handle{}).Attached(receiver, drainowner.NewInvocation()) {
			t.Fatal("nil chain readiness")
		}
		drainowner.CloseClaims(receiver)
		drainowner.Seal(receiver)
		drainowner.Retire(receiver)
	}
	native := NewData(nil)
	if _, err := drainowner.Claim(native, drainowner.NewInvocation()); err != nil {
		t.Fatal(err)
	}
}
