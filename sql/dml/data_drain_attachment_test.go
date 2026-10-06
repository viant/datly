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

func TestNativeDrainAttachmentBindsActualInvocation(t *testing.T) {
	native := NewData(nil)
	issuer := drainowner.NewInvocation()
	handle, err := drainowner.Claim(native, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if handle.Attached(native, issuer) {
		t.Fatal("identity-only claim became attached")
	}
	copyData := reflect.New(reflect.TypeOf(native).Elem())
	copyData.Elem().Set(reflect.ValueOf(native).Elem())
	for _, receiver := range []any{copyData.Interface(), &promotedDrainOwner{native}, native.ComponentData(ComponentImperative, "")} {
		if !errors.Is(handle.Attach(receiver, issuer), drainowner.ErrOwner) {
			t.Fatal("borrowed attachment accepted")
		}
	}
	if err = handle.Attach(native, drainowner.NewInvocation()); !errors.Is(err, drainowner.ErrAttachment) {
		t.Fatal("foreign issuer", err)
	}
	if err = handle.Attach(native, issuer); err != nil {
		t.Fatal(err)
	}
	native.mu.Lock()
	attached := native.invocation
	native.mu.Unlock()
	if !attached || !handle.Attached(native, issuer) {
		t.Fatal("actual native handoff missing")
	}
	copiedHandle := handle
	if !errors.Is(copiedHandle.Attach(native, issuer), drainowner.ErrAttachment) {
		t.Fatal("copied handle reattached")
	}
	if native.BeginInvocation() == nil {
		t.Fatal("public begin repeated actual attachment")
	}
	if err = native.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if handle.Attached(native, issuer) || handle.Valid(native, issuer) {
		t.Fatal("completed owner still attached")
	}
}
func TestNativeDrainAttachmentRejectsPublicAndClosedLifetime(t *testing.T) {
	for _, mode := range []string{"public begin", "sealed root", "completed root", "closed admission"} {
		t.Run(mode, func(t *testing.T) {
			native := NewData(nil)
			issuer := drainowner.NewInvocation()
			handle, err := drainowner.Claim(native, issuer)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "public begin":
				if err = native.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
			case "sealed root":
				native.SealComponent()
			case "completed root":
				if err = native.Complete(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
			case "closed admission":
				if err = native.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				if err = native.CloseMutationAdmission(); err != nil {
					t.Fatal(err)
				}
			}
			if err = handle.Attach(native, issuer); err == nil {
				t.Fatal("identity claim promoted through closed lifetime")
			}
			if handle.Attached(native, issuer) {
				t.Fatal("public/closed lifetime acknowledged issuer")
			}
			if err = handle.Attach(native, issuer); err == nil {
				t.Fatal("failed attempt retried")
			}
		})
	}
	native := NewData(nil)
	if err := native.beginInvocation(&drainowner.Permit{}); !errors.Is(err, drainowner.ErrAttachment) {
		t.Fatal("zero permit", err)
	}
	if native.invocation {
		t.Fatal("zero permit published native invocation")
	}
	if err := native.BeginInvocation(); err != nil {
		t.Fatal("invalid permit changed ordinary begin", err)
	}
}
func TestNativeDrainAttachmentCopiedHandlesHaveOneNativeWinner(t *testing.T) {
	native := NewData(nil)
	issuer := drainowner.NewInvocation()
	handle, err := drainowner.Claim(native, issuer)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var wins atomic.Int32
	for range 32 {
		copied := handle
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := copied.Attach(native, issuer)
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, drainowner.ErrAttachment) {
				t.Errorf("attach %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || !handle.Attached(native, issuer) {
		t.Fatalf("native winners %d", wins.Load())
	}
}
func TestNativeDrainAttachmentRacesPublicBeginAndCompletion(t *testing.T) {
	for _, complete := range []bool{false, true} {
		for range 64 {
			native := NewData(nil)
			issuer := drainowner.NewInvocation()
			handle, err := drainowner.Claim(native, issuer)
			if err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var attachErr, publicErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; attachErr = handle.Attach(native, issuer) }()
			go func() {
				defer wg.Done()
				<-start
				if complete {
					publicErr = native.Complete(context.Background(), nil)
				} else {
					publicErr = native.BeginInvocation()
				}
			}()
			close(start)
			wg.Wait()
			if complete {
				if errors.Is(publicErr, drainowner.ErrAttachment) {
					if attachErr != nil || native.completed || !handle.Attached(native, issuer) {
						t.Fatalf("rejected completion changed attachment: attach=%v completed=%t", attachErr, native.completed)
					}
					cause := errors.New("rightful attachment cleanup")
					if err := handle.Call(context.Background(), native, issuer, drainowner.Abort, cause); !errors.Is(err, cause) {
						t.Fatal(err)
					}
				} else if publicErr != nil {
					t.Fatal(publicErr)
				}
				if handle.Attached(native, issuer) || handle.Valid(native, issuer) {
					t.Fatal("completed cleanup left live attachment")
				}

			} else {
				if (attachErr == nil) == (publicErr == nil) {
					t.Fatalf("actual attachment winners: private%v public%v", attachErr, publicErr)
				}
				if handle.Attached(native, issuer) != (attachErr == nil) {
					t.Fatal("public begin falsely acknowledged private issuer")
				}
			}
		}
	}
}
func TestNativeDrainAttachmentPreservesCallerTransaction(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); err != nil {
		t.Fatal(err)
	}
	native := NewData(h.DB, WithTx(tx))
	issuer := drainowner.NewInvocation()
	handle, err := drainowner.Claim(native, issuer)
	if err != nil {
		t.Fatal(err)
	}
	if err = handle.Attach(native, issuer); err != nil {
		t.Fatal(err)
	}
	if err = native.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = native.PrepareFinalization(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("caller journal drained before finalization %d %v", count, err)
	}
	if err = native.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if native.TransactionOutcome().State != xhandler.TransactionCallerPending || handle.Attached(native, issuer) {
		t.Fatal("caller outcome/lifetime changed")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(100)"); err != nil {
		t.Fatal("caller transaction closed", err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 3 {
		t.Fatalf("caller journal %d %v", count, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 0 {
		t.Fatalf("caller rollback %d %v", count, err)
	}
}
