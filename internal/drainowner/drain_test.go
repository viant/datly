package drainowner

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// These exercise internal admission primitives, not native SQL serialization.
func TestDrainEnrollmentBothDirections(t *testing.T) {
	for _, first := range []string{"drain", "enrollment"} {
		t.Run(first, func(t *testing.T) {
			o := &attachmentTestOwner{}
			o.State = NewState(o, nil)
			i := NewInvocation()
			if _, err := Claim(o, i); err != nil {
				t.Fatal(err)
			}
			if first == "enrollment" {
				if err := EnrollActivities(i); err != nil {
					t.Fatal(err)
				}
				if _, err := BeginPublicDrain(o); !errors.Is(err, ErrDrain) {
					t.Fatalf("protected drain: %v", err)
				}
				if err := CheckActivities(i); !errors.Is(err, ErrDrain) {
					t.Fatalf("missing sticky rejection: %v", err)
				}
				return
			}
			r, err := BeginPublicDrain(o)
			if err != nil {
				t.Fatal(err)
			}
			copy := *r
			EndDrain(&copy)
			if err := EnrollActivities(i); !errors.Is(err, ErrDrainOverlap) {
				t.Fatalf("active ordinary drain: %v", err)
			}
			EndDrain(r)
			if err := EnrollActivities(i); !errors.Is(err, ErrDrainOverlap) {
				t.Fatalf("repeated enrollment lost failure: %v", err)
			}
			if err := CheckActivities(i); !errors.Is(err, ErrDrainOverlap) {
				t.Fatalf("failure lost after retirement: %v", err)
			}
		})
	}
}

func TestPreclaimDrainCannotBeAttached(t *testing.T) {
	o := &attachmentTestOwner{}
	o.State = NewState(o, nil)
	r, err := BeginPublicDrain(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Claim(o, NewInvocation()); !errors.Is(err, ErrClaim) {
		t.Fatalf("claim during drain: %v", err)
	}
	EndDrain(r)
	if _, err = Claim(o, NewInvocation()); err != nil {
		t.Fatal(err)
	}
}

func TestConstructorDrainPermitExactOperationAndOnce(t *testing.T) {
	o := &attachmentTestOwner{}
	i := NewInvocation()
	var retained *DrainPermit
	calls := 0
	ops := NativeOperations{Completion: func(_ context.Context, p *DrainPermit, cause error) error {
		calls++
		retained = p
		copy := *p
		if _, err := ConsumeDrain(o, &copy, Completion, nil); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("copied permit: %v", err)
		}
		if _, err := ConsumeDrain(o, p, AllPreparation, nil); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("wrong operation: %v", err)
		}
		r, err := ConsumeDrain(o, p, Completion, cause)
		if err != nil {
			return err
		}
		defer EndDrain(r)
		if _, err := ConsumeDrain(o, p, Completion, cause); !errors.Is(err, ErrDrainPermit) {
			t.Fatalf("replay: %v", err)
		}
		return nil
	}}
	o.State = NewState(o, func(p *Permit) error { return BindAttachment(o, p) }, ops)
	// Mutating the caller's original map cannot replace the constructor operation.
	ops[Completion] = func(context.Context, *DrainPermit, error) error { t.Fatal("operation map alias"); return nil }
	h, err := Claim(o, i)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Attach(o, i); err != nil {
		t.Fatal(err)
	}
	if err = EnrollActivities(i); err != nil {
		t.Fatal(err)
	}
	if err = CloseActivities(i); err != nil {
		t.Fatal(err)
	}
	if err = h.Call(context.Background(), o, i, Completion, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	if _, err = ConsumeDrain(o, retained, Completion, nil); !errors.Is(err, ErrDrainPermit) {
		t.Fatalf("retained permit: %v", err)
	}
}

func TestDrainAttachmentBothWinners(t *testing.T) {
	t.Run("attachment-first", func(t *testing.T) {
		o := &attachmentTestOwner{}
		entered, release := make(chan struct{}), make(chan struct{})
		o.State = NewState(o, func(p *Permit) error {
			close(entered)
			<-release
			return BindAttachment(o, p)
		})
		i := NewInvocation()
		h, err := Claim(o, i)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- h.Attach(o, i) }()
		<-entered
		pre := CheckPublicDrain(o)
		_, authoritative := BeginPublicDrain(o)
		close(release)
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		if !errors.Is(pre, ErrAttachment) || !errors.Is(authoritative, ErrAttachment) {
			t.Fatalf("attachment race: pre=%v admission=%v", pre, authoritative)
		}
		if !h.Attached(o, i) {
			t.Fatal("rejected ordinary call invalidated acknowledged attachment")
		}
	})
	t.Run("drain-first", func(t *testing.T) {
		o := &attachmentTestOwner{}
		calls := 0
		o.State = NewState(o, func(p *Permit) error { calls++; return BindAttachment(o, p) })
		i := NewInvocation()
		h, err := Claim(o, i)
		if err != nil {
			t.Fatal(err)
		}
		r, err := BeginPublicDrain(o)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.Attach(o, i); !errors.Is(err, ErrAttachment) {
			t.Fatalf("overlapping attach=%v", err)
		}
		EndDrain(r)
		if err = h.Attach(o, i); !errors.Is(err, ErrAttachment) {
			t.Fatalf("retried handoff=%v", err)
		}
		if calls != 0 {
			t.Fatalf("native attachment calls=%d", calls)
		}
	})
}

func TestDrainAttemptRetiresWithoutConsumptionAndOnPanic(t *testing.T) {
	for _, mode := range []string{"unused", "panic"} {
		t.Run(mode, func(t *testing.T) {
			o := &attachmentTestOwner{}
			i := NewInvocation()
			var permit *DrainPermit
			o.State = NewState(o, func(p *Permit) error { return BindAttachment(o, p) }, NativeOperations{
				Completion: func(_ context.Context, p *DrainPermit, _ error) error {
					permit = p
					if mode == "panic" {
						panic("native callback panic")
					}
					return nil
				},
			})
			h, err := Claim(o, i)
			if err != nil {
				t.Fatal(err)
			}
			if err = h.Attach(o, i); err != nil {
				t.Fatal(err)
			}
			if mode == "panic" {
				func() {
					defer func() {
						if recover() != "native callback panic" {
							t.Error("panic was not preserved")
						}
					}()
					_ = h.Call(context.Background(), o, i, Completion, nil)
				}()
			} else if err = h.Call(context.Background(), o, i, Completion, nil); !errors.Is(err, ErrDrainPermit) {
				t.Fatalf("unconsumed success=%v", err)
			}
			if o.pending != nil || permit == nil || !permit.consumed {
				t.Fatal("attempt remained usable")
			}
			if _, err = ConsumeDrain(o, permit, Completion, nil); !errors.Is(err, ErrDrainPermit) {
				t.Fatalf("retired permit=%v", err)
			}
		})
	}
}

func TestAdmissionDenialBelongsOnlyToCurrentAttempt(t *testing.T) {
	o := &attachmentTestOwner{}
	i := NewInvocation()
	var oldDenial error
	style := "direct"
	effects := 0
	o.State = NewState(o, func(p *Permit) error { return BindAttachment(o, p) }, NativeOperations{
		Completion: func(_ context.Context, p *DrainPermit, _ error) error {
			r, err := ConsumeDrain(o, p, Completion, nil)
			if err != nil {
				return err
			}
			defer EndDrain(r)
			effects++
			switch style {
			case "wrapped":
				return fmt.Errorf("native diagnostic: %w", oldDenial)
			case "joined":
				return errors.Join(errors.New("native diagnostic"), oldDenial)
			}
			return oldDenial
		},
	})
	h, err := Claim(o, i)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Attach(o, i); err != nil {
		t.Fatal(err)
	}
	if err = EnrollActivities(i); err != nil {
		t.Fatal(err)
	}
	oldDenial = h.Call(context.Background(), o, i, Completion, nil)
	if !IsAdmissionDenied(oldDenial) || effects != 0 {
		t.Fatalf("current denial=%v effects=%d", oldDenial, effects)
	}
	if err = CloseActivities(i); err != nil {
		t.Fatal(err)
	}
	returned := h.Call(context.Background(), o, i, Completion, nil)
	if returned == nil || IsAdmissionDenied(returned) || !errors.Is(returned, ErrActivityClosed) || effects != 1 {
		t.Fatalf("retained error misclassified after effects: error=%v effects=%d", returned, effects)
	}
	for _, nextStyle := range []string{"wrapped", "joined"} {
		style = nextStyle
		returned = h.Call(context.Background(), o, i, Completion, nil)
		if returned == nil || IsAdmissionDenied(returned) || !errors.Is(returned, ErrActivityClosed) {
			t.Fatalf("%s retained diagnostic reauthorized cleanup: %v", style, returned)
		}
	}
	if effects != 3 {
		t.Fatalf("admitted effects=%d", effects)
	}
	if IsAdmissionDenied(errors.Join(errors.New("native failure"), oldDenial)) {
		t.Fatal("nested older denial classified as current attempt")
	}
}

type drainMethodError struct{ probe func() }

func TestProtectedRejectedWorkCannotReadmitAndOrdinaryRemainsDormant(t *testing.T) {
	i := NewInvocation()
	cause := errors.New("rejected protected mutation")
	if FailProtected(i, cause) || ProtectedFailure(i) != nil {
		t.Fatal("ordinary rejection became sticky")
	}
	a, err := AdmitActivity(i)
	if err != nil {
		t.Fatal(err)
	}
	if err = FinishActivity(i, a, cause); err != nil {
		t.Fatal(err)
	}
	if err = EnrollActivities(i); err != nil {
		t.Fatalf("ordinary caught failure was retroactive: %v", err)
	}
	if !FailProtected(i, cause) {
		t.Fatal("protected failure lost")
	}
	if _, err = AdmitActivity(i); !errors.Is(err, cause) {
		t.Fatalf("later admission escaped protected failure: %v", err)
	}
	if err = EnrollActivities(i); !errors.Is(err, cause) {
		t.Fatalf("reenrollment escaped protected failure: %v", err)
	}
}

func (e *drainMethodError) Error() string { return "callback diagnostic" }
func (e *drainMethodError) Is(error) bool { e.probe(); return false }

func TestDrainErrorMethodsRunAfterPermitRetirement(t *testing.T) {
	for _, mode := range []string{"reentrant", "panic"} {
		t.Run(mode, func(t *testing.T) {
			o := &attachmentTestOwner{}
			i := NewInvocation()
			var h Handle
			var retained *DrainPermit
			probeCalls := 0
			other := &drainMethodError{probe: func() {
				probeCalls++
				if !h.Valid(o, i) {
					t.Error("owner unexpectedly invalid")
				}
				if o.pending != nil {
					t.Error("attempt not retired before application error method")
				}
				if mode == "panic" {
					panic("error method panic")
				}
			}}
			o.State = NewState(o, func(p *Permit) error { return BindAttachment(o, p) }, NativeOperations{
				Completion: func(_ context.Context, p *DrainPermit, _ error) error {
					retained = p
					_, _ = ConsumeDrain(o, p, Completion, nil)
					return other
				},
			})
			var err error
			h, err = Claim(o, i)
			if err != nil {
				t.Fatal(err)
			}
			if err = h.Attach(o, i); err != nil {
				t.Fatal(err)
			}
			if err = EnrollActivities(i); err != nil {
				t.Fatal(err)
			}
			if mode == "panic" {
				func() {
					defer func() {
						if recover() != "error method panic" {
							t.Error("error-method panic lost")
						}
					}()
					_ = h.Call(context.Background(), o, i, Completion, nil)
				}()
			} else {
				err = h.Call(context.Background(), o, i, Completion, nil)
				if !IsAdmissionDenied(err) || !errors.Is(err, ErrActivityClosed) {
					t.Fatalf("actual refusal lost: %v", err)
				}
			}
			if probeCalls == 0 || o.pending != nil || !retained.consumed {
				t.Fatal("error method or retirement missing")
			}
		})
	}
}
