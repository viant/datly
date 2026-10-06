package drainowner

import (
	"errors"
	"sync/atomic"
	"testing"
)

// These are internal handoff-primitive tests. Genuine native owner/transaction
// coverage is in sql/dml/data_drain_attachment_test.go.
type attachmentTestOwner struct{ *State }

func TestAttachmentAcknowledgmentFollowsCallbackAndClosure(t *testing.T) {
	for _, closeKind := range []string{"none", "seal", "retire"} {
		t.Run(closeKind, func(t *testing.T) {
			o := &attachmentTestOwner{}
			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			var retained *Permit
			o.State = NewState(o, func(p *Permit) error {
				calls.Add(1)
				retained = p
				copied := *p
				if !errors.Is(BindAttachment(o, &copied), ErrAttachment) {
					t.Error("copied permit accepted")
				}
				if err := BindAttachment(o, p); err != nil {
					return err
				}
				close(entered)
				<-release
				return nil
			})
			issuer := NewInvocation()
			h, err := Claim(o, issuer)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- h.Attach(o, issuer) }()
			<-entered
			if h.Attached(o, issuer) {
				t.Fatal("readiness before callback returned")
			}
			switch closeKind {
			case "seal":
				Seal(o)
			case "retire":
				Retire(o)
			}
			close(release)
			err = <-result
			if closeKind == "none" {
				if err != nil || !h.Attached(o, issuer) {
					t.Fatal("native handoff missing", err)
				}
			} else {
				if !errors.Is(err, ErrAttachment) || h.Attached(o, issuer) || h.Valid(o, issuer) {
					t.Fatal("closed handoff became ready", err)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("native begin replayed")
			}
			if !errors.Is(BindAttachment(o, retained), ErrAttachment) {
				t.Fatal("retained permit used after callback")
			}
			if !errors.Is(h.Attach(o, issuer), ErrAttachment) {
				t.Fatal("callback retry")
			}
		})
	}
}
func TestAttachmentPanicConsumesAttemptWithoutSwallowing(t *testing.T) {
	for _, bound := range []bool{false, true} {
		o := &attachmentTestOwner{}
		sentinel := &struct{ value string }{"native begin panic"}
		var calls atomic.Int32
		o.State = NewState(o, func(p *Permit) error {
			calls.Add(1)
			if bound {
				if err := BindAttachment(o, p); err != nil {
					return err
				}
			}
			panic(sentinel)
		})
		issuer := NewInvocation()
		h, err := Claim(o, issuer)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if recover() != sentinel {
					t.Error("native panic identity changed")
				}
			}()
			_ = h.Attach(o, issuer)
		}()
		if h.Attached(o, issuer) || h.Valid(o, issuer) || calls.Load() != 1 {
			t.Fatal("panicked attempt remains live")
		}
		if !errors.Is(h.Attach(o, issuer), ErrAttachment) {
			t.Fatal("panicked begin replayed")
		}
	}
}
