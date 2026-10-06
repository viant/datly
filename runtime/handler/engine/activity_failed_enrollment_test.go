package engine

import (
	"errors"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/sql/dml"
)

// This tests canonical lifecycle classification via native owner identity. It
// does not claim a SQL drain or transaction-outcome proof.
func TestFailedEnrollmentKeepsCanonicalProtectedLifetime(t *testing.T) {
	root := &dataScope{}
	root.root = root
	issuer := root.nativeIssuerLocked()
	data := dml.NewData(nil)
	if _, err := drainowner.Claim(data, issuer); err != nil {
		t.Fatal(err)
	}
	record, err := drainowner.BeginPublicDrain(data)
	if err != nil {
		t.Fatal(err)
	}
	child := &dataScope{root: root}
	if err = child.enrollBufferedScope(); !errors.Is(err, drainowner.ErrDrainOverlap) {
		t.Fatalf("overlap=%v", err)
	}
	if child.requireBufferedOwner || root.bufferedScopeEnrolled {
		t.Fatal("failed enrollment fabricated successful policy")
	}
	if !root.protectedLifetime() {
		t.Fatal("failed ledger treated as ordinary")
	}
	drainowner.EndDrain(record)
	if err = child.enrollBufferedScope(); !errors.Is(err, drainowner.ErrDrainOverlap) {
		t.Fatalf("later child lost sticky failure: %v", err)
	}
	root.completionStarted = true
	if !child.bufferedLifetimeClosed() {
		t.Fatal("retained child lifetime reopened")
	}
}

func TestClosedDormantLedgerDoesNotBecomeProtected(t *testing.T) {
	root := &dataScope{}
	root.root = root
	if err := drainowner.CloseActivities(root.nativeIssuerLocked()); err != nil {
		t.Fatal(err)
	}
	if err := root.enrollBufferedScope(); !errors.Is(err, drainowner.ErrActivityClosed) {
		t.Fatalf("late enrollment=%v", err)
	}
	if root.protectedLifetime() {
		t.Fatal("closed ordinary invocation became protected")
	}
}
