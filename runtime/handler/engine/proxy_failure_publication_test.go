package engine

import (
	"errors"
	"testing"

	"github.com/viant/datly/internal/drainowner"
)

// These prove guard/ledger bookkeeping. Native capability and physical SQL
// coverage is separate; binding never grants a transaction operation.
func TestProxyIssuerBindingPublishesEarlierFailureAndRejectsReplacement(t *testing.T) {
	i := drainowner.NewInvocation()
	cause := errors.New("proxy rejection before issuer binding")
	g := &mutationGuard{guardedFailure: cause}
	if err := g.bindProtectedIssuer(i); err != nil {
		t.Fatal(err)
	}
	if drainowner.ProtectedFailure(i) != nil || g.protectedIssuer != nil {
		t.Fatal("dormant root was bound or poisoned")
	}
	if err := drainowner.EnrollActivities(i); err != nil {
		t.Fatal(err)
	}
	if err := g.bindProtectedIssuer(i); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(drainowner.ProtectedFailure(i), cause) {
		t.Fatal("pre-binding rejection lost")
	}
	foreign := drainowner.NewInvocation()
	if err := drainowner.EnrollActivities(foreign); err != nil {
		t.Fatal(err)
	}
	if err := g.bindProtectedIssuer(foreign); err == nil {
		t.Fatal("proxy issuer replaced")
	}
	if drainowner.ProtectedFailure(foreign) != nil {
		t.Fatal("failure transplanted to foreign ledger")
	}
}

func TestProxyIssuerNeverPublishesOrdinaryEligibilityViolation(t *testing.T) {
	i := drainowner.NewInvocation()
	if err := drainowner.EnrollActivities(i); err != nil {
		t.Fatal(err)
	}
	g := &mutationGuard{violation: ErrWriteEligibilityMutation}
	if err := g.bindProtectedIssuer(i); err != nil {
		t.Fatal(err)
	}
	if drainowner.ProtectedFailure(i) != nil {
		t.Fatal("ordinary violation became a protected failure")
	}
}
