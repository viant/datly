package drainowner

import (
	"errors"
	"fmt"
	"sync"
)

var ErrActivity = errors.New("activity does not belong to this invocation")
var ErrActivityClosed = errors.New("invocation activity admission is closed")

// ErrDormantActivityClosed is determined under the same lock as closure. It is
// a classification, not permission to reopen or enroll a completed invocation.
var ErrDormantActivityClosed = errors.New("ordinary invocation activity admission is permanently closed")
var ErrActivityUnfinished = errors.New("invocation activity has not finished")

type activityLedger struct {
	mu                sync.Mutex
	active            map[*activityCell]struct{}
	enrolled          bool
	closed            bool
	failure           error
	entries           []bindingFailure
	group             *bindingGroupCell
	observation       uint64
	transactionStarts uint
	drains            map[*DrainRecord]struct{}
	prefix            *prefixGrant
}
type activityCell struct {
	identity    *identity
	frame       *Frame
	finished    bool
	group       *bindingGroupCell
	member      string
	observation uint64
}

// Activity copies share one private cell and therefore one retirement. This
// bookkeeping token carries no native execution, preparation or drain grant.
type Activity struct{ cell *activityCell }

func invocationLedger(invocation *Invocation) (*activityLedger, error) {
	if invocation == nil || invocation.identity == nil {
		return nil, ErrActivity
	}
	return &invocation.identity.activities, nil
}
func AdmitActivity(invocation *Invocation) (Activity, error) {
	return admitActivity(invocation, nil)
}

// AdmitFrameActivity retains the exact logical caller for engine-owned effects.
// The association grants no drain authority and cannot be replaced by a caller.
func AdmitFrameActivity(invocation *Invocation, frame *Frame) (Activity, error) {
	if frame == nil {
		return Activity{}, ErrActivity
	}
	return admitActivity(invocation, frame)
}

func admitActivity(invocation *Invocation, frame *Frame) (Activity, error) {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return Activity{}, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		if !ledger.enrolled {
			return Activity{}, ErrDormantActivityClosed
		}
		return Activity{}, ErrActivityClosed
	}
	if ledger.prefix != nil || externalPrefixLatched(invocation) {
		return Activity{}, prefixDeniedLocked(ledger)
	}
	if frame != nil {
		journal := invocation.journal()
		journal.mu.Lock()
		valid := frame.journal == journal && frame.open && !journal.freezeStarted
		journal.mu.Unlock()
		if !valid {
			return Activity{}, ErrActivity
		}
	}
	if ledger.group != nil && ledger.group.open {
		appendFailureLocked(ledger, ErrBindingGroup, nil, "", true, 0)
		return Activity{}, ErrBindingGroup
	}
	if ledger.enrolled && ledger.failure != nil {
		return Activity{}, ledger.failure
	}
	if ledger.active == nil {
		ledger.active = map[*activityCell]struct{}{}
	}
	cell := &activityCell{identity: invocation.identity, frame: frame}
	ledger.active[cell] = struct{}{}
	return Activity{cell: cell}, nil
}

// FinishActivity latches an applicable failure before removing live activity.
// Ordinary failures retired before enrollment are deliberately not retroactive.
func FinishActivity(invocation *Invocation, activity Activity, cause error) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if activity.cell == nil || activity.cell.identity != invocation.identity || activity.cell.finished {
		return ErrActivity
	}
	if _, found := ledger.active[activity.cell]; !found {
		return ErrActivity
	}
	if ledger.prefix != nil {
		cause = errors.Join(cause, prefixDeniedLocked(ledger))
	}
	if ledger.enrolled && cause != nil {
		appendFailureLocked(ledger, cause, activity.cell.group, activity.cell.member, terminalBindingFailure(cause), activity.cell.observation)
	}
	activity.cell.finished = true
	if activity.cell.group != nil {
		activity.cell.group.tickets[activity.cell.member].finished = true
	}
	delete(ledger.active, activity.cell)
	return nil
}
func EnrollActivities(invocation *Invocation) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return ErrActivityClosed
	}
	ledger.enrolled = true
	if len(ledger.drains) != 0 {
		err := ErrDrainOverlap
		appendFailureLocked(ledger, err, nil, "", true, 0)
		return err
	}
	return ledger.failure
}

// ActivitiesEnrolled distinguishes a failed protected transition from a closed
// never-enrolled ordinary invocation. It grants no operation authority.
func ActivitiesEnrolled(invocation *Invocation) bool {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.enrolled
}

// CloseActivities permanently closes admission before checking the stable
// ledger. It never waits on its own activity, and it grants no drain authority.
func CloseActivities(invocation *Invocation) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.closed = true
	if ledger.prefix != nil {
		prefixDeniedLocked(ledger)
	}
	if !ledger.enrolled {
		return nil
	}
	if len(ledger.active) != 0 {
		appendFailureLocked(ledger, fmt.Errorf("%w: %d", ErrActivityUnfinished, len(ledger.active)), nil, "", true, 0)
	}
	return ledger.failure
}
func CheckActivities(invocation *Invocation) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !ledger.enrolled {
		return nil
	}
	if len(ledger.active) != 0 {
		return errors.Join(ledger.failure, fmt.Errorf("%w: %d", ErrActivityUnfinished, len(ledger.active)))
	}
	return ledger.failure
}

// FailProtected retains rejected work only after the exact invocation enrolled.
// This bookkeeping grants no native operation or transaction authority.
func FailProtected(invocation *Invocation, cause error) bool {
	ledger, err := invocationLedger(invocation)
	if err != nil || cause == nil {
		return false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !ledger.enrolled {
		return false
	}
	appendFailureLocked(ledger, cause, nil, "", true, 0)
	return true
}
func ProtectedFailure(invocation *Invocation) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !ledger.enrolled {
		return nil
	}
	return ledger.failure
}
