package drainowner

import (
	"context"
	"errors"
)

var ErrDrain = errors.New("public native drain is forbidden in a protected invocation")
var ErrDrainOverlap = errors.New("protected enrollment overlaps an in-flight native drain")
var ErrDrainPermit = errors.New("native drain permit is invalid for this owner or operation")

// admissionFailure proves that a valid operation attempt was denied before
// native effects. It never classifies an uncertain transaction outcome.
type admissionFailure struct{ cause error }

func (e *admissionFailure) Error() string { return e.cause.Error() }
func (e *admissionFailure) Unwrap() error { return e.cause }
func IsAdmissionDenied(err error) bool    { _, denied := err.(*admissionFailure); return denied }

// returnedFailure keeps a retained denial from another operation diagnostic-only.
type returnedFailure struct{ cause error }

func (e *returnedFailure) Error() string { return e.cause.Error() }
func (e *returnedFailure) Unwrap() error { return e.cause }

type Operation uint8

const (
	LocalPreparation Operation = iota + 1
	AllPreparation
	Completion
	Abort
	PublicFlush
)

// Native operations are captured by the Data constructor, never supplied by a
// Handle caller. Abort is deliberately a distinct operation from completion.
type NativeOperations map[Operation]func(context.Context, *DrainPermit, error) error

type DrainPermit struct {
	state     *State
	identity  *identity
	operation Operation
	consumed  bool
	denied    error
}
type DrainRecord struct {
	self     *DrainRecord
	state    *State
	identity *identity
	ended    bool
}

func attachmentDrainLocked(s *State) error {
	if s.attaching == nil {
		return nil
	}
	if s.claimed != nil {
		ledger := &s.claimed.activities
		ledger.mu.Lock()
		if ledger.enrolled {
			appendFailureLocked(ledger, ErrDrainOverlap, nil, "", true, 0)
		}
		ledger.mu.Unlock()
	}
	return ErrAttachment
}

func publicDrainLocked(s *State) error {
	if err := attachmentDrainLocked(s); err != nil {
		return err
	}
	if s.claimed == nil {
		return nil
	}
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.enrolled {
		appendFailureLocked(ledger, ErrDrain, nil, "", true, 0)
		return ErrDrain
	}
	return nil
}

// This is rejection only. Native implementations repeat authoritative admission
// under executionMu, before any journal, seal, transaction or outcome effect.
func CheckPublicDrain(receiver any) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return publicDrainLocked(s)
}

// BeginPublicDrain is invoked by the native serialized operation. State-local
// records protect pre-claim operations; a claimed issuer uses that exact record.
func BeginPublicDrain(receiver any) (*DrainRecord, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := attachmentDrainLocked(s); err != nil {
		return nil, err
	}
	if s.claimed == nil {
		return addDrainLocked(s, nil), nil
	}
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.enrolled {
		appendFailureLocked(ledger, ErrDrain, nil, "", true, 0)
		return nil, ErrDrain
	}
	return addDrainLocked(s, s.claimed), nil
}

// addDrainLocked requires State.mu and, for a claimed issuer, its ledger.mu.
func addDrainLocked(s *State, issuer *identity) *DrainRecord {
	record := &DrainRecord{state: s, identity: issuer}
	record.self = record
	if s.drains == nil {
		s.drains = map[*DrainRecord]struct{}{}
	}
	s.drains[record] = struct{}{}
	if issuer != nil {
		ledger := &issuer.activities
		if ledger.drains == nil {
			ledger.drains = map[*DrainRecord]struct{}{}
		}
		ledger.drains[record] = struct{}{}
	}
	return record
}
func EndDrain(record *DrainRecord) {
	if record == nil || record.self != record || record.state == nil {
		return
	}
	s := record.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.ended {
		return
	}
	record.ended = true
	delete(s.drains, record)
	if record.identity != nil {
		ledger := &record.identity.activities
		ledger.mu.Lock()
		delete(ledger.drains, record)
		ledger.mu.Unlock()
	}
}

// Call dispatches only the exact constructor-bound operation. Native consumption
// validates this exact pending pointer after taking executionMu. Each attempt
// ends once even if the native implementation panics.
func (h Handle) Call(ctx context.Context, receiver any, invocation *Invocation, operation Operation, cause error) (err error) {
	s, err := h.attachmentState(receiver, invocation)
	if err != nil {
		return err
	}
	s.mu.Lock()
	abort := operation == Abort && cause != nil
	if s.claimed != h.identity || !s.attached || (!abort && (s.retired || !s.attachmentReady)) || s.pending != nil || s.operations[operation] == nil || operation == PublicFlush || (operation == Abort && !abort) {
		s.mu.Unlock()
		return ErrDrainPermit
	}
	permit := &DrainPermit{state: s, identity: h.identity, operation: operation}
	s.pending = permit
	native := s.operations[operation]
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.pending == permit {
			s.pending = nil
		}
		denied, consumed := permit.denied, permit.consumed
		permit.consumed = true
		s.mu.Unlock()
		if denied != nil {
			if err == nil {
				err = denied
			} else if !errors.Is(err, denied) {
				err = errors.Join(denied, err)
			}
			err = &admissionFailure{cause: err}
		} else if IsAdmissionDenied(err) {
			err = &returnedFailure{cause: err}
		}
		if err == nil && !consumed {
			err = ErrDrainPermit
		}
	}()
	return native(ctx, permit, cause)
}

// ConsumeDrain is called after executionMu, before native side effects. No
// state/ledger lock remains held across SQL or a constructor callback.
func ConsumeDrain(receiver any, permit *DrainPermit, operation Operation, cause error) (*DrainRecord, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	abort := operation == Abort && cause != nil
	if permit == nil || permit.state != s || s.pending != permit || permit.consumed || permit.operation != operation || permit.identity != s.claimed || !s.attached || (!abort && (s.retired || !s.attachmentReady)) || (operation == Abort && !abort) {
		return nil, ErrDrainPermit
	}
	permit.consumed = true
	ledger := &permit.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.enrolled && !abort {
		switch {
		case !ledger.closed:
			err = ErrActivityClosed
		case len(ledger.active) != 0:
			err = ErrActivityUnfinished
		case ledger.failure != nil:
			err = ledger.failure
		}
	}
	if err != nil {
		permit.denied = err
		return nil, err
	}
	return addDrainLocked(s, permit.identity), nil
}

// FailProtectedOwner uses only the actual receiver's sealed claimed identity.
func FailProtectedOwner(receiver any, cause error) bool {
	s, err := exactState(receiver)
	if err != nil || cause == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		return false
	}
	return FailProtected(&Invocation{identity: s.claimed}, cause)
}
func ProtectedOwnerFailure(receiver any) error {
	s, err := exactState(receiver)
	if err != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		return nil
	}
	return ProtectedFailure(&Invocation{identity: s.claimed})
}

func OwnerActivitiesEnrolled(receiver any) bool {
	s, err := exactState(receiver)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed != nil && ActivitiesEnrolled(&Invocation{identity: s.claimed})
}

// CheckProtectedDrainInFlight rejects callback reentry without waiting on the
// native execution mutex. Retirement does not end the enclosing drain record.
func CheckProtectedDrainInFlight(receiver any) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil || len(s.drains) == 0 {
		return nil
	}
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !ledger.enrolled {
		return nil
	}
	appendFailureLocked(ledger, ErrDrain, nil, "", true, 0)
	return ErrDrain
}
