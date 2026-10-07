// Package drainowner retains the identity of one native journal owner. It is
// internal bookkeeping, not a public execution or transaction capability.
package drainowner

import (
	"errors"
	"reflect"
	"sync"
)

var ErrOwner = errors.New("buffered drain requires its exact native owner")
var ErrClaim = errors.New("native drain owner is already attached or retired")
var ErrAttachment = errors.New("native drain attachment does not belong to this invocation")

// State is embedded under a private alias by the actual native Data. Copies
// and promoted methods retain the original receiver identity, not a new owner.
type State struct {
	mu              sync.Mutex
	receiver        any
	claimed         *identity
	claimsClosed    bool
	retired         bool
	attach          func(*Permit) error
	attachAttempted bool
	attaching       *Permit
	attached        bool
	attachmentReady bool
	barrierVersion  uint64
	drains          map[*DrainRecord]struct{}
	operations      NativeOperations
	pending         *DrainPermit
}
type identity struct {
	marker     byte
	activities activityLedger
}
type Invocation struct{ identity *identity }

func NewInvocation() *Invocation { return &Invocation{identity: &identity{marker: 1}} }
func NewState(receiver any, attach func(*Permit) error, operations ...NativeOperations) *State {
	state := &State{receiver: receiver, attach: attach}
	if len(operations) == 1 {
		state.operations = make(NativeOperations, len(operations[0]))
		for operation, native := range operations[0] {
			state.operations[operation] = native
		}
	}
	return state
}
func (s *State) drainOwnerState() *State { return s }

type owner interface{ drainOwnerState() *State }

// Handle has shared claim and attachment state even when copied. Attachment
// exposes no preparation, flush, commit, abort or mutation-admission bypass.
type Handle struct {
	state    *State
	identity *identity
}

func exactState(receiver any) (*State, error) {
	a := reflect.ValueOf(receiver)
	if a.Kind() != reflect.Pointer || a.IsNil() {
		return nil, ErrOwner
	}
	o, ok := receiver.(owner)
	if !ok || o == nil {
		return nil, ErrOwner
	}
	s := sealedState(o)
	if s == nil {
		return nil, ErrOwner
	}
	// Only the original native pointer is an owner; an embedded forwarding
	// wrapper, copied Data, borrowed frame or transplanted cell is not.
	b := reflect.ValueOf(s.receiver)
	if a.Kind() != reflect.Pointer || b.Kind() != reflect.Pointer || a.IsNil() || b.IsNil() || a.Type() != b.Type() || a.Pointer() != b.Pointer() {
		return nil, ErrOwner
	}
	return s, nil
}

// sealedState recovers only unsupported nil promoted-method resolution. Native
// lifecycle operations execute outside this extraction and retain their panics.
func sealedState(o owner) (s *State) {
	defer func() {
		if recover() != nil {
			s = nil
		}
	}()
	return o.drainOwnerState()
}
func Claim(receiver any, invocation *Invocation) (Handle, error) {
	s, err := exactState(receiver)
	if err != nil {
		return Handle{}, err
	}
	if invocation == nil || invocation.identity == nil {
		return Handle{}, ErrClaim
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimsClosed || s.retired || s.claimed != nil || len(s.drains) != 0 {
		return Handle{}, ErrClaim
	}
	s.claimed = invocation.identity
	return Handle{state: s, identity: invocation.identity}, nil
}

// CloseClaims linearizes native attachment against fresh engine claims. An
// earlier claim survives; it is never duplicated by a later invocation.
func CloseClaims(receiver any) {
	s, err := exactState(receiver)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.claimsClosed = true
	s.mu.Unlock()
}

// Retire closes authority lifetime, not transaction outcome. The native owner
// still records the actual commit/rollback/caller-pending result independently.
func Retire(receiver any) {
	s, err := exactState(receiver)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.claimsClosed = true
	s.retired = true
	s.mu.Unlock()
}
func (h Handle) Valid(receiver any, invocation *Invocation) bool {
	s, err := exactState(receiver)
	if err != nil || invocation == nil || invocation.identity == nil || h.state == nil || h.state != s || h.identity != invocation.identity {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.retired && s.claimed == h.identity
}

// Permit is created only for one claim's constructor-bound native attachment.
// A zero value, copy or foreign owner's permit cannot bind that attachment.
type Permit struct {
	state    *State
	identity *identity
	version  uint64
}

func (h Handle) attachmentState(receiver any, invocation *Invocation) (*State, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	if invocation == nil || invocation.identity == nil || h.state != s || h.identity != invocation.identity {
		return nil, ErrAttachment
	}
	return s, nil
}

// Attach invokes only the exact constructor-bound native begin operation. No
// state mutex is held across that native operation or its callbacks.
func (h Handle) Attach(receiver any, invocation *Invocation) (err error) {
	s, err := h.attachmentState(receiver, invocation)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.retired || s.claimed != h.identity || s.claimsClosed || s.attachAttempted || s.attach == nil {
		s.mu.Unlock()
		return ErrAttachment
	}
	s.attachAttempted = true
	s.claimsClosed = true
	if len(s.drains) != 0 {
		s.retired = true
		ledger := &h.identity.activities
		ledger.mu.Lock()
		if ledger.enrolled {
			appendFailureLocked(ledger, ErrDrainOverlap, nil, "", true, 0)
		}
		ledger.mu.Unlock()
		s.mu.Unlock()
		return ErrAttachment
	}
	permit := &Permit{state: s, identity: h.identity, version: s.barrierVersion}
	s.attaching = permit
	nativeAttach := s.attach
	s.mu.Unlock()
	returned := false
	// A native panic must still consume this private attempt. Propagation stays
	// with the existing caller's panic/error policy rather than inventing one.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.attaching = nil
		if !returned || err != nil || !s.attached || s.retired || s.barrierVersion != permit.version {
			if returned && err == nil {
				err = ErrAttachment
			}
			s.retired = true
			s.attachmentReady = false
			return
		}
		s.attachmentReady = true
	}()
	err = nativeAttach(permit)
	returned = true
	if err == nil {
		s.mu.Lock()
		bound := s.attached && !s.retired && s.barrierVersion == permit.version
		s.mu.Unlock()
		if !bound {
			err = ErrAttachment
		}
	}
	return err
}

// BindAttachment is called by the actual native begin operation while holding
// executionMu then owner.mu, immediately before publishing invocation=true.
// Identity-state locking never acquires native locks in the reverse direction.
func BindAttachment(receiver any, permit *Permit) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	if permit == nil || permit.state != s || permit.identity == nil {
		return ErrAttachment
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.attaching != permit || s.attached || s.claimed != permit.identity || s.barrierVersion != permit.version {
		return ErrAttachment
	}
	s.attached = true
	return nil
}

// Attached reports a completed private native handoff, not transaction outcome
// or permission to execute an unfinished program or drain the journal.
func (h Handle) Attached(receiver any, invocation *Invocation) bool {
	s, err := h.attachmentState(receiver, invocation)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.retired && s.claimed == h.identity && s.attached && s.attachmentReady
}

// Seal closes fresh attachment admission. A seal racing an unacknowledged
// native callback cannot promote an identity-only claim into a ready handoff.
// An already acknowledged handoff stays attached until native retirement.
func Seal(receiver any) {
	s, err := exactState(receiver)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.claimsClosed = true
	s.barrierVersion++
	s.mu.Unlock()
}

// OwnsAttachment reports historical binding by this exact native issuer. It
// survives retirement solely for truthful cleanup accounting; it grants no
// execution, journal drain or transaction outcome authority.
func (h Handle) OwnsAttachment(receiver any, invocation *Invocation) bool {
	s, err := h.attachmentState(receiver, invocation)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed == h.identity && s.attached
}
