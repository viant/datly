package drainowner

import (
	"context"
	"errors"
	"reflect"
)

type bindingTargetKind uint8

const (
	bindingTargetNone bindingTargetKind = iota
	bindingTargetReader
	bindingTargetScopedCustom
)

// ScopedGroupTarget is internal dispatcher-validated registration authority,
// not a claim that arbitrary Go handlers are read-only or sandboxed.
type ScopedGroupTarget struct{ cell *scopedGroupTarget }
type scopedGroupTarget struct {
	parent, child                  any
	parentRoute, childRoute, scope string
	childType                      reflect.Type
}

func NewScopedGroupTarget(parent, child any, parentRoute, childRoute, scope string, childType reflect.Type) (ScopedGroupTarget, error) {
	if !groupRegistration(parent) || !groupRegistration(child) || parent == child || parentRoute == "" || childRoute == "" || parentRoute == childRoute || scope == "" || childType == nil {
		return ScopedGroupTarget{}, ErrBindingGroup
	}
	return ScopedGroupTarget{cell: &scopedGroupTarget{parent: parent, child: child, parentRoute: parentRoute, childRoute: childRoute, scope: scope, childType: childType}}, nil
}
func groupRegistration(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Struct {
		return v.Type().Comparable()
	}
	return v.Kind() == reflect.Pointer && !v.IsNil()
}
func ValidateScopedBindingGroupTarget(ctx context.Context, target string, registration any, descriptor ScopedGroupTarget, inputs ...BindingGroupInput) error {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return ErrBindingGroup
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	d := descriptor.cell
	if d == nil || d.parent != registration || d.parentRoute != target || ticket.parent != nil || len(ticket.inputs) != 0 {
		return ErrBindingGroup
	}
	if err := validateBindingTargetLocked(ledger, ticket, target, inputs); err != nil {
		return err
	}
	ticket.kind = bindingTargetScopedCustom
	ticket.registration = registration
	ticket.scoped = d
	return nil
}
func ValidateRegisteredBindingGroupReader(ctx context.Context, target string, registration any, inputs ...BindingGroupInput) error {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket.parent != nil && (ticket.expectedRegistration != registration || !groupRegistration(registration)) {
		return ErrBindingGroup
	}
	if err := validateBindingTargetLocked(ledger, ticket, target, inputs); err != nil {
		return err
	}
	ticket.kind = bindingTargetReader
	ticket.registration = registration
	return nil
}

// BindingGroupTargetAdmission checks the actual validated kind without changing
// the native CanonicalReader flag for the scoped custom invocation.
func BindingGroupTargetAdmission(ctx context.Context, invocation *Invocation, canonicalReader bool) error {
	if err := BindingGroupAdmission(ctx, invocation); err != nil {
		return err
	}
	ticket := bindingContext(ctx)
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !bindingTicketLiveLocked(ledger, ticket) || (canonicalReader && ticket.kind != bindingTargetReader) || (!canonicalReader && (ticket.kind != bindingTargetScopedCustom || ticket.scoped == nil)) {
		return ErrBindingGroup
	}
	return nil
}

type bindingScopeLease struct {
	ticket *bindingTicket
	open   bool
}
type bindingScopeContextKey struct{}
type BindingGroupScope struct{ cell *bindingScopeLease }

// EnterBindingGroupScope is used only around the canonical named BindScope
// plan. Lookup and imperative invokers cannot obtain this private lease.
func EnterBindingGroupScope(ctx context.Context, scope string) (context.Context, BindingGroupScope, error) {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return ctx, BindingGroupScope{}, nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket.kind != bindingTargetScopedCustom || ticket.scoped == nil || scope != ticket.scoped.scope {
		return ctx, BindingGroupScope{}, nil
	}
	if !bindingTicketLiveLocked(ledger, ticket) || !ticket.activityUsed || ticket.finished || (ticket.scopeLease != nil && ticket.scopeLease.open) {
		return nil, BindingGroupScope{}, ErrBindingGroup
	}
	lease := &bindingScopeLease{ticket: ticket, open: true}
	ticket.scopeLease = lease
	return context.WithValue(ctx, bindingScopeContextKey{}, lease), BindingGroupScope{cell: lease}, nil
}
func (s BindingGroupScope) Close() error {
	if s.cell == nil {
		return nil
	}
	ticket := s.cell.ticket
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !s.cell.open {
		return ErrBindingGroup
	}
	s.cell.open = false
	if child := ticket.child; child != nil && !child.finished {
		appendFailureLocked(ledger, ErrActivityUnfinished, nil, "", true, 0)
		return ErrActivityUnfinished
	}
	return nil
}

type BindingGroupDispatch struct{ cell *bindingTicket }

// PrepareBindingGroupDispatch reserves one declared reader leaf only from its
// actual registered parent's sealed provider during the selected scope lease.
func PrepareBindingGroupDispatch(ctx context.Context, parent any, target string, typ reflect.Type) (context.Context, BindingGroupDispatch, error) {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return ctx, BindingGroupDispatch{}, nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket.kind != bindingTargetScopedCustom {
		return ctx, BindingGroupDispatch{}, nil
	}
	d := ticket.scoped
	lease, _ := ctx.Value(bindingScopeContextKey{}).(*bindingScopeLease)
	if !bindingTicketLiveLocked(ledger, ticket) || ticket.finished || !ticket.activityUsed || d == nil || parent != d.parent || target != d.childRoute || typ != d.childType || lease == nil || lease != ticket.scopeLease || !lease.open || ticket.child != nil {
		return nil, BindingGroupDispatch{}, ErrBindingGroup
	}
	if err := ctx.Err(); err != nil {
		return nil, BindingGroupDispatch{}, err
	}
	if err := groupFailureLocked(ledger, ticket.group); err != nil {
		return nil, BindingGroupDispatch{}, err
	}
	descendant := &bindingTicket{group: ticket.group, member: ticket.member, target: target, entered: true, parent: ticket, expectedRegistration: d.child}
	ticket.child = descendant
	return context.WithValue(ctx, bindingGroupContextKey{}, descendant), BindingGroupDispatch{cell: descendant}, nil
}

// Complete correlates native retirement; only an early dispatch failure without
// an activity or a first post-retirement failure appends an observation.
func (d BindingGroupDispatch) Complete(cause error) error {
	ticket := d.cell
	if ticket == nil {
		return nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket.dispatchClosed {
		return ErrBindingGroup
	}
	ticket.dispatchClosed = true
	if ticket.activityUsed && !ticket.finished {
		appendFailureLocked(ledger, ErrActivityUnfinished, nil, "", true, 0)
		return ErrActivityUnfinished
	}
	if ticket.finished {
		if ticket.failure == nil && cause != nil {
			ticket.failure = cause
			appendFailureLocked(ledger, cause, ticket.group, ticket.member, terminalBindingFailure(cause), ticket.observation)
		}
		return nil
	}
	if cause == nil {
		cause = ErrBindingGroup
	}
	ledger.observation++
	ticket.observation = ledger.observation
	ticket.finished = true
	ticket.failure = cause
	appendFailureLocked(ledger, cause, ticket.group, ticket.member, terminalBindingFailure(cause), ticket.observation)
	return nil
}
func bindingTicketLiveLocked(ledger *activityLedger, ticket *bindingTicket) bool {
	if ticket == nil || ledger.group != ticket.group || !ticket.group.open || ledger.closed || !ticket.entered || !ticket.targetValidated {
		return false
	}
	if ticket.parent != nil && (!ticket.parent.activityUsed || ticket.parent.finished) {
		return false
	}
	return true
}
func validateBindingTargetLocked(ledger *activityLedger, ticket *bindingTicket, target string, inputs []BindingGroupInput) error {
	if ledger.group != ticket.group || !ticket.group.open || !ticket.entered || ticket.target == "" || ticket.target != target || ticket.targetValidated || ticket.activityUsed || ledger.closed {
		return ErrBindingGroup
	}
	if ticket.parent != nil && !bindingTicketLiveLocked(ledger, ticket.parent) {
		return ErrBindingGroup
	}
	if err := groupFailureLocked(ledger, ticket.group); err != nil {
		return err
	}
	for _, required := range ticket.inputs {
		matched := false
		for _, actual := range inputs {
			if actual.Kind == required.Kind && actual.In == required.In && actual.Type == required.Type && !actual.Adapted {
				matched = true
				break
			}
		}
		if !matched {
			appendFailureLocked(ledger, ErrBindingGroup, nil, "", true, 0)
			return ErrBindingGroup
		}
	}
	ticket.targetValidated = true
	return nil
}
func bindingTicketRetirementLocked(ledger *activityLedger, ticket *bindingTicket, cause error) (error, error) {
	var introduced error
	if child := ticket.child; child != nil {
		if !child.finished {
			introduced = ErrActivityUnfinished
			cause = errors.Join(cause, introduced)
		} else if cause == nil && child.failure != nil {
			cause = child.failure
			introduced = child.failure
		}
	}
	ticket.finished = true
	ticket.failure = cause
	if ticket.scopeLease != nil {
		ticket.scopeLease.open = false
	}
	return cause, introduced
}

// ScopedBindingGroupMember only selects the private descendant validation path;
// it grants no activity, binding, or invocation authority.
func ScopedBindingGroupMember(ctx context.Context) bool {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return false
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ticket.kind == bindingTargetScopedCustom
}
