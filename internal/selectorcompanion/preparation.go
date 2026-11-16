package selectorcompanion

import (
	"context"
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
)

// preparationContract avoids a registry dependency at the selector owner.
type preparationContract interface {
	Type() reflect.Type
	Route() spec.RouteRef
	Plan() *bindly.Plan
	SelectorPlan() *BindingPlan
}

// PreparationHandoff is implemented by the dispatcher's private invocation receipt.
// It attests binding provenance only; it supplies no execution authority.
type PreparationHandoff interface {
	MatchesPreparation(scope any, seed any, paths []string) bool
	ValidatePreparation(context.Context) error
}

// PreparationBinding is operation-local binding evidence, not reader execution
// or authorization authority. Its fields and copy identity are private.
type PreparationBinding struct {
	self      *PreparationBinding
	handoff   PreparationHandoff
	handler   any
	reader    any
	contract  preparationContract
	input     reflect.Type
	route     spec.RouteRef
	public    *bindly.Plan
	selectors *BindingPlan
}

// NewPreparationBinding is called only by the dispatcher immediately after its
// native query-handler constructor. Go internal visibility is not a caller ACL;
// the sole production call site is protected by a source invariant test.
func NewPreparationBinding(handler, reader any, contract preparationContract, handoff ...PreparationHandoff) *PreparationBinding {
	if !pointerIdentity(handler) || !pointerIdentity(reader) || !pointerIdentity(contract) || contract.Type() == nil || contract.Plan() == nil {
		return nil
	}
	selectors := contract.SelectorPlan()
	if selectors != nil && (selectors.TransportOnly() || selectors.PublicPlan() != contract.Plan()) {
		return nil
	}
	result := &PreparationBinding{handler: handler, reader: reader, contract: contract, input: contract.Type(), route: contract.Route(), public: contract.Plan(), selectors: selectors}
	if len(handoff) > 1 {
		return nil
	}
	if len(handoff) == 1 {
		if !pointerIdentity(handoff[0]) {
			return nil
		}
		result.handoff = handoff[0]
	}
	result.self = result
	return result
}

func pointerIdentity(value any) bool {
	return value != nil && reflect.TypeOf(value).Kind() == reflect.Pointer && !reflect.ValueOf(value).IsNil()
}

// Matches compares actual invocation identities; copied or detached contracts,
// handlers and plans cannot reuse the evidence.
func (p *PreparationBinding) Matches(handler any, contract preparationContract) bool {
	return p != nil && p.self == p && pointerIdentity(handler) && pointerIdentity(contract) && p.handler == handler && p.contract == contract && p.reader != nil && p.input == contract.Type() && p.route == contract.Route() && p.public == contract.Plan() && p.selectors == contract.SelectorPlan()
}

// MatchesRequest admits only the exact seed, ordered paths and protocol scope
// retained by Runtime. Static canonical preparation needs no dynamic handoff.
func (p *PreparationBinding) MatchesRequest(scope any, seed any, paths []string) bool {
	if p == nil || p.self != p {
		return false
	}
	if p.handoff == nil {
		return seed == nil && len(paths) == 0
	}
	return p.handoff.MatchesPreparation(scope, seed, paths)
}

// Validate runs synchronously at both boundaries of successful native binding.
// The receipt must never escape into a prepared query or shared plan.
func (p *PreparationBinding) Validate(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p != nil && p.self == p && p.handoff != nil {
		return p.handoff.ValidatePreparation(ctx)
	}
	return nil
}
