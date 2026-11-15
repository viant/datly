package selectorcompanion

import (
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

// PreparationBinding is operation-local binding evidence, not reader execution
// or authorization authority. Its fields and copy identity are private.
type PreparationBinding struct {
	self      *PreparationBinding
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
func NewPreparationBinding(handler, reader any, contract preparationContract) *PreparationBinding {
	if !pointerIdentity(handler) || !pointerIdentity(reader) || !pointerIdentity(contract) || contract.Type() == nil || contract.Plan() == nil {
		return nil
	}
	selectors := contract.SelectorPlan()
	if selectors != nil && (selectors.TransportOnly() || selectors.PublicPlan() != contract.Plan()) {
		return nil
	}
	result := &PreparationBinding{handler: handler, reader: reader, contract: contract, input: contract.Type(), route: contract.Route(), public: contract.Plan(), selectors: selectors}
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
