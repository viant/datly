package report

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

// NewFacade links a generated cube input to the same selector/predicate facade
// used by dynamic DQL. The source component remains the execution authority.
func NewFacade[I, O any](definition Definition) (handler.TypedHandler, error) {
	if definition.Target.Component.Name == "" || definition.Target.Route.Method == "" || definition.Target.Route.Path == "" {
		return nil, fmt.Errorf("cube facade requires an exact source component and route")
	}
	plan, err := definition.Compile(reflect.TypeFor[I](), reflect.TypeFor[O]())
	if err != nil {
		return nil, err
	}
	return &facade{Handler: NewHandler(plan), parameters: cloneParams(definition.Parameters)}, nil
}

// DecodeDefinition decodes generation-owned metadata, never SQL or caller input.
func DecodeDefinition(encoded string) (Definition, error) {
	var definition Definition
	if err := json.Unmarshal([]byte(encoded), &definition); err != nil {
		return definition, fmt.Errorf("decode cube definition: %w", err)
	}
	return definition, nil
}

type facade struct {
	*Handler
	parameters []*spec.Parameter
}

func (h *facade) InputType() reflect.Type               { return h.plan.inputType }
func (h *facade) OutputType() reflect.Type              { return h.plan.outputType }
func (h *facade) ContractParameters() []*spec.Parameter { return cloneParams(h.parameters) }

// NewLinkedFacade is the entire generated handler factory. Owner establishes
// package identity from the actual linked source holder, never a global name.
func NewLinkedFacade[I, O, Owner any](encoded string) (handler.TypedHandler, error) {
	definition, err := DecodeDefinition(encoded)
	if err != nil {
		return nil, err
	}
	definition.Target.Component.Scope = reflect.TypeFor[Owner]().PkgPath()
	return NewFacade[I, O](definition)
}
