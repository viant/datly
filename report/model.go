package report

import (
	"reflect"

	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

// Source is an authority-resolved base component after its canonical input
// contract has been compiled and before runtime registration.
type Source struct {
	Component  *spec.Component
	Input      *registry.InputContract
	OutputType reflect.Type
}

// Derived is one ordinary cube component ready for normal artifact compilation
// and runtime registration.
type Derived struct {
	Component  *spec.Component
	InputType  reflect.Type
	OutputType reflect.Type
	Handler    handler.Handler
	Plan       *Plan
	Type       *x.Type
}

// Project contains the detached base metadata and newly derived components.
type Project struct {
	components []*spec.Component
	derived    []*Derived
}

func (p *Project) Components() []*spec.Component {
	if p == nil {
		return nil
	}
	result := make([]*spec.Component, len(p.components))
	for i, component := range p.components {
		result[i] = component.Clone()
	}
	return result
}

func (p *Project) Derived() []*Derived {
	if p == nil {
		return nil
	}
	return append([]*Derived(nil), p.derived...)
}

// Plan is immutable execution metadata for one derived cube route.
type Plan struct {
	target       exec.ComponentTarget
	view         string
	inputType    reflect.Type
	outputType   reflect.Type
	dimensions   []selection
	measures     []selection
	filters      []filter
	orderIndex   []int
	limitIndex   []int
	offsetIndex  []int
	holderByName map[string][]string
}

type selection struct {
	name  string
	index []int
}

type filter struct {
	name       string
	location   state.Location
	sourceType reflect.Type
	index      []int
}

func (p *Plan) Target() exec.ComponentTarget {
	if p == nil {
		return exec.ComponentTarget{}
	}
	return p.target
}

func (p *Plan) InputType() reflect.Type {
	if p == nil {
		return nil
	}
	return p.inputType
}

func (p *Plan) OutputType() reflect.Type {
	if p == nil {
		return nil
	}
	return p.outputType
}
