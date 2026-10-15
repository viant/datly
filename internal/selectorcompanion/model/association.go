// Package model carries immutable compiled selector-holder identity without
// depending on spec, tags, bootstrap, or invocation state.
package model

import "github.com/viant/x"

// Association retains only the checked declaration identity. Constructor and
// accessors detach mutable source AST through the established x clone boundary.
type Association struct {
	holder, companion *x.Type
	field             string
}

func New(holder *x.Type, field string, companion *x.Type) (*Association, error) {
	h, err := (x.Cloner{}).Type(holder)
	if err != nil {
		return nil, err
	}
	c, err := (x.Cloner{}).Type(companion)
	if err != nil {
		return nil, err
	}
	return &Association{holder: h, field: field, companion: c}, nil
}
func (a *Association) Holder() (*x.Type, error)    { return (x.Cloner{}).Type(a.holder) }
func (a *Association) Companion() (*x.Type, error) { return (x.Cloner{}).Type(a.companion) }
func (a *Association) Field() string               { return a.field }
