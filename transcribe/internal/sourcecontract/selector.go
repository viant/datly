// Package sourcecontract carries detached evidence from prepared DQL admission
// to its generation plan. It is never runtime contract authority.
package sourcecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type Selector struct {
	original      *spec.Component
	parameters    []byte
	catalog       *typecatalog.Catalog
	types         *typecatalog.Resolver
	input, output *x.Type
}

// NewSelector is called only by the prepared-source admission owner after
// verifying its source/resource lineage and parsing its authored declaration.
func NewSelector(original, current *spec.Component, input, output *x.Type, catalog *typecatalog.Catalog, types *typecatalog.Resolver) (*Selector, error) {
	if original == nil || current == nil || original.SelectorCompanion == nil || current.SelectorCompanion != original.SelectorCompanion || catalog == nil || types == nil {
		return nil, fmt.Errorf("prepared selector source association mismatch")
	}
	if _, err := selectorcompanion.SelectContracts(original, input, output, types.Descriptor); err != nil {
		return nil, fmt.Errorf("prepared selector original contract: %w", err)
	}
	data, err := json.Marshal(current.Parameters)
	if err != nil {
		return nil, err
	}
	snapshot, err := catalog.Clone()
	if err != nil {
		return nil, err
	}
	in, err := (x.Cloner{}).Type(input)
	if err != nil {
		return nil, err
	}
	out, err := (x.Cloner{}).Type(output)
	if err != nil {
		return nil, err
	}
	return &Selector{original: original.Clone(), parameters: data, catalog: snapshot, types: types, input: in, output: out}, nil
}
func (s *Selector) Validate(current *spec.Component) error {
	if s == nil || s.original == nil || s.catalog == nil || current == nil || current.SelectorCompanion != s.original.SelectorCompanion {
		return fmt.Errorf("prepared selector association changed")
	}
	data, err := json.Marshal(current.Parameters)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, s.parameters) {
		return fmt.Errorf("prepared selector declarations changed")
	}
	return nil
}
func (s *Selector) Catalog() (*typecatalog.Catalog, error) { return s.catalog.Clone() }
func (s *Selector) Contracts() (*x.Type, *x.Type, error) {
	in, err := (x.Cloner{}).Type(s.input)
	if err != nil {
		return nil, nil, err
	}
	out, err := (x.Cloner{}).Type(s.output)
	return in, out, err
}

// Types is the immutable original contract resolver already prepared at admission.
func (s *Selector) Types() *typecatalog.Resolver { return s.types }
