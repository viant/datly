package data

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/data/io"
	"github.com/viant/datly/generic"
	"github.com/viant/datly/shared"
)

//IO represents data input/output
type IO struct {
	DataView    string `json:",omitempty"`
	Key         string `json:",omitempty"`
	CaseFormat  string `json:",omitempty"`
	Cardinality string `json:",omitempty"`
}

//SetOutput sets output with specified cardinality
func (o IO) SetOutput(collection generic.Collection, output io.Output) {
	switch o.Cardinality {
	case shared.CardinalityOne:
		output.Put(o.Key, collection.First())
	default:
		output.Put(o.Key, collection)
	}
}

//Validate check if output is valid
func (o IO) Validate() error {
	if o.DataView == "" {
		return fmt.Errorf("dataView was empty")
	}
	if o.CaseFormat != "" {
		if err := generic.ValidateCaseFormat(o.CaseFormat); err != nil {
			return errors.Wrapf(err, "invalid case format for data view: %v", o.DataView)
		}
	}
	if o.Cardinality != "" {
		if err := ValidateCardinality(o.Cardinality); err != nil {
			return err
		}
	}
	return nil
}

//Init initialises Input
func (o *IO) Init() {
	if o.DataView != "" && o.Key == "" {
		o.Key = o.DataView
	}
	if o.Cardinality == "" {
		o.Cardinality = shared.CardinalityMany
	}
}
