package data

import (
	"datly/generic"
	"fmt"
	"github.com/pkg/errors"
)

//Output data output
type Output struct {
	DataView string `json:",omitempty"`
	Key string `json:",omitempty"`
	CaseFormat string `json:",omitempty"`
}

//Validate check if output is valid
func (o Output) Validate() error {
	if o.DataView == "" {
		return fmt.Errorf("outout.dataView was empty")
	}
	if o.CaseFormat != "" {
		if err := generic.ValidateCaseFormat(o.CaseFormat); err != nil {
			return errors.Wrapf(err, "invalid output: %v", o.DataView)
		}
	}
	return nil
}

//Init initialises output
func (o *Output) Init() {
	if o.DataView != "" && o.Key == "" {
		o.Key = o.DataView
	}
}
