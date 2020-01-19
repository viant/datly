package data

import "fmt"

//Output data output
type Output struct {
	DataView string `json:",omitempty"`
	Key string `json:",omitempty"`
}

func (o Output) Validate() error {
	if o.DataView == "" {
		return fmt.Errorf("outout.dataView was empty")
	}
	return nil
}

func (o *Output) Init() {
	if o.DataView != "" && o.Key == "" {
		o.Key = o.DataView
	}
}
