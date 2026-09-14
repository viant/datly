package provider

import (
	"fmt"

	"github.com/viant/datly/sql/reader/readmeta"
	xhandler "github.com/viant/xdatly/handler"
)

// readProjection exposes only immutable field evidence, not reader internals.
type readProjection struct{ result *readmeta.Result }

func (p *readProjection) RootHolder() string {
	if p == nil {
		return ""
	}
	return p.result.RootHolder()
}
func (p *readProjection) DirectOutput() bool { return p != nil && p.result.DirectOutput() }
func (p *readProjection) Fields(rootOrdinal int, relations ...xhandler.ReadStep) (xhandler.FieldSet, error) {
	if p == nil || p.result == nil {
		return nil, fmt.Errorf("read projection is unavailable")
	}
	row, err := p.result.Row(rootOrdinal)
	if err != nil {
		return nil, err
	}
	for _, step := range relations {
		row, err = row.RelationRow(step.Holder, step.Index)
		if err != nil {
			return nil, err
		}
	}
	fields := row.Fields()
	if !fields.Known() {
		return nil, fmt.Errorf("read field provenance is unknown")
	}
	return fields, nil
}

var _ xhandler.ReadProjection = (*readProjection)(nil)
var _ xhandler.ReadOutputProjection = (*readProjection)(nil)

func (p *readProjection) Output(holder string) (xhandler.ReadProjection, error) {
	if p == nil || p.result == nil {
		return nil, fmt.Errorf("read projection is unavailable")
	}
	result, err := p.result.Output(holder)
	if err != nil {
		return nil, err
	}
	return &readProjection{result: result}, nil
}
