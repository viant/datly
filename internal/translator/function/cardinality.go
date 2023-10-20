package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
)

type cardinality struct{}

func (c *cardinality) Description() string {
	return "set view.Schema.Cardinality cardinality, default is Many"
}

func (c *cardinality) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	if aView.Schema == nil {
		aView.Schema = &state.Schema{}
	}
	aView.Schema.Cardinality = state.Cardinality(args[0])
	return nil
}

func (c *cardinality) Name() string {
	return "cardinality"
}

func (c *cardinality) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "cardinality",
			Description: "One or Many cardinality",
			Required:    true,
			DataType:    "string",
		},
	}
}
