package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type groupable struct{}

func (c *groupable) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	values, err := convertArguments(c, args)
	if err != nil {
		return err
	}
	aView.Groupable = values[0].(bool)
	return nil
}

func (c *groupable) Name() string {
	return "groupable"
}

func (c *groupable) Description() string {
	return "sets view.Groupable flag to enable dynamic group by rewriting for the view"
}

func (c *groupable) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "flag",
			Description: "enable dynamic group by for the view",
			Required:    false,
			Default:     true,
			DataType:    "bool",
		},
	}
}
