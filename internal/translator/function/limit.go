package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type limit struct{}

func (c *limit) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if aView.Selector == nil {
		aView.Selector = &view.Config{}
	}
	values, err := convertArguments(c, args)
	if err != nil {
		return err
	}
	aLimit := values[0].(int)
	if aView.Selector.Constraints == nil {
		aView.Selector.Constraints = &view.Constraints{}
	}
	aView.Selector.Constraints.Limit = true
	aView.Selector.Limit = aLimit
	return nil
}

func (c *limit) Name() string {
	return "set_limit"
}

func (c *limit) Description() string {
	return "set view.Selector.Limit and enables corresponding view.Selector.Constraints.Limit"
}

func (c *limit) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "queryLimit",
			Description: "query selector limit",
			Required:    true,
			DataType:    "int",
		},
	}
}
