package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type orderBy struct{}

func (c *orderBy) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if aView.Selector == nil {
		aView.Selector = &view.Config{}
	}
	values, err := convertArguments(c, args)
	if err != nil {
		return err
	}
	if aView.Selector.Constraints == nil {
		aView.Selector.Constraints = &view.Constraints{}
	}
	aView.Selector.Constraints.OrderBy = true
	aView.Selector.OrderBy = values[0].(string)
	return nil
}

func (c *orderBy) Name() string {
	return "order_by"
}

func (c *orderBy) Description() string {
	return "set view.Selector.OrderBy and enables corresponding view.Selector.Constraints.OrderBy"
}

func (c *orderBy) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "orderBy",
			Description: "query selector orderBy",
			Required:    true,
			DataType:    "string",
		},
	}
}
