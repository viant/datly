package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type matchStrategy struct{}

func (c *matchStrategy) Description() string {
	return "set view.Schema.matchStrategy matchStrategy, default is Many"
}

func (c *matchStrategy) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	aView.MatchStrategy = view.MatchStrategy(args[0])
	return nil
}

func (c *matchStrategy) Name() string {
	return "matchstrategy"
}

func (c *matchStrategy) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "strategy",
			Description: "strategy",
			Required:    true,
			DataType:    "string",
		},
	}
}
