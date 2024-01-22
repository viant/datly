package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type fetch struct{}

func (c *fetch) Description() string {
	return "set view.Schema.fetch fetch, default is Many"
}

func (c *fetch) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	aView.MatchStrategy = view.MatchStrategy(args[0])
	return nil
}

func (c *fetch) Name() string {
	return "fetch_strategy"
}

func (c *fetch) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "strategy",
			Description: "strategy",
			Required:    true,
			DataType:    "string",
		},
	}
}
