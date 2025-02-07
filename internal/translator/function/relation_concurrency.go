package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"strconv"
)

type relationalConcurrency struct{}

func (c *relationalConcurrency) Description() string {
	return "set on relational concurrency, default is 1"
}

func (c *relationalConcurrency) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	if aView.RelationalConcurrency == nil {
		aView.RelationalConcurrency = &view.RelationalConcurrency{}
	}
	var err error
	aView.RelationalConcurrency.Number, err = strconv.Atoi(args[0])
	return err
}

func (c *relationalConcurrency) Name() string {
	return "relational_concurrency"
}

func (c *relationalConcurrency) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "strategy",
			Description: "strategy",
			Required:    true,
			DataType:    "string",
		},
	}
}
