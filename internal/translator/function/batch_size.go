package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"strconv"
)

type batchSize struct{}

func (c *batchSize) Description() string {
	return "set view.Batch.Size batchSize, default is 1000"
}

func (c *batchSize) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	if aView.Batch == nil {
		aView.Batch = &view.Batch{}
	}
	var err error
	aView.Batch.Size, err = strconv.Atoi(args[0])
	return err
}

func (c *batchSize) Name() string {
	return "batchSize"
}

func (c *batchSize) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "strategy",
			Description: "strategy",
			Required:    true,
			DataType:    "string",
		},
	}
}
