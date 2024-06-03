package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type partitioner struct{}

func (c *partitioner) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if aView.Selector == nil {
		aView.Selector = &view.Config{}
	}
	values, err := convertArguments(c, args)
	if err != nil {
		return err
	}

	partitionrType := values[0].(string)
	if aView.Partitioned == nil {
		aView.Partitioned = &view.Partitioned{}
	}
	aView.Partitioned.DataType = partitionrType
	if len(values) > 1 {
		aView.Partitioned.Concurrency = values[1].(int)
	}
	return nil
}

func (c *partitioner) Name() string {
	return "set_partitioner"
}

func (c *partitioner) Description() string {
	return "set view.Partitioned with partitioner type and concurrency"
}

func (c *partitioner) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "partitionerType",
			Description: "partitioner data type or . for a view type",
			Required:    true,
			DataType:    "string",
		},
		{
			Name:        "partitionerConcurrecny",
			Description: "partitioner concurrency",
			Required:    false,
			DataType:    "int",
		},
	}
}
