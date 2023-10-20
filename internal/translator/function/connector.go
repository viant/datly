package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type connector struct{}

func (c *connector) Description() string {
	return "set view.Connector connector reference"
}

func (c *connector) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	aView.Connector = view.NewRefConnector(args[0])
	return nil
}

func (c *connector) Name() string {
	return "use_connector"
}

func (c *connector) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "connectorReference",
			Description: "connector reference",
			Required:    true,
			DataType:    "string",
		},
	}
}
