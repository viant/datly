package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type cache struct{}

func (c *cache) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	aView.Cache = view.NewRefCache(args[0])
	return nil
}

func (c *cache) Name() string {
	return "use_cache"
}

func (c *cache) Description() string {
	return "set view.Cache reference"
}

func (c *cache) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "cacheReference",
			Description: "cache reference",
			Required:    true,
			DataType:    "string",
		},
	}
}
