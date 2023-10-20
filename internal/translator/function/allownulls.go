package function

import (
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type allownulls struct{}

func (c *allownulls) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	values, err := convertArguments(c, args)
	if err != nil {
		return err
	}
	flag := values[0].(bool)
	aView.AllowNulls = &flag
	return nil
}

func (c *allownulls) Name() string {
	return "allow_nulls"
}

func (c *allownulls) Description() string {
	return "sets view.AllowNulls flag, when flag is set, SQL builder adds COALESCE expression to nullable columns"
}

func (c *allownulls) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "flag",
			Description: "allow null flag",
			Required:    true,
			Default:     true,
			DataType:    "bool",
		},
	}
}

/*
	column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		flag := true
		if len(funcArgs) == 2 {
			flag, _ = strconv.ParseBool(funcArgs[1])
		}
		dest.View.AllowNulls = &flag
*/
