package function

import (
	"fmt"
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type allowedOrderByColumns struct{}

func (c *allowedOrderByColumns) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
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
	if len(values) == 0 {
		return fmt.Errorf("failed to discover expression in allowedOrderByColumns")
	}
	columns, ok := values[0].(string)
	if !ok {
		return fmt.Errorf("invalid columns type: %T, expected: %T in allowedOrderByColumns", values[0], columns)
	}
	if len(aView.Selector.Constraints.OrderByColumn) == 0 {
		aView.Selector.Constraints.OrderByColumn = map[string]string{}
	}
	for _, expression := range strings.Split(columns, ",") {
		expression = strings.TrimSpace(expression)

		key := expression
		value := expression
		if strings.Contains(expression, ":") {
			parts := strings.SplitN(expression, ":", 2)
			key = parts[0]
			value = parts[1]
		}

		aView.Selector.Constraints.OrderByColumn[key] = value
		lcKey := strings.ToLower(key)
		if lcKey != key {
			aView.Selector.Constraints.OrderByColumn[lcKey] = value
		}

		if index := strings.Index(key, "."); index != -1 {
			aView.Selector.Constraints.OrderByColumn[key[index+1:]] = value
		}
	}
	return nil
}

func (c *allowedOrderByColumns) Name() string {
	return "allowed_order_by_columns"
}

func (c *allowedOrderByColumns) Description() string {
	return "set view.Selector.OrderBy and enables corresponding view.Selector.Constraints.OrderBy"
}

func (c *allowedOrderByColumns) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "allowedOrderByColumns",
			Description: "query selector allowedOrderByColumns",
			Required:    true,
			DataType:    "string",
		},
	}
}
