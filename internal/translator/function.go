package translator

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"strconv"
	"strings"
)

type (
	function interface {
		apply(args []string, viewlets *Viewlets) error
	}

	functionRegistry map[string]function
)

// TODO introduce function abstraction for datly -h list funciton, with validation signtaure description
func (n *Viewlets) applySettingFunctions(column *sqlparser.Column) (bool, error) {
	funcName, funcArgs := extractFunction(column)
	funcName = strings.ReplaceAll(funcName, "_", "")
	switch strings.ToLower(funcName) {
	case "useconnector":
		column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		dest.Connector = funcArgs[1]
	case "usecacheref":
		column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		dest.View.Cache = view.NewRefCache(funcArgs[1])
	case "cardinality":
		column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		if dest.View.Schema == nil {
			dest.View.Schema = &state.Schema{}
		}
		dest.View.Schema.Cardinality = state.Cardinality(funcArgs[1])
	case "limit":
		column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		if dest.View.Selector == nil {
			dest.View.Selector = &view.Config{}
		}
		limit, err := strconv.Atoi(funcArgs[1])
		if err != nil {
			return false, err
		}
		if dest.View.Selector.Constraints == nil {
			dest.View.Selector.Constraints = &view.Constraints{}
		}
		dest.View.Selector.Constraints.Limit = true
		dest.View.Selector.Limit = limit

	case "allownulls":
		column.Namespace = funcArgs[0]
		dest := n.Lookup(column.Namespace)
		flag := true
		if len(funcArgs) == 2 {
			flag, _ = strconv.ParseBool(funcArgs[1])
		}
		dest.View.AllowNulls = &flag
	default:
		return false, nil
	}
	return true, nil
}