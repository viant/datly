package translator

import (
	"github.com/viant/datly/internal/translator/function"
	"github.com/viant/sqlparser"
	"strings"
)

// TODO introduce function abstraction for datly -h list funciton, with validation signtaure description
func (n *Viewlets) applySettingFunctions(column *sqlparser.Column) (bool, error) {
	funcName, funcArgs := extractFunction(column)
	funcName = strings.ReplaceAll(funcName, "_", "")

	fn := function.Lookup(funcName)
	if fn == nil {
		return false, nil
	}
	column.Namespace = funcArgs[0]
	dest := n.Lookup(column.Namespace)
	if err := fn.Apply(funcArgs[1:], column, &dest.Resource.Resource, &dest.View.View); err != nil {
		return false, err
	}
	return true, nil

	//switch strings.ToLower(funcName) {
	//case "useconnector":
	//	dest.Connector = funcArgs[1]
	//case "usecacheref":
	//	column.Namespace = funcArgs[0]
	//	dest := n.Lookup(column.Namespace)
	//	dest.View.Cache = view.NewRefCache(funcArgs[1])
	//case "cardinality":
	//	column.Namespace = funcArgs[0]
	//	dest := n.Lookup(column.Namespace)
	//	if dest.View.Schema == nil {
	//		dest.View.Schema = &state.Schema{}
	//	}
	//	dest.View.Schema.Cardinality = state.Cardinality(funcArgs[1])
	//case "limit":
	//	column.Namespace = funcArgs[0]
	//	dest := n.Lookup(column.Namespace)
	//	if dest.View.Selector == nil {
	//		dest.View.Selector = &view.Config{}
	//	}
	//	limit, err := strconv.Atoi(funcArgs[1])
	//	if err != nil {
	//		return false, err
	//	}
	//	if dest.View.Selector.Constraints == nil {
	//		dest.View.Selector.Constraints = &view.Constraints{}
	//	}
	//	dest.View.Selector.Constraints.Limit = true
	//	dest.View.Selector.Limit = limit
	//
	//case "allownulls":
	//	column.Namespace = funcArgs[0]
	//	dest := n.Lookup(column.Namespace)
	//	flag := true
	//	if len(funcArgs) == 2 {
	//		flag, _ = strconv.ParseBool(funcArgs[1])
	//	}
	//	dest.View.AllowNulls = &flag
	//default:
	//	return false, nil
	//}
	//return true, nil
}
