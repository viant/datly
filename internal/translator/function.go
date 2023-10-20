package translator

import (
	"fmt"
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
	if column.Namespace == "" {
		column.Namespace = funcArgs[0]
	}
	dest := n.Lookup(column.Namespace)
	if fn != nil && dest == nil {
		return false, fmt.Errorf("invalida function %v namespace %v", funcName, column.Namespace)
	}
	if dest != nil {
		if err := fn.Apply(funcArgs[1:], column, &dest.Resource.Resource, &dest.View.View); err != nil {
			return false, fmt.Errorf("failed to execute dql function: '%s', %w", funcName, err)
		}
	}
	return true, nil
}
