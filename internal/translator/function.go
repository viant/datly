package translator

import (
	"fmt"
	"github.com/viant/datly/internal/translator/function"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"reflect"
	"strings"
)

// TODO introduce function abstraction for datly -h list funciton, with validation signtaure description
func (n *Viewlets) applySettingFunctions(column *sqlparser.Column, namespace string) (bool, error) {
	funcName, funcArgs := extractFunction(column)
	if funcName == "" {
		return false, nil
	}

	funcName = strings.ReplaceAll(funcName, "_", "")

	if column.Namespace == "" && funcArgs[0] != "" {
		if strings.Contains(funcArgs[0], ".") {
			column.Namespace, column.Name = namespacedColumn(funcArgs[0])
		} else {
			if len(n.keys) == 1 && namespace != funcArgs[0] {
				column.Namespace = namespace
				column.Name = funcArgs[0]
			} else {
				if n.Lookup(column.Namespace) == nil {
					return false, nil
				}
				column.Namespace = funcArgs[0]
			}
		}
	}

	dest := n.Lookup(column.Namespace)
	fn := function.Lookup(funcName)
	if fn == nil {

		if dest != nil {
			switch strings.ToLower(funcName) {
			case "tag":
				if column.Name == column.Namespace && !strings.Contains(column.Tag, column.Name+"."+column.Name) {
					if dest.View == nil {
						dest.View = &View{}
					}
					dest.View.Tag = strings.Trim(column.Tag, "'")
					return true, nil
				}
				columnConfig := dest.columnConfig(column.Name)
				column.Tag = strings.Trim(strings.TrimSpace(column.Tag), "'")
				columnConfig.Tag = &column.Tag
				return true, nil
			case "cast":
				return dest.applyExplicitCast(column, funcArgs)
			case "required":
				column := dest.columnConfig(column.Name)
				required := true
				column.Required = &required
				return true, nil
			}
			column := dest.columnConfig(column.Name)
			column.Codec = &state.Codec{Name: funcName, Args: funcArgs[1:]}
			codec, err := extension.Config.Codecs.Lookup(funcName)
			if err != nil {
				return false, fmt.Errorf("unknown codec: %v at %v, %w", funcName, column.Name, err)
			}
			if rType, _ := codec.Instance.ResultType(reflect.TypeOf("")); rType != nil {
				column.Codec.OutputType = rType.String()
				column.Codec.Schema = state.NewSchema(rType)
			}

		}
		return false, nil
	}
	if fn != nil && dest == nil {
		return false, fmt.Errorf("invalid function %v namespace %v", funcName, column.Namespace)
	}
	if dest != nil {
		if err := fn.Apply(funcArgs[1:], column, &dest.Resource.Resource, &dest.View.View); err != nil {
			return false, fmt.Errorf("failed to execute dql function: '%s', %w", funcName, err)
		}
	}
	if dest.View != nil && dest.View.View.Connector != nil {
		dest.Connector = dest.View.View.Connector.Ref
	}
	return true, nil
}

func (v *Viewlet) applyExplicitCast(column *sqlparser.Column, funcArgs []string) (bool, error) {
	if column.Name == "" || column.Name == column.Namespace {
		if v.View.Schema == nil {
			v.View.Schema = &state.Schema{}
		}
		v.View.Schema.Name = funcArgs[1]
		v.View.Schema.DataType = "*" + funcArgs[1]
		if !strings.Contains(v.View.Schema.Name, ".") {
			if pkg, ok := v.Resource.typePackages[funcArgs[1]]; ok {
				v.View.Schema.Package = pkg
			}
		}
		if v.View.Schema.Cardinality == "" {
			v.View.Schema.Cardinality = state.Many
		}
		return true, nil
	}
	columnConfig := v.columnConfig(column.Name)
	columnConfig.DataType = &funcArgs[1]
	column.Type = funcArgs[1]
	rType, err := types.LookupType(v.Resource.typeRegistry.Lookup, column.Type)
	if err != nil {
		return false, fmt.Errorf("unknown column %v type: %s, %w", column.Name, column.Type, err)
	}
	column.RawType = rType
	return true, nil
}
