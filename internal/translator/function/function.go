package function

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"strconv"
)

type (
	Function interface {
		Name() string
		Description() string
		Signature
		Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error
	}

	Signature interface {
		Arguments() []*Argument
	}

	Argument struct {
		Name        string
		DataType    string
		Default     interface{}
		Required    bool
		Description string
	}
)

func convertArguments(signature Signature, args []string) ([]interface{}, error) {
	var result = make([]interface{}, 0, len(signature.Arguments()))
	for i, argument := range signature.Arguments() {
		arg := ""
		if i < len(args) {
			arg = args[i]
		}

		if arg == "" && argument.Default != nil {
			result = append(result, argument.Default)
			continue
		}

		if argument.Required && arg == "" {
			return nil, fmt.Errorf("%v is required", argument.Name)
		}

		switch argument.DataType {
		case "string":
			result = append(result, arg)
		case "int":
			v, err := strconv.Atoi(arg)
			if err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
			result = append(result, v)
		case "bool":
			v, err := strconv.ParseBool(arg)
			if err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
			result = append(result, v)

		case "float64":
			v, err := strconv.ParseFloat(arg, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
			result = append(result, v)

		default:
			return nil, fmt.Errorf("unsupported %v data type: %s", argument.Name, argument.DataType)
		}
	}
	return result, nil
}
