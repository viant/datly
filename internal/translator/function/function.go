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
	var result = make([]interface{}, len(args))
	var err error
	for i, argument := range signature.Arguments() {
		arg := ""
		if i < len(args) {
			arg = args[i]
		}
		if !argument.Required && arg == "" {
			return nil, fmt.Errorf("%v is required", argument.Name)
		}

		switch argument.DataType {
		case "string":
			result[i] = arg
		case "int":
			if arg == "" {
				result[i] = argument.Default
				continue
			}
			if result[i], err = strconv.Atoi(arg); err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
		case "bool":
			if arg == "" {
				result[i] = argument.Default
				continue
			}
			if result[i], err = strconv.ParseBool(arg); err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
		case "float64":
			if arg == "" {
				result[i] = argument.Default
				continue
			}
			if result[i], err = strconv.ParseFloat(arg, 64); err != nil {
				return nil, fmt.Errorf("invalid %v data type, expeceted %s, %w", argument.Name, argument.DataType, err)
			}
		default:
			return nil, fmt.Errorf("unsupported %v data type", argument.Name, argument.DataType)
		}
	}
	return result, nil
}
