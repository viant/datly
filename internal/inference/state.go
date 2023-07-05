package inference

import (
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"go/ast"
	"go/parser"
	"path"
	"reflect"
	"strings"
)

//State defines datly view/resource parameters
type State []*Parameter

//Append append parameter
func (s *State) Append(param ...*Parameter) {
	*s = append(*s, param...)
}

//IndexByName indexes parameter by name
func (s State) IndexByName() map[string]*Parameter {
	result := map[string]*Parameter{}
	for _, parameter := range s {
		result[parameter.Name] = parameter
	}

	return result
}

//IndexByPathIndex indexes parameter by index variable
func (s State) IndexByPathIndex() map[string]*Parameter {
	result := map[string]*Parameter{}
	for _, parameter := range s {
		if parameter.PathParam == nil {
			continue
		}
		result[parameter.IndexVariable()] = parameter
	}

	return result
}

//FilterByKind filters state parameter by kind
func (s State) FilterByKind(kind view.Kind) State {
	result := State{}
	for _, parameter := range s {
		if parameter.In.Kind == kind {
			result.Append(parameter)
		}
	}
	return result
}

//DsqlParameterDeclaration returns dsql parameter declaration
func (s State) DsqlParameterDeclaration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

func (s State) ensureSchema(dirTypes *xreflect.DirTypes) error {
	for _, param := range s {
		if param.Schema.Type() != nil {
			continue
		}
		paramDataType := param.Schema.DataType
		paramType, err := xreflect.Parse(paramDataType, xreflect.WithTypeLookup(func(name string, option ...xreflect.Option) (reflect.Type, error) {
			return dirTypes.Type(name)
		}))
		if err != nil {
			return fmt.Errorf("invalid parameter '%v' schema: '%v'  %w", param.Name, param.Schema.DataType, err)
		}

		oldSchema := param.Schema
		param.Schema = view.NewSchema(paramType)
		param.Schema.DataType = paramDataType

		if oldSchema != nil {
			param.Schema.Cardinality = oldSchema.Cardinality
		}
	}
	return nil
}

//HandlerLocalVariables returns golang handler local variables reassigned from state
func (s State) HandlerLocalVariables() ([]string, string) {
	var vars []string
	var names []string
	for _, p := range s {
		if p.Parameter.In.Kind == view.KindParam || p.IsAuxiliary {
			continue
		}
		fieldName, definition := p.localVariableDefinition()
		names = append(names, fieldName)
		vars = append(vars, "\t"+definition)
	}
	return names, strings.Join(vars, "\n")
}

//NewState creates a state from state go struct
func NewState(modulePath, dataType string, types *xreflect.Types) (State, error) {
	baseDir := modulePath
	if pair := strings.Split(dataType, "."); len(pair) > 1 {
		baseDir = path.Join(baseDir, pair[0])
		dataType = pair[1]
	}

	var state = State{}
	dirTypes, err := xreflect.ParseTypes(baseDir,
		xreflect.WithParserMode(parser.ParseComments),
		xreflect.WithRegistry(types),
		xreflect.WithOnField(func(typeName string, field *ast.Field) error {
			if field.Tag == nil {
				return nil
			}
			datlyTag, _ := reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Lookup(view.DatlyTag)
			if datlyTag == "" {
				return nil
			}
			tag := view.ParseTag(datlyTag)
			if tag.Kind == "" {
				return nil
			}
			param, err := buildParameter(field, types)
			if param == nil {
				return err
			}
			state.Append(param)
			return nil
		}))

	if err != nil {
		return nil, err
	}
	if _, err = dirTypes.Type(dataType); err != nil {
		return nil, err
	}
	if err = state.ensureSchema(dirTypes); err != nil {
		return nil, err
	}
	return state, nil
}
