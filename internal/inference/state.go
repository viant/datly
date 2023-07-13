package inference

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/data"
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
func (s *State) Append(params ...*Parameter) {
	for i := range params {
		if s.Has(params[i].Name) {
			continue
		}
		*s = append(*s, params[i])
	}
}

func (s State) ViewParameters() []*view.Parameter {
	var result = make([]*view.Parameter, 0, len(s))
	for i := range s {
		result = append(result, &s[i].Parameter)
	}
	return result
}

func (s *State) AppendViewParameters(params ...*view.Parameter) {
	for i := range params {
		if s.Has(params[i].Name) {
			continue
		}
		*s = append(*s, &Parameter{Parameter: *params[i], Explicit: true})
	}
}
func (s *State) AppendConstants(constants map[string]interface{}) {
	for paramName, paramValue := range constants {
		s.Append(NewConstParameter(paramName, paramValue))
	}
}

func (s *State) StateForSQL(SQL string, isRoot bool) State {
	var result = State{}
	for _, candidate := range *s {
		if (isRoot && candidate.Explicit) || candidate.IsUsedBy(SQL) {
			result = append(result, candidate)
		}
	}
	return result
}

func (s State) Clone() State {
	var result = make(State, len(s))
	copy(result, s)
	return result
}

//Has returns true if state already has a parameter
func (s State) Has(name string) bool {
	for _, candidate := range s {
		if candidate.Name == name {
			return true
		}
	}
	return false
}

//Lookup returns matched paramter
func (s State) Lookup(name string) *Parameter {
	for _, candidate := range s {
		if candidate.Name == name {
			return candidate
		}
	}
	return nil
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
	if len(s) == 0 {
		return result
	}
	for _, parameter := range s {
		if parameter.In.Kind == kind {
			result.Append(parameter)
		}
	}
	return result
}

//Implicit filters implicit parameters
func (s State) Implicit() State {
	result := State{}
	for _, parameter := range s {
		if !parameter.Explicit {
			result.Append(parameter)
		}
	}
	return result
}

//Implicit filters implicit parameters
func (s State) Explicit() State {
	result := State{}
	for _, parameter := range s {
		if parameter.Explicit {
			result.Append(parameter)
		}
	}
	return result
}

func (s State) Expand(text string) string {
	expander := data.Map{}
	if parameters := s.FilterByKind(view.KindLiteral); len(parameters) > 0 {
		for _, literal := range parameters {
			expander[literal.Name] = literal.Const
		}
	}
	return expander.ExpandAsText(text)
}

//DsqlParameterDeclaration returns dsql parameter declaration
func (s State) DsqlParameterDeclaration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

//EnsureSchema initialises reflect.Type for each state parameter
func (s State) EnsureSchema(dirTypes *xreflect.DirTypes) error {
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

func (s State) ReflectType(pkgPath string, lookupType xreflect.LookupType) (reflect.Type, error) {
	var fields []reflect.StructField
	var err error
	for _, param := range s {
		schema := param.OutputSchema()
		if schema == nil {
			return nil, fmt.Errorf("invalid parameter: %v schema was empty", param.Name)
		}
		if schema.DataType == "" && param.DataType != "" {
			schema.DataType = param.DataType
		}
		rType := schema.Type()
		if rType == nil {
			if rType, err = types.LookupType(lookupType, schema.DataType); err != nil {
				return nil, fmt.Errorf("failed to detect parmater '%v' type for: %v %v, %w", param.Name, schema.DataType, err)
			}
		}
		param.Schema.Cardinality = schema.Cardinality
		if rType != nil {
			fields = append(fields, reflect.StructField{Name: param.Name, Type: rType, PkgPath: PkgPath(param.Name, pkgPath)})
		}
	}

	if len(fields) == 0 {
		return reflect.TypeOf(struct{}{}), nil
	}
	baseType := reflect.StructOf(fields)
	return baseType, nil
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
	if err = state.EnsureSchema(dirTypes); err != nil {
		return nil, err
	}
	return state, nil
}
