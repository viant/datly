package inference

import (
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/structql"
	"github.com/viant/toolbox/data"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"go/ast"
	"go/parser"
	"path"
	"reflect"
	"strings"
)

// State defines datly view/resource parameters
type State []*Parameter

// Append append parameter
func (s *State) Append(params ...*Parameter) bool {
	appended := false
	for i := range params {
		if s.Has(params[i].Name) {
			continue
		}
		params[i].adjustMetaViewIfNeeded()
		*s = append(*s, params[i])
		appended = true
	}
	return appended
}

func (s State) RemoveReserved() State {
	var result State
	for _, parameter := range s {
		if keywords.Has(parameter.Name) {
			continue
		}
		result = append(result, parameter)
	}
	return result
}

func (s State) ViewParameters() view.Parameters {
	var result = make([]*view.Parameter, 0, len(s))
	for i, _ := range s {
		parameter := &s[i].Parameter
		result = append(result, parameter)
	}
	return result
}

func (s State) Compact(modulePath string) (State, error) {
	if err := s.EnsureReflectTypes(modulePath); err != nil {
		return nil, err
	}
	var result = State{}
	var structs = make(map[string]*parameterStruct)
	for _, parameter := range s {
		if !strings.Contains(parameter.Name, ".") {
			result = append(result, parameter)
			continue
		}
		index := strings.Index(parameter.Name, ".")
		holder := parameter.Name[:index]
		child := parameter.Name[index+1:]
		if _, ok := structs[holder]; !ok {
			structs[holder] = newParameterStruct("")
		}
		structs[holder].Add(child, parameter)
	}

	for holder, pStruct := range structs {
		param := &Parameter{}
		param.Name = holder
		param.In = view.NewBodyLocation("")
		param.Schema = view.NewSchema(pStruct.reflectType())
		result = append(result, param)
	}
	return result, nil

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

// Has returns true if state already has a parameter
func (s State) Has(name string) bool {
	for _, candidate := range s {
		if candidate.Name == name {
			return true
		}
	}
	return false
}

// Lookup returns matched paramter
func (s State) Lookup(name string) *Parameter {
	for _, candidate := range s {
		if candidate.Name == name {
			return candidate
		}
	}
	return nil
}

// IndexByName indexes parameter by name
func (s State) IndexByName() map[string]*Parameter {
	result := map[string]*Parameter{}
	for _, parameter := range s {
		result[parameter.Name] = parameter
	}

	return result
}

// IndexByPathIndex indexes parameter by index variable
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

// FilterByKind filters state parameter by kind
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

func (s State) BodyField() string {
	body := s.FilterByKind(view.KindRequestBody)
	if len(body) == 0 {
		return ""
	}
	return body[0].In.Name
}

// Implicit filters implicit parameters
func (s State) Implicit() State {
	result := State{}
	for _, parameter := range s {
		if !parameter.Explicit {
			result.Append(parameter)
		}
	}
	return result
}

// Implicit filters implicit parameters
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
	text = expandPredicateExpr(text)
	return expander.ExpandAsText(text)
}

func expandPredicateExpr(query string) string {
	//TODO make it more generics
	indexStart := strings.Index(query, "${predicate.")
	if indexStart == -1 {
		return query
	}
	match := query[indexStart:]
	indexEnd := strings.Index(match, "}")
	match = match[:indexEnd+1]
	return strings.Replace(query, match, "  ", 1)
}

// DsqlParameterDeclaration returns dsql parameter declaration
func (s State) DsqlParameterDeclaration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

// ensureSchema initialises reflect.Type for each state parameter
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

// HandlerLocalVariables returns golang handler local variables reassigned from state
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
				return nil, fmt.Errorf("failed to detect parmater '%v' type for: %v  %w", param.Name, schema.DataType, err)
			}
		}
		param.Schema.Cardinality = schema.Cardinality
		if rType != nil {
			fields = append(fields, reflect.StructField{Name: param.Name, Type: rType, PkgPath: PkgPath(param.Name, pkgPath)})
		}
	}

	if len(fields) == 0 {
		return reflect.StructOf([]reflect.StructField{{Name: "Dummy", Type: reflect.TypeOf(true)}}), nil
	}
	baseType := reflect.StructOf(fields)
	return baseType, nil
}

func (s State) EnsureReflectTypes(modulePath string) error {
	typeRegistry := xreflect.NewTypes(xreflect.WithPackagePath(modulePath), xreflect.WithRegistry(config.Config.Types))
	for _, param := range s {
		if param.Schema == nil {
			continue
		}
		if param.Schema.Type() != nil {
			continue
		}
		if param.In.Kind == view.KindParam {
			sourceParam := s.Lookup(param.In.Name)
			if sourceParam == nil {
				return fmt.Errorf("failed to lookup queryql parameter: %v", param.In.Name)
			}
			query, err := structql.NewQuery(param.SQL, sourceParam.Schema.Type(), nil)
			if err != nil {
				return fmt.Errorf("failed to queryql param %v from %s(%s) due to: %w", param.Name, param.In.Name, sourceParam.Schema.Type().String(), err)
			}
			param.Schema = view.NewSchema(query.StructType())
			param.Schema.DataType = param.Name
			continue
		}
		dataType := param.Schema.Name
		if dataType == "" {
			dataType = param.Schema.DataType
		}

		if dataType == "" {
			dataType = "string"
			//			return fmt.Errorf("data type was emtpy for %v", param.Name)
		}
		rType, err := types.LookupType(typeRegistry.Lookup, dataType)
		if err != nil {
			return err
		}
		orig := param.Schema
		param.Schema = view.NewSchema(rType)
		param.Schema.Cardinality = orig.Cardinality
		param.Schema.DataType = orig.DataType
	}
	return nil
}

func (s State) MetaViewSQL() *Parameter {
	for _, candidate := range s {
		if strings.HasPrefix(candidate.Name, "View.") && strings.HasSuffix(candidate.Name, ".SQL") {
			return candidate
		}
	}
	return nil
}

func (s *State) SetLiterals(target interface{}) *types.Accessors {
	accessors := types.NewAccessors(&types.FieldNamer{})
	accessors.InitPath(reflect.TypeOf(target), "")
	ptr := xunsafe.AsPointer(target)
	for _, parameter := range s.FilterByKind(view.KindLiteral) {
		if accessor, _ := accessors.AccessorByName(parameter.Name); accessor != nil {
			accessor.SetValue(ptr, parameter.Const)
		}
	}
	return accessors
}

func (s *State) Group() State {
	newState := make(State, 0, len(*s))
	groupIndex := map[string][]*Parameter{}
	groupParams := s.FilterByKind(view.KindGroup)
	for _, param := range groupParams {
		baseParams := strings.Split(param.In.Name, ",")
		for _, baseParam := range baseParams {
			groupIndex[baseParam] = append(groupIndex[baseParam], param)
		}
	}

	for _, parameter := range *s {
		parameters, ok := groupIndex[parameter.Name]
		if !ok {
			newState.Append(parameter)
			continue
		}

		for _, parent := range parameters {
			parent.Group = append(parent.Group, &parameter.Parameter)
		}
	}

	return newState
}

// NewState creates a state from state go struct
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
