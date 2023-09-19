package inference

import (
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/structql"
	"github.com/viant/toolbox/data"
	"github.com/viant/xreflect"
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

func (s State) ViewParameters() state.Parameters {
	var result = make([]*state.Parameter, 0, len(s))
	for i, _ := range s {
		parameter := &s[i].Parameter
		if len(s[i].Group) > 0 {
			parameter.Group = nil
			for j := range s[i].Group {
				parameter.Group = append(parameter.Group, &s[i].Group[j].Parameter)
			}
		}
		if len(s[i].Repeated) > 0 {
			parameter.Repeated = nil
			for j := range s[i].Repeated {
				parameter.Repeated = append(parameter.Repeated, &s[i].Repeated[j].Parameter)
			}
		}
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
		param.In = state.NewBodyLocation("")
		param.Schema = state.NewSchema(pStruct.reflectType())
		result = append(result, param)
	}
	return result, nil

}

func (s *State) AppendViewParameters(params ...*state.Parameter) {
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
func (s State) FilterByKind(kind state.Kind) State {
	result := State{}
	if len(s) == 0 {
		return result
	}
	for _, parameter := range s {
		switch parameter.In.Kind {
		case kind:
			result.Append(parameter)
		case state.KindRepeated:
			for _, candidate := range parameter.Repeated {
				if candidate.In.Kind == kind {
					result.Append(candidate)
				}
			}
		case state.KindGroup:
			for _, candidate := range parameter.Group {
				if candidate.In.Kind == kind {
					result.Append(candidate)
				}
			}
		}
	}
	return result
}

func (s State) BodyField() string {
	body := s.FilterByKind(state.KindRequestBody)
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
	if parameters := s.FilterByKind(state.KindLiteral); len(parameters) > 0 {
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
	query = strings.Replace(query, match, "  ", 1)
	if !strings.Contains(query, "${predicate.") {
		return query
	}
	return expandPredicateExpr(query)
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
		param.Schema = state.NewSchema(paramType)
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
		if p.Parameter.In.Kind == state.KindParam || p.IsAuxiliary {
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
		if param.In.Kind == state.KindParam {
			sourceParam := s.Lookup(param.In.Name)
			if sourceParam == nil {
				return fmt.Errorf("failed to lookup queryql parameter: %v", param.In.Name)
			}
			query, err := structql.NewQuery(param.SQL, sourceParam.Schema.Type(), nil)
			if err != nil {
				return fmt.Errorf("failed to queryql param %v from %s(%s) due to: %w", param.Name, param.In.Name, sourceParam.Schema.Type().String(), err)
			}
			param.Schema = state.NewSchema(query.StructType())
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
		param.Schema = state.NewSchema(rType)
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

// Normalize normalizes state
func (s State) Normalize() (State, error) {
	ret := s.Group()
	return ret.Repeated()
}

func (s *State) Group() State {
	newState := make(State, 0, len(*s))
	groupIndex := map[string][]*Parameter{}
	groupParams := s.FilterByKind(state.KindGroup)
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
			parent.Parameter.Group = append(parent.Parameter.Group, &parameter.Parameter)
			parent.Group = append(parent.Group, parameter)
		}
	}

	return newState
}

func (s State) Repeated() (State, error) {
	sliceParameters := s.FilterByKind(state.KindRepeated)
	if len(sliceParameters) == 0 {
		return s, nil
	}
	sliceParameter := map[string]state.Parameters{}
	byName := s.IndexByName()
	for _, param := range sliceParameters {
		sliceParameter[param.Name] = state.Parameters{}
		if param.In.Name == "" {
			continue
		}
		baseParameters := strings.Split(param.In.Name, ",")
		for _, name := range baseParameters {
			baseParameter, ok := byName[strings.TrimSpace(name)]
			if !ok {
				return nil, fmt.Errorf("unknwon slice base paramter: %s", name)
			}
			baseParameter.Of = param.Name
		}
	}

	result := State{}
	var repeatedName []string
	for i, parameter := range s {
		if parameter.Of == "" {
			result = append(result, s[i])
			continue
		}
		parent, ok := byName[parameter.Of]
		if !ok {
			return nil, fmt.Errorf("unkown parent parameter %v, base: %v", parameter.Of, parameter.Name)
		}
		repeatedName = append(repeatedName, parameter.Name)
		parent.Parameter.Repeated = append(parent.Parameter.Repeated, &parameter.Parameter)
		parent.Repeated = append(parent.Repeated, parameter)
		parent.Parameter.In.Name = strings.Join(repeatedName, ",")
	}
	return result, nil
}

func (s *State) AdjustOutput() error {
	outputParameter := s.GetOutputParameter()
	if outputParameter == nil {
		return nil
	}
	predefined := s.IndexByName()
	unique := map[string]bool{}
	outputType := outputParameter.Schema.Type()
	if outputType == nil {
		return fmt.Errorf("invalid output type - missing schema type")
	}
	sType := structology.NewStateType(outputType)
	outputParameters := sType.MatchByTag(state.TagName)

	var adjusted State
	var err error

	for _, parameterField := range outputParameters {
		var parent *Parameter
		name := parameterField.Path()
		if unique[name] {
			continue
		}
		unique[name] = true
		if prev := predefined[name]; prev != nil {
			adjusted = append(adjusted, prev)
			continue
		}

		if index := strings.LastIndex(name, "."); index != -1 {
			parentName := name[:index]
			parent = predefined[parentName]
			if parent == nil {
				parentField := sType.Lookup(parentName)
				if parent, err = s.selectorParameter(parentField); err != nil {
					return fmt.Errorf("failed to expand output type: %w", err)
				}
				parent.In = state.NewGroupLocation("")
				predefined[parentName] = parent
				if parentField.IsAnonymous() {
					parent.Tag += ` anonymous:"true"`
				}
				parent.Schema = state.NewSchema(xreflect.InterfaceType)
				adjusted = append(adjusted, parent)
			}

			itemParameter, err := s.selectorParameter(parameterField)
			if err != nil {
				return fmt.Errorf("failed to expand output group type: %w", err)
			}
			parent.Group = append(parent.Group, itemParameter)
			var items []string
			for _, item := range parent.Group {
				items = append(items, item.Name)
			}
			parent.In.Name = strings.Join(items, ",`")
			continue
		}

		stateParameter, err := s.selectorParameter(parameterField)
		if err != nil {
			return fmt.Errorf("failed to expand output group type: %w", err)
		}
		predefined[name] = stateParameter
		adjusted = append(adjusted, stateParameter)
	}
	*s = adjusted
	return nil
}

func (s *State) selectorParameter(parameterField *structology.Selector) (*Parameter, error) {
	tag := string(parameterField.Tag())
	structField := &reflect.StructField{Name: parameterField.Name(), Tag: reflect.StructTag(tag), Type: parameterField.Type()}
	stateParameter, err := state.BuildParameter(structField, nil)
	return &Parameter{Parameter: *stateParameter}, err
}

func (s *State) GetOutputParameter() *Parameter {
	for _, candidate := range *s {
		if candidate.In.Kind == state.KindOutput && candidate.In.Name == "" {
			return candidate
		}
	}
	return nil
}

// NewState creates a state from state go struct
func NewState(modulePath, dataType string, types *xreflect.Types) (State, error) {
	baseDir := modulePath
	if pair := strings.Split(dataType, "."); len(pair) > 1 {
		baseDir = path.Join(baseDir, pair[0])
		dataType = pair[1]
	}

	var aState = State{}
	dirTypes, err := xreflect.ParseTypes(baseDir,
		xreflect.WithParserMode(parser.ParseComments),
		xreflect.WithRegistry(types),
		xreflect.WithOnField(func(typeName string, field *ast.Field) error {
			if field.Tag == nil {
				return nil
			}
			fieldTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			datlyTag, _ := fieldTag.Lookup(view.DatlyTag)
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
			state.BuildPredicate(fieldTag, &param.Parameter)
			aState.Append(param)
			return nil
		}))

	if err != nil {
		return nil, err
	}
	if _, err = dirTypes.Type(dataType); err != nil {
		return nil, err
	}
	if err = aState.ensureSchema(dirTypes); err != nil {
		return nil, err
	}
	return aState, nil
}
