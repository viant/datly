package inference

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/embed"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
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
		candidate := params[i]
		if s.Has(candidate.Name) {
			prev := s.Lookup(candidate.Name)
			if prev.Value != nil && candidate.Value == nil {
				candidate.Value = prev.Value
			} else if prev.Value == nil && candidate.Value != nil {
				prev.Value = candidate.Value
			}

			if prev.Required == nil {
				prev.Required = candidate.Required
			}
			continue
		}
		params[i].adjustMetaViewIfNeeded()
		*s = append(*s, params[i])
		appended = true
	}
	return appended
}

func (s *State) AppendParameters(parameters ...state.Parameters) {
	for _, params := range parameters {
		for _, param := range params {
			s.Append(&Parameter{Parameter: *param})
		}
	}
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

func (s State) Parameters() state.Parameters {
	var result = make([]*state.Parameter, 0, len(s))
	for i, _ := range s {
		parameter := &s[i].Parameter
		if len(s[i].Object) > 0 {
			parameter.Object = nil
			for j := range s[i].Object {
				parameter.Object = append(parameter.Object, &s[i].Object[j].Parameter)
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
func (s *State) AppendConst(constants map[string]interface{}) {
	for paramName, paramValue := range constants {
		s.Append(NewConstParameter(paramName, paramValue))
	}
}

func (s *State) StateForSQL(SQL string, isRoot bool) State {
	var result = State{}
	includeParameter := isRoot
	if isPredicateUsed(SQL) {
		includeParameter = true
	}
	for _, candidate := range *s {
		if (includeParameter && candidate.Explicit) || candidate.IsUsedBy(SQL) {
			result = append(result, candidate)
		}
	}
	return result
}

func isPredicateUsed(SQL string) bool {
	return strings.Contains(SQL, "${predicate.Builder()")
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
		if kind == state.KindView && parameter.In.IsView() {
			result.Append(parameter)
			continue
		}
		switch parameter.In.Kind {
		case kind:
			result.Append(parameter)
		case state.KindRepeated:
			for _, candidate := range parameter.Repeated {
				if candidate.In.Kind == kind {
					result.Append(candidate)
					continue
				}
				switch candidate.In.Kind {
				case state.KindRepeated:
					if values := State(candidate.Repeated).FilterByKind(kind); len(values) > 0 {
						result.Append(values...)
					}
				case state.KindObject:
					if values := State(candidate.Object).FilterByKind(kind); len(values) > 0 {
						result.Append(values...)
					}

				}
			}
		case state.KindObject:
			for _, candidate := range parameter.Object {
				if candidate.In.Kind == kind {
					result.Append(candidate)
					continue
				}

				switch candidate.In.Kind {
				case state.KindRepeated:
					if values := State(candidate.Repeated).FilterByKind(kind); len(values) > 0 {
						result.Append(values...)
					}
				case state.KindObject:
					if values := State(candidate.Object).FilterByKind(kind); len(values) > 0 {
						result.Append(values...)
					}
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
	if parameters := s.FilterByKind(state.KindConst); len(parameters) > 0 {
		for _, literal := range parameters {
			expander[literal.Name] = literal.Value
		}
	}

	text = removeBuilinExpr(text)
	return expander.ExpandAsText(text)
}

func removeBuilinExpr(query string) string {
	//TODO make it more generics
	indexStart := strings.Index(query, "${predicate.")
	if indexStart == -1 {
		return query
	}
	match := query[indexStart:]
	indexEnd := strings.Index(match, "}")
	match = match[:indexEnd+1]
	query = strings.Replace(query, match, "  ", 1)

	if index := strings.Index(query, "$View.ParentJoinOn"); index != -1 {
		fragment := query[index:]
		if endIndex := strings.Index(fragment, ")"); endIndex != -1 {
			fragment = fragment[:endIndex+1]
		}
		query = strings.ReplaceAll(query, fragment, "")
	}

	if !strings.Contains(query, "${predicate.") {
		return query
	}
	return removeBuilinExpr(query)
}

// DsqlParameterDeclaration returns dql parameter declaration
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
		paramType, err := xreflect.Parse(paramDataType, xreflect.WithTypeLookup(func(name string, options ...xreflect.Option) (reflect.Type, error) {
			result, err := dirTypes.Type(name)
			if err == nil {
				return result, nil
			}
			return dirTypes.Registry.Lookup(name, options...)
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
		rType := schema.Type()
		if rType == nil {
			if rType, err = types.LookupType(lookupType, schema.DataType); err != nil {
				return nil, fmt.Errorf("failed to detect paramater '%v' type for: %v  %w", param.Name, schema.DataType, err)
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
	typeRegistry := xreflect.NewTypes(xreflect.WithPackagePath(modulePath), xreflect.WithRegistry(extension.Config.Types))
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
				return fmt.Errorf("failed to lookup param parameter: %v", param.In.Name)
			}
			if param.SQL != "" {
				query, err := structql.NewQuery(param.SQL, sourceParam.Schema.Type(), nil)
				if err != nil {
					return fmt.Errorf("failed to queryql param %v from %s(%s) due to: %w", param.Name, param.In.Name, sourceParam.Schema.Type().String(), err)
				}
				param.Schema = state.NewSchema(query.StructType())
				param.Schema.DataType = param.Name
			}
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
		rType, err := types.LookupType(typeRegistry.Lookup, dataType, xreflect.WithPackage(param.Schema.Package))
		if err != nil {
			return err
		}
		param.Schema.SetType(rType)
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

// NormalizeComposites normalizes state
func (s State) NormalizeComposites() (State, error) {
	var result = State{}
	byName := s.IndexByName()
	var itemParaemters = map[string]bool{}
	for _, candidate := range s { //TODO to be deprecated we just one way of assembling compositie types
		switch candidate.In.Kind {
		case state.KindRepeated, state.KindObject:
			if len(candidate.Object) > 0 || len(candidate.Repeated) > 0 {
				continue
			}
			if candidate.In.Name != "" {
				baseParams := strings.Split(candidate.In.Name, ",")
				candidate.In.Name = ""
				for _, name := range baseParams {
					itemParaemters[name] = true
					baseParameter := byName[name]
					if baseParameter == nil {
						return nil, fmt.Errorf("invalid %v(%v) failed to lookup base parameter: %s", candidate.Name, candidate.In.Kind, name)
					}
					candidate.AppendComposite(baseParameter)
				}
			}
		}
	}

	result = State{}
	for i, candidate := range s { //filter composite element parameters
		if itemParaemters[candidate.Name] {
			continue
		}
		result = append(result, s[i])
	}
	s = result
	result = State{}
	for i, candidate := range s {
		if candidate.Of != "" {
			candidate.Name = strings.Trim(candidate.Name, ".")
			holder := byName[candidate.Of]
			if holder == nil {
				return nil, fmt.Errorf("invalid %v(%v) failed to lookup holder: %s", candidate.Name, candidate.In.Kind, candidate.Of)
			}
			holder.AppendComposite(s[i])
			continue
		}
		result = append(result, s[i])
	}
	return result, nil
}

func (p *Parameter) AppendComposite(baseParameter *Parameter) {
	if p.In.Kind == state.KindObject {
		p.Object = append(p.Object, baseParameter)
		p.Parameter.Object = append(p.Parameter.Object, &baseParameter.Parameter)
	} else {
		p.Repeated = append(p.Repeated, baseParameter)
		p.Parameter.Repeated = append(p.Parameter.Repeated, &baseParameter.Parameter)
	}
	if p.In.Name != "" {
		p.In.Name += ","
	}
	p.In.Name += baseParameter.Name
}

func (s *State) AdjustOutput() error {
	outputParameter := s.GetOutputParameter()
	if outputParameter == nil {
		return nil
	}
	parameters := s.IndexByName()
	outputType := outputParameter.Schema.Type()
	if outputType == nil {
		return fmt.Errorf("invalid output type - missing schema type")
	}
	sType := structology.NewStateType(outputType)
	outputParameters := sType.MatchByTag(tags.ParameterTag)

	var adjustedMap = map[string]bool{}
	var adjusted State

	for _, parameterField := range outputParameters {
		name := parameterField.Path()
		index := strings.LastIndex(name, ".")
		parameter, err := s.selectorParameter(parameters, parameterField)
		if err != nil {
			return fmt.Errorf("failed to expand output group type: %w", err)
		}
		if index != -1 {
			parentName := name[:index]
			parent := parameters[parentName]
			if parent == nil {
				parentField := sType.Lookup(parentName)
				if parent, err = s.selectorParameter(parameters, parentField); err != nil {
					return fmt.Errorf("failed to expand output type: %w", err)
				}
			}
			parent.Object = append(parent.Object, parameter)
			var items []string
			for _, item := range parent.Object {
				items = append(items, item.Name)
			}
			parent.In.Name = strings.Join(items, ",")
			continue
		}
		if !adjustedMap[parameter.Name] {
			adjusted = append(adjusted, parameter)
			adjustedMap[parameter.Name] = true
		}
	}
	*s = adjusted
	return nil
}

func (s *State) selectorParameter(predefined map[string]*Parameter, parameterField *structology.Selector) (*Parameter, error) {
	ret, ok := predefined[parameterField.Name()]
	if ok {
		return ret, nil
	}
	tag := string(parameterField.Tag())
	structField := &reflect.StructField{Name: parameterField.Name(), Tag: reflect.StructTag(tag), Type: parameterField.Type()}
	stateParameter, err := state.BuildParameter(structField, nil, nil)
	if err != nil {
		return nil, err
	}
	ret = &Parameter{Parameter: *stateParameter}
	predefined[ret.Name] = ret
	return ret, nil
}

func (s *State) GetOutputParameter() *Parameter {
	for _, candidate := range *s {
		if candidate.In.Kind == state.KindOutput && candidate.In.Name == "" {
			return candidate
		}
	}
	return nil
}

func (s State) AddDescriptions(doc state.Documentation) {
	for _, parameter := range s {
		description, ok := doc.ByName(parameter.Name)
		if ok {
			parameter.Description = description
		}
	}
}

// NewState creates a state from state go struct
func NewState(modulePath, dataType string, types *xreflect.Types) (State, error) {
	baseDir := modulePath
	parent, name := path.Split(dataType)
	if name != "" {
		dataType = name
	}
	if !strings.HasSuffix(baseDir, parent) {
		var subPath string
		parent, subPath = path.Split(parent)
		if strings.HasSuffix(baseDir, parent) {
			baseDir = path.Join(baseDir, subPath)
		}
	}

	if pair := strings.Split(dataType, "."); len(pair) > 1 {
		if !strings.HasSuffix(baseDir, pair[0]) {
			baseDir = path.Join(baseDir, pair[0])
		}
		dataType = pair[1]
	}

	embedRoot := baseDir
	if index := strings.LastIndex(dataType, "."); index != -1 {
		embedRoot = path.Join(embedRoot, dataType[:index])
	}
	embedHolder := discoverEmbeds(embedRoot)
	embedFS := embedHolder.EmbedFs()
	var aState = State{}
	dirTypes, err := xreflect.ParseTypes(baseDir,
		xreflect.WithParserMode(parser.ParseComments),
		xreflect.WithRegistry(types),
		xreflect.WithOnField(func(typeName string, field *ast.Field) error {
			if field.Tag == nil {
				return nil
			}
			fieldTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
			aTag, err := tags.ParseStateTags(fieldTag, embedFS)
			if err != nil {
				return err
			}
			if aTag.Parameter == nil {
				return nil
			}
			pTag := aTag.Parameter
			if pTag.Kind == "" {
				return nil
			}
			var name string
			if len(field.Names) > 0 {
				name = field.Names[0].Name
			}
			setter.SetStringIfEmpty(&pTag.Name, name)
			param, err := buildParameter(field, aTag, types, embedFS)
			if param == nil {
				return err
			}
			state.BuildPredicate(aTag, &param.Parameter)
			state.BuildCodec(aTag, &param.Parameter)
			if param.Output != nil {
				if param.Output.Schema == nil && param.Schema != nil {
					param.Output.Schema = param.Schema
					param.Schema = &state.Schema{DataType: aTag.Parameter.DataType}
					if aTag.Parameter.ErrorCode != 0 {
						param.ErrorStatusCode = aTag.Parameter.ErrorCode
					}
				}
			}
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

func discoverEmbeds(embedRoot string) *embed.Holder {
	embedRoot = url.Normalize(embedRoot, file.Scheme)
	fs := afs.New()
	embedFs := embed.NewHolder()
	if objects, _ := fs.List(context.Background(), embedRoot); len(objects) > 0 {
		for _, holder := range objects {
			if !holder.IsDir() || url.Equals(holder.URL(), embedRoot) {
				continue
			}
			assets, _ := fs.List(context.Background(), holder.URL())
			for _, candidate := range assets {
				name := strings.TrimSpace(candidate.Name())

				if strings.HasSuffix(name, ".sql") {
					URI := path.Join(holder.Name(), name)
					content, err := fs.DownloadWithURL(context.Background(), candidate.URL())
					fmt.Printf("!%s! %v %s\n", name, strings.HasSuffix(name, ".sql"), content)

					if err != nil {
						fmt.Printf("1\n")
					}
					if len(content) > 0 {
						embedFs.Add(URI, string(content))
					}
				}
			}
		}
	}
	return embedFs
}
