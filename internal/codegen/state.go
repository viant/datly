package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"go/ast"
	"go/parser"
	"path"
	"reflect"
	"strings"
)

type State []*Parameter

func (s *State) Append(param ...*Parameter) {
	*s = append(*s, param...)
}

func (s State) IndexByName() map[string]*Parameter {
	result := map[string]*Parameter{}
	for _, parameter := range s {
		result[parameter.Name] = parameter
	}

	return result
}

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

func (s State) FilterByKind(kind view.Kind) State {
	result := State{}
	for _, parameter := range s {
		if parameter.In.Kind == kind {
			result.Append(parameter)
		}
	}
	return result
}

func (s State) dsqlParameterDeclaration() string {
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

func (s State) localStateBasedVariableDefinition() ([]string, string) {
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

func buildParameter(field *ast.Field, types *xreflect.Types) (*Parameter, error) {
	SQL := extractSQL(field)
	if field.Tag == nil {
		return nil, nil
	}
	structTag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
	datlyTag := structTag.Get(view.DatlyTag)
	if datlyTag == "" {
		return nil, nil
	}
	tag := view.ParseTag(datlyTag)
	param := &Parameter{
		SQL:      SQL,
		FieldTag: field.Tag.Value,
	}
	//	updateSQLTag(field, SQL)
	param.Name = field.Names[0].Name
	param.In = &view.Location{Name: tag.In, Kind: view.Kind(tag.Kind)}

	cardinality := view.One
	if sliceExpr, ok := field.Type.(*ast.ArrayType); ok {
		field.Type = sliceExpr.Elt
		cardinality = view.Many
	}

	if ptr, ok := field.Type.(*ast.StarExpr); ok {
		field.Type = ptr.X
	}

	fieldType, err := xreflect.Node{Node: field.Type}.Stringify()
	if err != nil {
		return nil, fmt.Errorf("failed to create param: %v due to %w", param.Name, err)
	}
	if strings.Contains(fieldType, "struct{") {
		typeName := ""
		if field.Tag != nil {
			if typeName, _ = reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Lookup("typeName"); typeName == "" {
				typeName = field.Names[0].Name
			}
		}
		rType, err := types.Lookup(typeName, xreflect.WithTypeDefinition(fieldType))
		if err != nil {
			return nil, fmt.Errorf("failed to create param: %v due reflect.Type %w", param.Name, err)
		}
		param.Schema = view.NewSchema(rType)
	} else {
		param.Schema = &view.Schema{DataType: fieldType}
	}

	param.Schema.Cardinality = cardinality
	return param, nil
}

func extractSQL(field *ast.Field) string {
	SQL := ""
	if field.Doc != nil {
		comments := xreflect.CommentGroup(*field.Doc).Stringify()
		comments = strings.Trim(comments, "\"/**/")
		comments = strings.ReplaceAll(comments, "\t", "  ")
		comments = strings.ReplaceAll(comments, "\n", " ")
		SQL = strings.TrimSpace(comments)
	}
	return SQL
}

//go:embed tmpl/state.gox
var stateGoTemplate string

func (t *Template) GenerateState(pkg string, info *plugin.Info) string {
	pkg = t.getPakcage(pkg)
	if len(t.State) == 0 {
		return ""
	}
	var output = strings.Replace(stateGoTemplate, "$Package", pkg, 1)
	var fields = []string{}
	for _, input := range t.State {
		fields = append(fields, input.FieldDeclaration())
	}
	output = strings.Replace(output, "$Fields", strings.Join(fields, "\n\n"), 1)

	registerTypes := t.RegisterFragment("State")

	importFragment := ""
	imports := Imports{}
	imports.AddPackage(info.ChecksumPkg())
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:
		importFragment = imports.PackageImports()
	default:
		registerTypes = ""
	}
	output = strings.Replace(output, "$Imports", importFragment, 1)

	output = strings.Replace(output, "$RegisterTypes", registerTypes, 1)
	return output
}

func (t *Template) getPakcage(pkg string) string {
	if pkg == "" {
		if t.TypeDef != nil {
			pkg = t.TypeDef.Package
		}
	}
	if pkg == "" {
		pkg = "main"
	}
	return pkg
}

func (t *Template) buildState(spec *Spec, state *State, card view.Cardinality) reflect.Type {
	t.Imports.AddType(spec.Type.TypeName())

	pathParameter := t.buildPathParameterIfNeeded(spec)
	if pathParameter != nil {
		state.Append(pathParameter)
	}

	if spec.Type.Cardinality == view.Many {
		card = view.Many
	}

	var relationFields []reflect.StructField
	for _, rel := range spec.Relations {
		var tag string
		for _, field := range spec.Type.relationFields {
			if field.Name == rel.Name {
				tag = field.Tag
			}
		}

		relationFields = append(relationFields, reflect.StructField{
			Name: rel.Name,
			Type: t.buildState(rel.Spec, state, rel.Cardinality),
			Tag:  reflect.StructTag(tag),
		})
	}

	parameter := t.buildDataViewParameter(spec, card, relationFields)
	parameter.PathParam = pathParameter
	state.Append(parameter)
	return parameter.Schema.Type()
}

func (t *Template) buildPathParameterIfNeeded(spec *Spec) *Parameter {
	selector := spec.Selector()
	indexField, SQL := spec.pkStructQL(selector)
	if SQL == "" {
		return nil
	}
	param := &Parameter{}
	parameterNamer := t.ColumnParameterNamer(selector)
	param.Name = parameterNamer(indexField)
	param.SQL = SQL
	param.In = &view.Location{Kind: view.KindParam, Name: selector[0]}
	var paramType = reflect.StructOf([]reflect.StructField{{Name: "Values", Type: reflect.SliceOf(indexField.Schema.Type())}})
	param.Schema = view.NewSchema(paramType)
	param.IndexField = indexField
	return param
}

func (t *Template) buildDataViewParameter(spec *Spec, cardinality view.Cardinality, fields []reflect.StructField) *Parameter {
	param := &Parameter{IsAuxiliary: spec.isAuxiliary}
	param.Name = t.ParamName(spec.Type.Name)
	param.Schema = &view.Schema{DataType: spec.Type.Name, Cardinality: cardinality}
	param.In = &view.Location{Kind: view.KindDataView, Name: param.Name}
	param.SQL = spec.viewSQL(t.ColumnParameterNamer(spec.Selector()))

	columnFields := spec.Type.Fields()
	columnFields = append(columnFields, fields...)

	param.Schema.SetType(reflect.PtrTo(reflect.StructOf(columnFields)))
	return param
}
