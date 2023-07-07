package codegen

import (
	_ "embed"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view"
	"reflect"
	"strings"
)

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
	imports := inference.NewImports()
	imports.AddPackage(info.ChecksumPkg())
	imports.AddPackage("reflect")
	imports.AddPackage(info.TypeCorePkg())
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

func (t *Template) buildState(spec *inference.Spec, state *inference.State, card view.Cardinality) reflect.Type {
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
		for _, field := range spec.Type.RelationFields {
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

func (t *Template) buildPathParameterIfNeeded(spec *inference.Spec) *inference.Parameter {
	selector := spec.Selector()
	indexField, SQL := spec.PkStructQL(selector)
	if SQL == "" {
		return nil
	}
	param := &inference.Parameter{}
	parameterNamer := t.ColumnParameterNamer(selector)
	param.Name = parameterNamer(indexField)
	param.SQL = SQL
	param.In = &view.Location{Kind: view.KindParam, Name: selector[0]}
	var paramType = reflect.StructOf([]reflect.StructField{{Name: "Values", Type: reflect.SliceOf(indexField.Schema.Type())}})
	param.Schema = view.NewSchema(paramType)
	param.IndexField = indexField
	return param
}

func (t *Template) buildDataViewParameter(spec *inference.Spec, cardinality view.Cardinality, fields []reflect.StructField) *inference.Parameter {
	param := &inference.Parameter{ModificationSetting: inference.ModificationSetting{IsAuxiliary: spec.IsAuxiliary}}
	param.Name = t.ParamName(spec.Type.Name)
	param.Schema = &view.Schema{DataType: spec.Type.Name, Cardinality: cardinality}
	param.In = &view.Location{Kind: view.KindDataView, Name: param.Name}
	param.SQL = spec.ViewSQL(t.ColumnParameterNamer(spec.Selector()))
	columnFields := spec.Type.Fields()
	columnFields = append(columnFields, fields...)

	param.Schema.SetType(reflect.PtrTo(reflect.StructOf(columnFields)))
	return param
}
