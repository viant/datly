package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

//go:embed tmpl/input.gox
var inputGoTemplate string

func (t *Template) GenerateInput(pkg string, info *plugin.Info, embedContent map[string]string) string {
	pkg = t.getPackage(pkg)
	if len(t.State) == 0 {
		return ""
	}
	output := inputGoTemplate
	if t.MethodFragment != "" && t.MethodFragment != "get" {
		output = strings.Replace(output, "$Package", strings.ToLower(t.MethodFragment), 1)
	}
	output = strings.Replace(output, "$Package", pkg, 1)
	var fields = []string{}
	root := text.DetectCaseFormat(t.Prefix).Format(t.Prefix, text.CaseFormatLowerUnderscore)
	for _, input := range t.State {
		fields = append(fields, input.FieldDeclaration(root, embedContent, t.TypeDef))
	}

	if t.IndexGenerator != nil && len(t.IndexGenerator.paramByIndexName) > 0 && !t.InsertOnly {
		for _, indexField := range t.IndexGenerator.paramByIndexName {
			decl := indexField.IndexFieldDeclaration()
			fields = append(fields, decl)
		}
	}

	imports := inference.NewImports()

	embedFSSnippet := ""
	embedFSName := ""
	if len(embedContent) > 0 {
		embedFSName = fmt.Sprintf("&%vFS", t.Prefix+t.MethodFragment)
		embedFSSnippet = fmt.Sprintf(`
//go:embed %v/*.sql
var %vFS embed.FS`, root, t.Prefix+t.MethodFragment)
		imports.AddPackage("embed")
	}
	output = strings.Replace(output, "$Fields", strings.Join(fields, "\n\n"), 1)
	registerTypes := ""
	importFragment := ""
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:
		imports.AddPackage(info.ChecksumPkg())
		imports.AddPackage("reflect")
		imports.AddPackage(info.TypeCorePkg())
		importFragment = imports.PackageImports()
		registry := &customTypeRegistry{}
		registry.register("Input")
		registerTypes = registry.stringify()
	}
	output = strings.ReplaceAll(output, "$DataField", t.InputDataField())
	output = strings.ReplaceAll(output, "$OutputField", t.OutputDataField())

	output = strings.Replace(output, "$Imports", importFragment, 1)
	output = strings.Replace(output, "$RegisterTypes", registerTypes, 1)
	output = strings.ReplaceAll(output, "$EmbedFS", embedFSSnippet)

	embedderFun := ""
	if t.IsHandler {
		embedderFun = strings.Replace(`
	func (i *Input) EmbedFS() (fs *embed.FS) {
		return ${EmbedFSName}
	}
`, "${EmbedFSName}", embedFSName, 1)

	}
	output = strings.ReplaceAll(output, "${EmbedFSFunc}\n", embedderFun)
	return output
}

//go:embed tmpl/input_init.gox
var inputInitGoTemplate string

func (t *Template) GenerateInputInit(pkg string) string {
	pkg = t.getPackage(pkg)
	output := inputInitGoTemplate
	if t.MethodFragment != "" && t.MethodFragment != "get" {
		output = strings.Replace(output, "$Package", strings.ToLower(t.MethodFragment), 1)
	}
	output = strings.Replace(output, "$Package", pkg, 1)
	output = strings.Replace(output, "$IndexSlice", t.IndexByCode, 1)
	return output
}

//go:embed tmpl/input_validate.gox
var inputValidateGoTemplate string

func (t *Template) GenerateInputValidate(pkg string) string {
	pkg = t.getPackage(pkg)
	output := inputValidateGoTemplate
	if t.MethodFragment != "" && t.MethodFragment != "get" {
		output = strings.Replace(output, "$Package", strings.ToLower(t.MethodFragment), 1)
	}
	output = strings.Replace(output, "$Package", pkg, 1)
	output = strings.ReplaceAll(output, "$DataField", t.InputDataField())
	output = strings.Replace(output, "$IndexSlice", t.IndexByCode, 1)

	builder := strings.Builder{}
	for _, parameter := range t.State {
		if parameter.In.IsView() && !parameter.IsAuxiliary {
			builder.WriteString("case *")
			builder.WriteString(parameter.Schema.SimpleTypeName())
			builder.WriteString(":\n_, ok := i.")
			builder.WriteString(parameter.IndexVariable())
			builder.WriteString("[actual.")
			builder.WriteString(parameter.PathParam.IndexField.Name)
			builder.WriteString("]\nreturn ok\n")
		}
	}
	output = strings.Replace(output, "${CanUseMarkerProviderCases}", builder.String(), 1)
	return output
}

//go:embed tmpl/output.gox
var outputGoTemplate string

func (t *Template) GenerateOutput(pkg string, info *plugin.Info) string {
	pkg = t.getPackage(pkg)
	if t.MethodFragment != "" && t.MethodFragment != "get" {
		pkg = strings.ToLower(t.MethodFragment)
	}
	var imports []string

	registerTypes := ""
	switch info.IntegrationMode {
	case plugin.ModeExtension, plugin.ModeCustomTypeModule:

		imports = append(imports, info.ChecksumPkg())
		imports = append(imports, "reflect")
		imports = append(imports, info.TypeCorePkg())
		registry := &customTypeRegistry{}
		registry.register("Output")
		registerTypes = registry.stringify()
	}
	var registerSnippet = outputGoTemplate
	registerSnippet = strings.Replace(registerSnippet, "$RegisterTypes", registerTypes, 1)

	var options = []xreflect.Option{
		xreflect.WithPackage(pkg),
		xreflect.WithImports(imports),
		xreflect.WithSkipFieldType(func(field *reflect.StructField) bool {
			if aTag, _ := tags.Parse(field.Tag, nil, tags.ParameterTag); aTag != nil && aTag.Parameter != nil {
				return aTag.Parameter.Kind == string(state.KindRequestBody)
			}
			return false
		}),
		xreflect.WithRegistry(t.Resource.Rule.TypeRegistry()),
		xreflect.WithSnippetBefore(registerSnippet),
	}
	outputState := xreflect.GenerateStruct("Output", t.OutputType, options...)

	return outputState
}

func (t *Template) getPackage(pkg string) string {
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

func (t *Template) buildState(spec *inference.Spec, aState *inference.State, card state.Cardinality) reflect.Type {
	t.Imports.AddType(spec.Type.TypeName())
	pathParameter := t.buildPathParameterIfNeeded(spec)
	if pathParameter != nil {
		aState.Append(pathParameter)
	}
	if spec.Type.Cardinality == state.Many {
		card = state.Many
	}
	spec.EnsureRelationType()
	for _, relation := range spec.Relations {
		t.buildState(relation.Spec, aState, card)
	}
	parameter := t.buildDataViewParameter(spec, card)
	parameter.PathParam = pathParameter
	aState.Append(parameter)
	return parameter.Schema.Type()
}

func (t *Template) buildPathParameterIfNeeded(spec *inference.Spec) *inference.Parameter {
	selector := spec.Selector(t.InputDataField())
	indexField, SQL := spec.PkStructQL(selector)
	if SQL == "" {
		return nil
	}
	param := &inference.Parameter{}
	parameterNamer := t.ColumnParameterNamer(selector)
	param.Name = parameterNamer(indexField)
	param.SQL = SQL
	param.In = &state.Location{Kind: state.KindParam, Name: selector[0]}
	var paramType = reflect.StructOf([]reflect.StructField{{Name: "Values", Type: reflect.SliceOf(indexField.Schema.Type())}})
	param.Schema = state.NewSchema(paramType)
	param.IndexField = indexField
	return param
}

func (t *Template) buildDataViewParameter(spec *inference.Spec, cardinality state.Cardinality) *inference.Parameter {
	param := &inference.Parameter{ModificationSetting: inference.ModificationSetting{IsAuxiliary: spec.IsAuxiliary}}
	param.Name = t.ParamName(spec.Type.Name)
	param.Schema = &state.Schema{DataType: spec.Type.Name, Cardinality: cardinality}
	param.Schema.SetPackage(spec.Package)
	param.In = state.NewViewLocation(param.Name)
	param.SQL = spec.ViewSQL(t.ColumnParameterNamer(spec.Selector(t.InputDataField())))
	columnFields := spec.Type.Fields(inference.WithStructTag())
	param.Schema.SetType(reflect.PointerTo(reflect.StructOf(columnFields)))
	return param
}
