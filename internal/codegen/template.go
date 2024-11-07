package codegen

import (
	_ "embed"
	"fmt"
	ast "github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	Template struct {
		Resource   *translator.Resource
		Spec       *inference.Spec
		Config     *translator.Rule
		TypeDef    *view.TypeDefinition
		IsHandler  bool
		InsertOnly bool
		inference.Imports
		State              inference.State
		Output             inference.State
		BusinessLogic      *ast.Block
		IndexGenerator     *IndexGenerator
		IndexByCode        string
		paramPrefix        string
		recordPrefix       string
		InputType          reflect.Type
		BodyType           reflect.Type
		BodyParameter      *inference.Parameter
		OutputType         reflect.Type
		MethodFragment     string
		fileMethodFragment string
		Prefix             string
		filePrefix         string
	}
)

const (
	paramPrefix  = "Cur"
	recordPrefix = "Rec"
)

func (t *Template) FilePrefix() string {
	if t.MethodFragment != "" || !t.IsHandler {
		return ""
	}

	if t.filePrefix != "" {
		return t.filePrefix
	}
	t.filePrefix = text.DetectCaseFormat(t.Prefix).Format(t.Prefix, text.CaseFormatLowerUnderscore)
	return t.filePrefix
}

func (t *Template) FileMethodFragment() string {
	if t.fileMethodFragment != "" {
		return t.fileMethodFragment
	}
	t.fileMethodFragment = text.DetectCaseFormat(t.MethodFragment).Format(t.MethodFragment, text.CaseFormatLowerUnderscore)
	return t.fileMethodFragment
}

func (t *Template) ParamPrefix() string {
	prefix := t.paramPrefix
	if prefix == "" {
		prefix = paramPrefix
	}
	return prefix
}

func (t *Template) ParamName(name string) string {
	return t.ParamPrefix() + name
}

func (t *Template) RecordName(name string) string {
	return t.RecordPrefix() + name
}

func (t *Template) ParamIndexName(name, by string) string {
	return t.ParamPrefix() + name + "By" + by
}

func (t *Template) ColumnParameterNamer(selector inference.Selector) inference.ColumnParameterNamer {
	prefix := t.ParamPrefix() + selector.Name()
	return func(field *inference.Field) string {
		return prefix + field.Name
	}
}

func (t *Template) SetResource(resource *translator.Resource) {
	t.Resource = resource
	t.setMethodFragment()
}

func (t *Template) setMethodFragment() {
	method := strings.ToLower(t.Resource.Rule.Method)
	switch method {
	case "get", "":
		method = ""
	case "patch":
		method = "Patch"
	case "post":
		method = "Post"
	case "put":
		method = "Put"
	}
	if t.IsHandler {
		t.MethodFragment = method
	}
}

func (t *Template) BuildInput(spec *inference.Spec, explicitInput inference.State, opts ...Option) {
	t.State = inference.State{}
	options := &Options{}
	options.apply(opts)

	bodyParameter := &inference.Parameter{Parameter: state.Parameter{Schema: &state.Schema{Cardinality: state.Many}}}
	bodyParameters := explicitInput.FilterByKind(state.KindRequestBody)
	if len(bodyParameters) > 0 {
		bodyParameter = bodyParameters[0]
		if bodyParameter.Schema.Cardinality != "" {
			spec.Type.Cardinality = bodyParameter.Schema.Cardinality
		}
	}
	t.ensureBodyParameter(spec, bodyParameter)
	t.State.Append(bodyParameter)

	bodyParameter.Schema.SetType(t.buildState(spec, &t.State, spec.Type.Cardinality))
	t.BodyType = bodyParameter.Schema.Type()
	t.InsertOnly = options.IsInsertOnly()
	if options.IsInsertOnly() {
		return
	}

	var structFields []reflect.StructField

	for _, parameter := range t.State {
		if parameter.In.IsView() && !parameter.IsAuxiliary {
			parameter.Schema.Cardinality = state.Many
		}
		var structTag = reflect.StructTag(parameter.Tag)
		if parameter.Schema.DataType != "" {
			structTag = reflect.StructTag(fmt.Sprintf(`%v:"%v"`, xreflect.TagTypeName, parameter.Schema.TypeName()))
		}
		structFields = append(structFields, reflect.StructField{
			Name: parameter.Name,
			Type: parameter.Schema.Type(),
			Tag:  structTag,
		})
	}
	for i, parameter := range explicitInput {
		if parameter.In.Kind == state.KindRequestBody {
			continue
		}
		var structTag reflect.StructTag
		if parameter.Schema.DataType != "" {
			structTag = reflect.StructTag(fmt.Sprintf(`%v:"%v"`, xreflect.TagTypeName, parameter.Schema.TypeName()))
		}
		structFields = append(structFields, reflect.StructField{
			Name: parameter.Name,
			Type: parameter.Schema.Type(),
			Tag:  structTag,
		})
		t.State.Append(explicitInput[i])
	}

	t.InputType = reflect.StructOf(structFields)
}

func (t *Template) ensureBodyParameter(spec *inference.Spec, bodyParameter *inference.Parameter) {
	if bodyParameter.Name == "" {
		bodyParameter.Name = spec.Type.Name
	}
	bodyParameter.Schema = &state.Schema{DataType: spec.Type.Name, Cardinality: spec.Type.Cardinality}
	bodyParameter.In = &state.Location{Kind: state.KindRequestBody, Name: bodyParameter.In.Name}
}

func (t *Template) BuildLogic(spec *inference.Spec, opts ...Option) {
	block := ast.Block{}
	options := &Options{}
	options.apply(opts)
	if options.withInsert {
		t.allocateSequence(options, spec, &block)
	}
	block.AppendEmptyLine()
	if options.withUpdate {
		t.indexRecords(options, spec, &block)
	}
	t.modifyRecords(options, "", spec, spec.Type.Cardinality, &block, nil)
	t.BusinessLogic = &block
}

func (t *Template) InputDataField() string {
	dataFields := t.State.FilterByKind(state.KindRequestBody)
	if len(dataFields) > 0 {
		return dataFields[0].Name
	}
	return ""
}

func (t *Template) OutputDataField() string {
	dataFields := t.Output.FilterByKind(state.KindRequestBody)
	if len(dataFields) > 0 {
		return dataFields[0].Name
	}
	return "Data"
}

func (t *Template) allocateSequence(options *Options, spec *inference.Spec, block *ast.Block) {
	if spec.IsAuxiliary {
		return
	}
	if len(spec.Type.PkFields) != 1 {
		return
	}

	dataField := t.InputDataField()

	if field := spec.Type.PkFields[0]; strings.Contains(field.Schema.TypeName(), "int") {
		selector := spec.Selector(t.InputDataField())
		if dataField == "" {
			dataField = selector[0]
		}

		indent := ast.NewIdent(dataField)
		if t.IsHandler {
			indent = ast.NewHolderIndent("input", dataField)
		}
		var args = []ast.Expression{ast.NewQuotedLiteral(spec.Table), indent,
			ast.NewQuotedLiteral(strings.TrimLeft(selector.Path(field.Name), "/")),
		}
		if options.IsGoLang() {
			args = append([]ast.Expression{ast.Expression(ast.NewIdent("ctx"))}, args...)
		}

		call := ast.NewErrorCheck(ast.NewCallExpr(ast.NewIdent("sequencer"), "Allocate", args...))
		block.Append(ast.NewStatementExpression(call))
	}

	for _, rel := range spec.Relations {
		t.allocateSequence(options, rel.Spec, block)
	}

}

func (t *Template) indexRecords(options *Options, spec *inference.Spec, block *ast.Block) {
	//if spec.IsAuxiliary {
	//	return
	//}
	field := spec.Type.PkFields[0]
	holder := t.ParamIndexName(spec.Type.Name, field.Name)
	source := t.ParamName(spec.Type.Name)

	indexBy := ast.NewCallExpr(ast.NewHolderIndent("i", source), "IndexBy", ast.NewQuotedLiteral(field.Name))
	block.Append(ast.NewAssign(ast.NewHolderIndent("i", holder), indexBy))
	for _, rel := range spec.Relations {
		t.indexRecords(options, rel.Spec, block)
	}
}

func (t *Template) modifyRecords(options *Options, structPathPrefix string, spec *inference.Spec, cardinality state.Cardinality, parentBlock *ast.Block, rel *inference.Relation) {
	if spec.IsAuxiliary {
		return
	}
	if len(spec.Type.PkFields) != 1 {
		return
	}

	structPath := spec.Type.Name
	if spec == t.Spec {
		structPath = t.InputDataField()
	}

	if structPathPrefix != "" {
		structPath = structPathPrefix + "." + structPath
	}

	switch cardinality {
	case state.One:
		structSelector := ast.NewIdent(structPath)
		checkValid := ast.NewCondition(structSelector, ast.Block{}, nil)
		if rel != nil {
			parentSelector := structPathPrefix + "." + rel.ParentField.Name
			holder := structPathPrefix + "." + rel.Name + "." + rel.KeyField.Name
			t.synchronizeRefKeys(holder, parentSelector, rel, &checkValid.IFBlock)
		}

		t.modifyRecord(options, structPath, spec, &checkValid.IFBlock)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, structPath, rel.Spec, rel.Cardinality, &checkValid.IFBlock, rel)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(checkValid)
	case state.Many:
		recordPath := t.RecordName(spec.Type.Name)
		forEach := ast.NewForEach(ast.NewIdent(recordPath), ast.NewIdent(structPath), ast.Block{})

		if rel != nil && rel.KeyField != nil {
			parentSelector := structPathPrefix + "." + rel.ParentField.Name
			holder := recordPath + "." + rel.KeyField.Name
			t.synchronizeRefKeys(holder, parentSelector, rel, &forEach.Body)
		}

		t.modifyRecord(options, recordPath, spec, &forEach.Body)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, recordPath, rel.Spec, rel.Cardinality, &forEach.Body, rel)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(forEach)
	}
}

func (t *Template) synchronizeRefKeys(x, y string, rel *inference.Relation, block *ast.Block) {
	src := ast.Expression(ast.NewIdent(y))
	if !rel.ParentField.Ptr && rel.KeyField.Ptr {
		src = ast.NewRefExpression(src)
	} else if rel.ParentField.Ptr != rel.KeyField.Ptr {
		src = ast.NewDerefExpression(src)
	}

	assignKey := ast.NewAssign(ast.NewIdent(x), src)
	block.Append(assignKey)
}

func (t *Template) modifyRecord(options *Options, recordPath string, spec *inference.Spec, block *ast.Block) {
	field := spec.Type.PkFields[0]
	fieldPath := recordPath + "." + field.Name
	recordSelector := ast.NewIdent(recordPath)
	fieldSelector := ast.NewIdent(fieldPath)
	var matchCond *ast.Condition

	if options.withUpdate {
		xSelector := ast.NewIdent(t.ParamIndexName(spec.Type.Name, field.Name))
		if t.IsHandler {
			xSelector = ast.NewHolderIndent("input", t.ParamIndexName(spec.Type.Name, field.Name))
		}
		hasFn := "HasKey"
		if options.IsGoLang() {
			hasFn = "Has"
		}
		x := ast.NewCallExpr(xSelector, hasFn, fieldSelector)
		expr := ast.NewBinary(x, "==", ast.NewLiteral("true"))
		matchCond = ast.NewCondition(expr, ast.Block{}, nil)
		t.update(options, recordSelector, spec, &matchCond.IFBlock)
	}
	if options.withInsert {
		insertBlock := ast.Block{}
		t.insert(options, recordSelector, spec, &insertBlock)
		if matchCond != nil {
			matchCond.ElseBlock = insertBlock
		} else {
			block.Append(insertBlock)
		}
	}
	if matchCond != nil {
		block.Append(matchCond)
	}
}

func (t *Template) insert(options *Options, selector *ast.Ident, spec *inference.Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")

	args := []ast.Expression{selector, ast.NewQuotedLiteral(spec.Table)}
	if options.IsGoLang() {
		t.swapArgs(args)
	}

	callExpr := ast.NewErrorCheck(ast.NewCallExpr(holder, "Insert", args...))
	block.Append(ast.NewTerminatorExpression(callExpr))

}

func (t *Template) swapArgs(args []ast.Expression) {
	args[0], args[1] = args[1], args[0]
}

func (t *Template) update(options *Options, selector *ast.Ident, spec *inference.Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")
	args := []ast.Expression{selector, ast.NewQuotedLiteral(spec.Table)}
	if options.IsGoLang() {
		t.swapArgs(args)
	}

	callExpr := ast.NewErrorCheck(ast.NewCallExpr(holder, "Update", args...))
	block.Append(ast.NewTerminatorExpression(callExpr))
}

func (t *Template) RecordPrefix() string {
	if t.recordPrefix != "" {
		return t.recordPrefix
	}
	return recordPrefix
}

func (t *Template) BuildTypeDef(spec *inference.Spec, wrapperField string, columns state.Documentation) {
	t.TypeDef = spec.TypeDefinition(wrapperField, true, columns)
	t.ensurePackageImports(t.TypeDef.Package, t.TypeDef.Fields)
	t.ensureTypeImport(spec.Type.Name)
	if wrapperField != "" {
		t.ensureTypeImport(wrapperField)
	}
}

func (t *Template) ensureTypeImport(simpleTypeName string) {
	typeName := simpleTypeName
	if t.TypeDef.Package != "" {
		typeName = t.TypeDef.Package + "." + simpleTypeName
	}
	t.Imports.AddType(typeName)
}

func (t *Template) EnsureImports(aType *inference.Type) {
	t.Imports.AddType(aType.TypeName())
	if len(aType.RelationFields) == 0 {
		return
	}

	for _, field := range aType.RelationFields {
		t.Imports.AddType(aType.ExpandType(field.Schema.TypeName()))
	}
}

func (t *Template) ensurePackageImports(defaultPkg string, fields []*view.Field) {
	for _, field := range fields {
		if len(field.Fields) > 0 {
			t.ensurePackageImports(defaultPkg, field.Fields)
		}
		schema := field.Schema
		if schema == nil {
			continue
		}
		rType := schema.Type()
		if rType == nil {
			continue
		}
		if rType.PkgPath() != defaultPkg {
			t.Imports.AddPackage(rType.PkgPath())
		}
	}
}

func NewTemplate(rule *translator.Rule, spec *inference.Spec) *Template {
	return &Template{paramPrefix: paramPrefix, Prefix: rule.Root, Config: rule, Imports: inference.NewImports(), Spec: spec}
}
