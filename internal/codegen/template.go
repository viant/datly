package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/cmd/option"
	ast "github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	Template struct {
		Spec    *Spec
		Config  *option.RouteConfig
		TypeDef *view.TypeDefinition
		Imports
		State
		BusinessLogic *ast.Block
		paramPrefix   string
		recordPrefix  string
		StateType     reflect.Type
	}

	ColumnParameterNamer func(column *Field) string
)

const (
	paramPrefix  = "Cur"
	recordPrefix = "Rec"
)

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

func (t *Template) ColumnParameterNamer(selector Selector) ColumnParameterNamer {
	prefix := t.ParamPrefix() + selector.Name()
	return func(field *Field) string {
		return prefix + field.Name
	}
}

func (t *Template) BuildState(spec *Spec, bodyHolder string, opts ...Option) {
	t.State = State{}
	options := &Options{}
	options.apply(opts)
	bodyParam := t.buildBodyParameter(spec, bodyHolder)
	t.State.Append(bodyParam)
	if options.isInsertOnly() {
		return
	}

	bodyParam.Schema.SetType(t.buildState(spec, &t.State, spec.Type.Cardinality))
	var structFields []reflect.StructField
	for _, parameter := range t.State {
		var structTag reflect.StructTag
		if parameter.Schema.DataType != "" {
			structTag = reflect.StructTag(fmt.Sprintf(`%v:"%v"`, xreflect.TagTypeName, parameter.Schema.DataType))
		}

		structFields = append(structFields, reflect.StructField{
			Name: parameter.Name,
			Type: parameter.Schema.Type(),
			Tag:  structTag,
		})
	}

	t.StateType = reflect.StructOf(structFields)
}

func (t *Template) buildBodyParameter(spec *Spec, bodyHolder string) *Parameter {
	param := &Parameter{}
	param.Name = spec.Type.Name
	param.Schema = &view.Schema{DataType: spec.Type.Name, Cardinality: spec.Type.Cardinality}
	param.In = &view.Location{Kind: view.KindRequestBody, Name: bodyHolder}
	return param
}

func (t *Template) BuildLogic(spec *Spec, opts ...Option) {
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

func (t *Template) allocateSequence(options *Options, spec *Spec, block *ast.Block) {
	if spec.isAuxiliary {
		return
	}
	if len(spec.Type.pkFields) != 1 {
		return
	}
	if field := spec.Type.pkFields[0]; strings.Contains(field.Schema.DataType, "int") {
		selector := spec.Selector()

		var args = []ast.Expression{ast.NewQuotedLiteral(spec.Table), ast.NewIdent(selector[0]),
			ast.NewQuotedLiteral(strings.TrimLeft(selector.Path(field.Name), "/")),
		}
		if options.IsGoLang() {
			args = append([]ast.Expression{ast.Expression(ast.NewIdent("ctx"))}, args...)
		}
		call := ast.NewCallExpr(ast.NewIdent("sequencer"), "Allocate", args...)
		block.Append(ast.NewStatementExpression(call))
	}

	for _, rel := range spec.Relations {
		t.allocateSequence(options, rel.Spec, block)
	}

}

func (t *Template) indexRecords(options *Options, spec *Spec, block *ast.Block) {
	if spec.isAuxiliary {
		return
	}

	field := spec.Type.pkFields[0]
	holder := t.ParamIndexName(spec.Type.Name, field.Name)
	source := t.ParamName(spec.Type.Name)
	indexBy := ast.NewCallExpr(ast.NewIdent(source), "IndexBy", ast.NewQuotedLiteral(field.Name))
	block.Append(ast.NewAssign(ast.NewIdent(holder), indexBy))
	for _, rel := range spec.Relations {
		t.indexRecords(options, rel.Spec, block)
	}
}

func (t *Template) modifyRecords(options *Options, structPathPrefix string, spec *Spec, cardinality view.Cardinality, parentBlock *ast.Block, rel *Relation) {
	if spec.isAuxiliary {
		return
	}
	if len(spec.Type.pkFields) != 1 {
		return
	}

	structPath := spec.Type.Name
	if structPathPrefix != "" {
		structPath = structPathPrefix + "." + structPath
	}

	switch cardinality {
	case view.One:
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
	case view.Many:
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

func (t *Template) synchronizeRefKeys(x, y string, rel *Relation, block *ast.Block) {
	src := ast.Expression(ast.NewIdent(y))
	if !rel.ParentField.Ptr && rel.KeyField.Ptr {
		src = ast.NewRefExpression(src)
	} else if rel.ParentField.Ptr != rel.KeyField.Ptr {
		src = ast.NewDerefExpression(src)
	}

	assignKey := ast.NewAssign(ast.NewIdent(x), src)
	block.Append(assignKey)
}

func (t *Template) modifyRecord(options *Options, recordPath string, spec *Spec, block *ast.Block) {
	field := spec.Type.pkFields[0]
	fieldPath := recordPath + "." + field.Name
	recordSelector := ast.NewIdent(recordPath)
	fieldSelector := ast.NewIdent(fieldPath)
	var matchCond *ast.Condition

	if options.withUpdate {
		xSelector := ast.NewIdent(t.ParamIndexName(spec.Type.Name, field.Name))

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

func (t *Template) insert(options *Options, selector *ast.Ident, spec *Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")

	args := []ast.Expression{selector, ast.NewQuotedLiteral(spec.Table)}
	if options.IsGoLang() {
		t.swapArgs(args)
	}

	callExpr := ast.NewCallExpr(holder, "Insert", args...)
	block.Append(ast.NewStatementExpression(ast.NewTerminatorExpression(callExpr)))

}

func (t *Template) swapArgs(args []ast.Expression) {
	args[0], args[1] = args[1], args[0]
}

func (t *Template) update(options *Options, selector *ast.Ident, spec *Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")
	args := []ast.Expression{selector, ast.NewQuotedLiteral(spec.Table)}
	if options.IsGoLang() {
		t.swapArgs(args)
	}

	callExpr := ast.NewCallExpr(holder, "Update", args...)
	block.Append(ast.NewStatementExpression(ast.NewTerminatorExpression(callExpr)))
}

func (t *Template) RecordPrefix() string {
	if t.recordPrefix != "" {
		return t.recordPrefix
	}
	return recordPrefix
}

func (t *Template) BuildTypeDef(spec *Spec, wrapperField string) {
	t.TypeDef = spec.TypeDefinition(wrapperField)
	t.ensurePackageImports(t.TypeDef.Package, t.TypeDef.Fields)
	t.ensureTypeImport(spec.Type.Name)
	if wrapperField != "" {
		t.ensureTypeImport(wrapperField)
	}
	t.setResponseBody()
}

func (t *Template) setResponseBody() {
	if t.Config.ResponseBody == nil {
		t.Config.ResponseBody = &option.ResponseBodyConfig{}
	}
	if t.Config.ResponseBody.From == "" {
		t.Config.ResponseBody.From = t.TypeDef.Name
	}
}

func (t *Template) ensureTypeImport(simpleTypeName string) {
	typeName := simpleTypeName
	if t.TypeDef.Package != "" {
		typeName = t.TypeDef.Package + "." + simpleTypeName
	}
	t.Imports.AddType(typeName)
}

func (t *Template) EnsureImports(aType *Type) {
	t.Imports.AddType(aType.TypeName())
	if len(aType.relationFields) == 0 {
		return
	}

	for _, field := range aType.relationFields {
		t.Imports.AddType(aType.ExpandType(field.Schema.DataType))
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

func NewTemplate(config *option.RouteConfig, spec *Spec) *Template {
	return &Template{paramPrefix: paramPrefix, Config: config, Imports: NewImports(), Spec: spec}
}
