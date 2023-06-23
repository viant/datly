package codegen

import (
	_ "embed"
	"github.com/viant/datly/cmd/option"
	ast "github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/sink"
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
	}

	ColumnParameterNamer func(column *sink.Column) string
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
	return func(column *sink.Column) string {
		return prefix + column.Name
	}
}

func (t *Template) BuildState(spec *Spec, bodyHolder string, opts ...Option) {
	t.State = State{}
	options := &Options{}
	options.apply(opts)
	t.State.Append(t.buildBodyParameter(spec, bodyHolder))
	if options.isInsertOnly() {
		return
	}
	t.buildState(spec, &t.State, spec.Type.Cardinality)
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
	t.modifyRecords(options, "Unsafe", spec, spec.Type.Cardinality, &block, nil)
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
		call := ast.NewCallExpr(ast.NewIdent("sequencer"), "Allocate", ast.NewQuotedLiteral(spec.Table), ast.NewIdent(selector[0]), ast.NewQuotedLiteral(strings.TrimLeft(selector.Path(field.Name), "/")))
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
	structPath := structPathPrefix + "." + spec.Type.Name

	switch cardinality {
	case view.One:
		structSelector := ast.NewIdent(structPath)
		checkValid := ast.NewCondition(structSelector, ast.Block{}, nil)

		if rel != nil {
			parentSelector := structPathPrefix + "." + rel.ParentField.Name
			holder := structPathPrefix + "." + rel.Name + "." + rel.KeyField.Name
			assignKey := ast.NewAssign(ast.NewIdent(holder), ast.NewIdent(parentSelector))
			checkValid.IFBlock.Append(assignKey)
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

		if rel != nil {
			parentSelector := structPathPrefix + "." + rel.ParentField.Name
			holder := recordPath + "." + rel.KeyField.Name
			assignKey := ast.NewAssign(ast.NewIdent(holder), ast.NewIdent(parentSelector))
			forEach.Body.Append(assignKey)
		}

		t.modifyRecord(options, recordPath, spec, &forEach.Body)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, recordPath, rel.Spec, rel.Cardinality, &forEach.Body, rel)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(forEach)
	}
}

func (t *Template) modifyRecord(options *Options, recordPath string, spec *Spec, block *ast.Block) {
	field := spec.Type.pkFields[0]
	fieldPath := recordPath + "." + field.Name
	recordSelector := ast.NewIdent(recordPath)
	fieldSelector := ast.NewIdent(fieldPath)
	var matchCond *ast.Condition

	if options.withUpdate {
		xSelector := ast.NewIdent(t.ParamIndexName(spec.Type.Name, field.Name))
		x := ast.NewCallExpr(xSelector, "HasKey", fieldSelector)
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
	callExpr := ast.NewCallExpr(holder, "Insert", selector, ast.NewQuotedLiteral(spec.Table))
	block.Append(ast.NewStatementExpression(ast.NewTerminatorExpression(callExpr)))

}

func (t *Template) update(options *Options, selector *ast.Ident, spec *Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")
	callExpr := ast.NewCallExpr(holder, "Update", selector, ast.NewQuotedLiteral(spec.Table))
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

}

func (t *Template) ensureTypeImport(simpleTypeName string) {
	typeName := simpleTypeName
	if t.TypeDef.Package != "" {
		typeName = t.TypeDef.Package + "." + simpleTypeName
	}
	t.Imports.AddType(typeName)
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
