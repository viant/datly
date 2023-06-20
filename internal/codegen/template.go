package codegen

import (
	_ "embed"
	"github.com/viant/datly/cmd/option"
	ast2 "github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/sink"
	"strings"
)

type (
	Template struct {
		Config  *option.RouteConfig
		TypeDef *view.TypeDefinition
		Imports
		State
		BusinessLogic *ast2.Block
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

func (t *Template) BuildState(spec *Spec, bodyHolder string) {
	state := State{}
	state.Append(t.buildBodyParameter(spec, bodyHolder))
	t.buildState(spec, &state)
	t.State = state
}

func (t *Template) buildBodyParameter(spec *Spec, bodyHolder string) *Parameter {
	param := &Parameter{}
	param.Name = spec.Type.Name
	param.Schema = &view.Schema{DataType: spec.Type.Name}
	param.In = &view.Location{Kind: view.KindRequestBody, Name: bodyHolder}
	return param
}

func (t *Template) BuildLogic(spec *Spec, opts ...Option) {
	block := ast2.Block{}
	options := &Options{}
	options.apply(opts)
	if options.withInsert {
		t.allocateSequence(options, spec, &block)
	}
	block.AppendEmptyLine()
	t.indexRecords(options, spec, spec.Type.Cardinality, &block)
	t.modifyRecords(options, "Unsafe", spec, spec.Type.Cardinality, &block)
	t.BusinessLogic = &block
}

func (t *Template) allocateSequence(options *Options, spec *Spec, block *ast2.Block) {
	if spec.isAuxiliary {
		return
	}
	if len(spec.Type.pkFields) != 1 {
		return
	}
	if field := spec.Type.pkFields[0]; strings.Contains(field.Schema.DataType, "int") {
		selector := spec.Selector()
		call := ast2.NewCallExpr(ast2.NewIdent("sequencer"), "Allocate", ast2.NewQuotedLiteral(spec.Table), ast2.NewIdent(selector[0]), ast2.NewQuotedLiteral(strings.TrimLeft(selector.Path(field.Name), "/")))
		block.Append(ast2.NewStatementExpression(call))
	}

	for _, rel := range spec.Relations {
		t.allocateSequence(options, rel.Spec, block)
	}

}

func (t *Template) indexRecords(options *Options, spec *Spec, cardinality view.Cardinality, block *ast2.Block) {
	if spec.isAuxiliary {
		return
	}

	if cardinality == view.Many {
		field := spec.Type.pkFields[0]
		holder := t.ParamIndexName(spec.Type.Name, field.Name)
		source := t.ParamName(spec.Type.Name)
		indexBy := ast2.NewCallExpr(ast2.NewIdent(source), "IndexBy", ast2.NewQuotedLiteral(source))
		block.Append(ast2.NewAssign(ast2.NewIdent(holder), indexBy))
	}

	for _, rel := range spec.Relations {
		t.indexRecords(options, rel.Spec, rel.Cardinality, block)
	}
}

func (t *Template) modifyRecords(options *Options, structPathPrefix string, spec *Spec, cardinality view.Cardinality, parentBlock *ast2.Block) {
	if spec.isAuxiliary {
		return
	}
	if len(spec.Type.pkFields) != 1 {
		return
	}
	structPath := structPathPrefix + "." + spec.Type.Name

	switch cardinality {
	case view.One:
		structSelector := ast2.NewIdent(structPath)
		checkValid := ast2.NewCondition(structSelector, ast2.Block{}, nil)
		t.modifyRecord(options, structPath, spec, &checkValid.IFBlock)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, structPath, rel.Spec, rel.Cardinality, &checkValid.IFBlock)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(checkValid)
	case view.Many:
		recordPath := t.RecordName(spec.Type.Name)
		forEach := ast2.NewForEach(ast2.NewIdent(recordPath), ast2.NewIdent(structPath), ast2.Block{})
		t.modifyRecord(options, recordPath, spec, &forEach.Body)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, recordPath, rel.Spec, rel.Cardinality, &forEach.Body)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(forEach)
	}
}

func (t *Template) modifyRecord(options *Options, recordPath string, spec *Spec, block *ast2.Block) {
	field := spec.Type.pkFields[0]
	fieldPath := recordPath + "." + field.Name
	recordSelector := ast2.NewIdent(recordPath)
	fieldSelector := ast2.NewIdent(fieldPath)
	var matchCond *ast2.Condition

	if options.withUpdate {
		xSelector := ast2.NewIdent(t.ParamIndexName(spec.Type.Name, field.Name))
		x := ast2.NewCallExpr(xSelector, "HasKey", fieldSelector)
		expr := ast2.NewBinary(x, "==", ast2.NewLiteral("true"))
		matchCond = ast2.NewCondition(expr, ast2.Block{}, nil)
		t.update(options, recordSelector, spec, &matchCond.IFBlock)
	}
	if options.withInsert {
		insertBlock := ast2.Block{}
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

func (t *Template) insert(options *Options, selector *ast2.Ident, spec *Spec, block *ast2.Block) {
	if options.withDML {
		return
	}
	holder := ast2.NewIdent("sql")
	block.Append(ast2.NewStatementExpression(ast2.NewCallExpr(holder, "Insert", selector, ast2.NewQuotedLiteral(spec.Table))))

}

func (t *Template) update(options *Options, selector *ast2.Ident, spec *Spec, block *ast2.Block) {
	if options.withDML {
		return
	}
	holder := ast2.NewIdent("sql")
	block.Append(ast2.NewStatementExpression(ast2.NewCallExpr(holder, "Update", selector, ast2.NewQuotedLiteral(spec.Table))))
}

func (t *Template) RecordPrefix() string {
	if t.recordPrefix != "" {
		return t.recordPrefix
	}
	return recordPrefix
}

func (t *Template) BuildTypeDef(spec *Spec, wrapperField string) {
	t.TypeDef = spec.TypeDefinition(wrapperField)
	t.ensureImports(t.TypeDef.Package, t.TypeDef.Fields)

}

func (t *Template) ensureImports(defaultPkg string, fields []*view.Field) {
	for _, field := range fields {
		if len(field.Fields) > 0 {
			t.ensureImports(defaultPkg, field.Fields)
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

func NewTemplate(config *option.RouteConfig) *Template {
	return &Template{paramPrefix: paramPrefix, Config: config, Imports: NewImports()}
}
