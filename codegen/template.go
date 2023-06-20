package codegen

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/option"
	ast "github.com/viant/datly/codegen/ast"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/metadata/sink"
	"reflect"
	"strings"
)

type (
	Template struct {
		Config option.RouteConfig
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

func (t *Template) buildState(spec *Spec, state *State) {
	if param := t.buildPathParameterIfNeeded(spec); param != nil {
		state.Append(param)
	}
	state.Append(t.buildDataViewParameter(spec))
	for _, rel := range spec.Relations {
		t.buildState(rel.Spec, state)
	}
}

func (t *Template) buildPathParameterIfNeeded(spec *Spec) *Parameter {
	selector := spec.Selector()
	field, SQL := spec.pkStructQL(selector)
	if SQL == "" {
		return nil
	}
	param := &Parameter{}
	parameterNamer := t.ColumnParameterNamer(selector)
	param.Name = parameterNamer(field.Column)
	param.SQL = SQL
	param.In = &view.Location{Kind: view.KindParam, Name: selector[0]}
	var paramType = reflect.StructOf([]reflect.StructField{{Name: "Values", Type: reflect.SliceOf(field.Schema.Type())}})
	param.Schema = view.NewSchema(paramType)
	return param
}

func (t *Template) buildDataViewParameter(spec *Spec) *Parameter {
	param := &Parameter{}
	param.Name = t.ParamName(spec.Type.Name)
	param.Schema = &view.Schema{DataType: spec.Type.Name, Cardinality: view.One}
	param.Schema.Cardinality = spec.Type.Cardinality
	param.In = &view.Location{Kind: view.KindDataView, Name: param.Name}
	param.SQL = spec.viewSQL(t.ColumnParameterNamer(spec.Selector()))
	return param
}

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

func (t *Template) GenerateDSQL() (string, error) {
	config, err := json.Marshal(t.Config)
	if err != nil {
		return "", err
	}
	tmpl := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	tmpl = strings.Replace(tmpl, "$Import", t.Imports.TypeImports(), 1)
	tmpl = strings.Replace(tmpl, "$Declaration", t.State.GenerateDSQLDeclration(), 1)
	builder := ast.NewBuilder(ast.Options{Lang: "dsql"})
	err = t.BusinessLogic.Generate(builder)
	if err != nil {
		return "", err
	}
	tmpl = strings.Replace(tmpl, "$BusinessLogic", builder.String(), 1)
	return tmpl, nil
}

func (t *Template) BuildLogic(spec *Spec, opts ...Option) {
	block := ast.Block{}
	options := &Options{}
	options.apply(opts)
	if options.withInsert {
		t.allocateSequence(options, spec, &block)
	}
	block.AppendEmptyLine()
	t.indexRecords(options, spec, spec.Type.Cardinality, &block)
	t.modifyRecords(options, "Unsafe", spec, spec.Type.Cardinality, &block)

	builder := ast.NewBuilder(ast.Options{Lang: "dsql"})
	block.Generate(builder)

	fmt.Printf("GENERATED: %v\n", builder.String())

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

func (t *Template) indexRecords(options *Options, spec *Spec, cardinality view.Cardinality, block *ast.Block) {
	if spec.isAuxiliary {
		return
	}

	if cardinality == view.Many {
		field := spec.Type.pkFields[0]
		holder := t.ParamIndexName(spec.Type.Name, field.Name)
		source := t.ParamName(spec.Type.Name)
		indexBy := ast.NewCallExpr(ast.NewIdent(source), "IndexBy", ast.NewQuotedLiteral(source))
		block.Append(ast.NewAssign(ast.NewIdent(holder), indexBy))
	}

	for _, rel := range spec.Relations {
		t.indexRecords(options, rel.Spec, rel.Cardinality, block)
	}
}

func (t *Template) modifyRecords(options *Options, structPathPrefix string, spec *Spec, cardinality view.Cardinality, parentBlock *ast.Block) {
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
		t.modifyRecord(options, structPath, spec, &checkValid.IFBlock)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, structPath, rel.Spec, rel.Cardinality, &checkValid.IFBlock)
		}
		parentBlock.AppendEmptyLine()
		parentBlock.Append(checkValid)
	case view.Many:
		recordPath := t.RecordName(spec.Type.Name)
		forEach := ast.NewForEach(ast.NewIdent(recordPath), ast.NewIdent(structPath), ast.Block{})
		t.modifyRecord(options, recordPath, spec, &forEach.Body)
		for _, rel := range spec.Relations {
			t.modifyRecords(options, recordPath, rel.Spec, rel.Cardinality, &forEach.Body)
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
	block.Append(ast.NewStatementExpression(ast.NewCallExpr(holder, "Insert", selector, ast.NewQuotedLiteral(spec.Table))))

}

func (t *Template) update(options *Options, selector *ast.Ident, spec *Spec, block *ast.Block) {
	if options.withDML {
		return
	}
	holder := ast.NewIdent("sql")
	block.Append(ast.NewStatementExpression(ast.NewCallExpr(holder, "Update", selector, ast.NewQuotedLiteral(spec.Table))))
}

func (t *Template) RecordPrefix() string {
	if t.recordPrefix != "" {
		return t.recordPrefix
	}
	return recordPrefix
}

func NewTemplate() *Template {
	return &Template{paramPrefix: paramPrefix}
}
