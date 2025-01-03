package codegen

import (
	_ "embed"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"strings"
)

//go:embed tmpl/handler/handler.gox
var handlerTemplate string

//go:embed tmpl/handler/handler_init.gox
var handlerInitTemplate string

func (t *Template) GenerateHandler(opts *options.Generate, info *plugin.Info) (string, string, string, error) {
	fields, localVariableDeclaration := t.State.HandlerLocalVariables()
	t.Config.Type = opts.HandlerType(t.MethodFragment)
	t.Config.InputType = opts.InputType(t.MethodFragment)
	t.Config.OutputType = opts.OutputType(t.MethodFragment)

	index := NewIndexGenerator(t.State)
	t.IndexGenerator = index
	builder := ast.NewBuilder(ast.Options{
		Lang:               ast.LangGO,
		StateName:          "i",
		CallNotifier:       index.OnCallExpr,
		AssignNotifier:     index.OnAssign,
		SliceItemNotifier:  index.OnSliceItem,
		WithLowerCaseIdent: true,
		OnIfNotifier:       index.OnConditionStmt,
	}, fields...)

	if err := t.BusinessLogic.Generate(builder); err != nil {
		return "", "", "", err
	}
	if builder.IndexByCode != nil {
		t.IndexByCode = builder.IndexByCode.String()
	}
	indexContent := t.expandOptions(goIndexTmpl, opts)
	indexContent = strings.ReplaceAll(indexContent, "$Content", index.builder.String())

	handlerContent := t.expandOptions(handlerTemplate, opts)
	handlerContent = strings.Replace(handlerContent, "$LocalVariable", localVariableDeclaration, 1)

	registry := &customTypeRegistry{}
	registry.register("Handler")
	registerTypes := registry.stringify()
	handlerContent = strings.ReplaceAll(handlerContent, "$RegisterTypes", registerTypes)
	imports := inference.NewImports()
	imports.AddPackage(info.ChecksumPkg())
	imports.AddPackage(info.TypeCorePkg())
	imports.AddPackage("reflect")
	handlerContent = strings.Replace(handlerContent, "$RawImports", imports.RawImports(), 1)

	info.ChecksumPkg()
	logic := builder.String()
	handlerContent = strings.Replace(handlerContent, "$BusinessLogic", logic, 1)
	handlerContent = strings.ReplaceAll(handlerContent, "$DataField", t.InputDataField())
	handlerContent = strings.ReplaceAll(handlerContent, "$OutputField", t.OutputDataField())

	handlerContent = t.expandOptions(handlerContent, opts)

	handlerInit := t.expandOptions(handlerInitTemplate, opts)
	return handlerContent, indexContent, handlerInit, nil
}

func (t *Template) expandOptions(text string, opts *options.Generate) string {
	text = strings.ReplaceAll(text, "$PackageName", strings.ToLower(t.MethodFragment))
	if t.Resource != nil {
		text = strings.ReplaceAll(text, "$Method", t.Resource.Rule.Method)
		text = strings.ReplaceAll(text, "$URI", t.Resource.Rule.URI)
	}
	text = strings.ReplaceAll(text, "${Prefix}", t.Prefix)
	text = strings.ReplaceAll(text, "${MethodFragment}", t.MethodFragment)
	return text
}
