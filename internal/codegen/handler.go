package codegen

import (
	_ "embed"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view"
	"strings"
)

//go:embed tmpl/handler/handler.gox
var handlerTemplate string

func (t *Template) GenerateHandler(opts *options.Generate, info *plugin.Info) (string, string, error) {
	fields, localVariableDeclaration := t.State.HandlerLocalVariables()
	t.Config.HandlerType = opts.HandlerType()
	t.Config.StateType = opts.StateType()
	t.Config.ResponseBody = nil

	index := NewIndexGenerator(t.State)
	builder := ast.NewBuilder(ast.Options{
		Lang:               ast.LangGO,
		CallNotifier:       index.OnCallExpr,
		AssignNotifier:     index.OnAssign,
		SliceItemNotifier:  index.OnSliceItem,
		WithLowerCaseIdent: true,
		OnIfNotifier:       index.OnConditionStmt,
	}, fields...)

	if err := t.BusinessLogic.Generate(builder); err != nil {
		return "", "", err
	}

	indexContent := strings.Replace(goIndexTmpl, "$PackageName", opts.Package, 1)
	indexContent = strings.ReplaceAll(indexContent, "$Content", index.builder.String())

	handlerContent := strings.Replace(handlerTemplate, "$Package", opts.Package, 1)
	handlerContent = strings.Replace(handlerContent, "$LocalVariable", localVariableDeclaration, 1)

	registry := &customTypeRegistry{}
	registry.register("Handler")
	registerTypes := registry.stringify()
	handlerContent = strings.Replace(handlerContent, "$RegisterTypes", registerTypes, 1)
	imports := inference.NewImports()
	imports.AddPackage(info.ChecksumPkg())
	imports.AddPackage(info.TypeCorePkg())
	imports.AddPackage("reflect")
	handlerContent = strings.Replace(handlerContent, "$RawImports", imports.RawImports(), 1)

	info.ChecksumPkg()
	logic := builder.String()
	handlerContent = strings.Replace(handlerContent, "$BusinessLogic", logic, 1)
	bodyParam := t.State.FilterByKind(view.KindRequestBody)[0]
	handlerContent = strings.Replace(handlerContent, "$Response", "state."+bodyParam.Name, 1)
	return handlerContent, indexContent, nil
}
