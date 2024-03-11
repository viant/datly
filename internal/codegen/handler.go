package codegen

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view/state"
	"strings"
)

//go:embed tmpl/handler/handler.gox
var handlerTemplate string

//go:embed tmpl/handler/handler_init.gox
var handlerInitTemplate string

func (t *Template) GenerateHandler(opts *options.Generate, info *plugin.Info) (string, string, string, error) {
	fields, localVariableDeclaration := t.State.HandlerLocalVariables()
	t.Config.Type = opts.HandlerType(t.Prefix, t.MethodFragment)
	t.Config.InputType = opts.InputType(t.Prefix, t.MethodFragment)
	t.Config.OutputType = opts.OutputType(t.Prefix, t.MethodFragment)
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
		return "", "", "", err
	}

	indexContent := t.expandOptions(goIndexTmpl, opts)
	indexContent = strings.ReplaceAll(indexContent, "$Content", index.builder.String())

	handlerContent := t.expandOptions(handlerTemplate, opts)
	handlerContent = strings.Replace(handlerContent, "$LocalVariable", localVariableDeclaration, 1)

	registry := &customTypeRegistry{}
	registry.register(t.Prefix + t.MethodFragment + "Handler")
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

	bodyParam := t.State.FilterByKind(state.KindRequestBody)[0]

	responseSnippet := `
    return $Response, nil
`
	if t.OutputType != nil && t.BodyParameter != nil {
		responseSnippet = fmt.Sprintf(`
    response := ${Prefix}${MethodFragment}Output{}
	response.%v = %v
	return response, nil
`, t.BodyParameter.Name, "input."+bodyParam.Name)
	}

	responseSnippet = strings.Replace(responseSnippet, "$Response", "input."+bodyParam.Name, 1)
	handlerContent = strings.Replace(handlerContent, "$ResponseCode", responseSnippet, 1)
	handlerContent = t.expandOptions(handlerContent, opts)

	handlerInit := t.expandOptions(handlerInitTemplate, opts)
	return handlerContent, indexContent, handlerInit, nil
}

func (t *Template) expandOptions(text string, opts *options.Generate) string {
	text = strings.ReplaceAll(text, "$PackageName", opts.Package())
	text = strings.ReplaceAll(text, "${Prefix}", t.Prefix)
	text = strings.ReplaceAll(text, "${MethodFragment}", t.MethodFragment)
	if t.Resource != nil {
		text = strings.ReplaceAll(text, "$Method", t.Resource.Rule.Method)
		text = strings.ReplaceAll(text, "$URI", t.Resource.Rule.URI)
	}
	return text
}
