package codegen

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/options"
	ast "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

//go:embed tmpl/handler.gox
var goTemplate string

func (t *Template) GenerateDSQL(opts ...Option) (string, error) {
	options := Options{}
	options.apply(opts)

	return t.generateContent(ast.Options{Lang: ast.LangVelty})
}

func (t *Template) GenerateGo(opts *options.Gen) (string, error) {
	methodHandler := NewMethodHandler(t.StateType)
	content, err := t.generateContent(ast.Options{
		Lang:              ast.LangGO,
		CallNotifier:      methodHandler.OnCallExpr,
		AssignNotifier:    methodHandler.OnAssign,
		SliceItemNotifier: methodHandler.OnSliceItem,
	})

	fmt.Println(t.StateType.String())

	if err != nil {
		return "", err
	}

	result := strings.Replace(goTemplate, "$Package", opts.Package, 1)
	result = strings.Replace(result, "$SnippetBefore", methodHandler.builder.String(), 1)
	result = strings.Replace(result, "$MethodContent", content, 1)
	return result, nil
}

func (t *Template) generateContent(options ast.Options) (string, error) {
	config, err := json.Marshal(t.Config)
	if err != nil {
		return "", err
	}
	code := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	code = strings.Replace(code, "$Imports", t.Imports.TypeImports(), 1)
	code = strings.Replace(code, "$Declaration", t.dsqlParameterDeclaration(), 1)

	builder := ast.NewBuilder(options)
	if t.BusinessLogic != nil {
		err = t.BusinessLogic.Generate(builder)
		if err != nil {
			return "", err
		}
	}
	code = strings.Replace(code, "$BusinessLogic", builder.String(), 1)
	return code, nil
}
