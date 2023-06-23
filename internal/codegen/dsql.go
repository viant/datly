package codegen

import (
	_ "embed"
	"encoding/json"
	"fmt"
	ast "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

func (t *Template) GenerateDSQL() (string, error) {
	generateContent, err2 := t.GenerateGo()
	fmt.Println(generateContent, err2)

	content, err := t.generateContent(ast.Options{Lang: ast.LangVelty})
	return content, err
}

func (t *Template) GenerateGo() (string, error) {
	fmt.Println(t.StateType.String())
	notifier := NewMethodNotifier(t.StateType)

	return t.generateContent(ast.Options{
		Lang:         ast.LangGO,
		CallNotifier: notifier.OnCallExpr,
	})
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
	err = t.BusinessLogic.Generate(builder)
	if err != nil {
		return "", err
	}
	code = strings.Replace(code, "$BusinessLogic", builder.String(), 1)
	return code, nil
}
