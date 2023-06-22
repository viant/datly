package codegen

import (
	_ "embed"
	"encoding/json"
	ast "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

func (t *Template) GenerateDSQL() (string, error) {
	return t.generateContent(ast.Options{Lang: ast.LangVelty})
}

func (t *Template) GenerateGo() (string, error) {
	stateName := t.TypeDef.Name
	notifier := NewMethodNotifier(stateName, t.TypeDef.Type())

	return t.generateContent(ast.Options{
		Lang:         ast.LangGO,
		StateName:    stateName,
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
