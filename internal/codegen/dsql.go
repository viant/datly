package codegen

import (
	_ "embed"
	"encoding/json"
	ast2 "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

func (t *Template) GenerateDSQL() (string, error) {
	return t.generateContent(ast2.Options{Lang: ast2.LangDSQL})
}

func (t *Template) GenerateGo() (string, error) {
	stateName := t.TypeDef.Name
	notifier := NewMethodNotifier(stateName, t.TypeDef.Type())

	return t.generateContent(ast2.Options{
		Lang:         ast2.LangGO,
		StateName:    stateName,
		CallNotifier: notifier.OnCallExpr,
	})
}

func (t *Template) generateContent(options ast2.Options) (string, error) {
	config, err := json.Marshal(t.Config)
	if err != nil {
		return "", err
	}
	tmpl := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	tmpl = strings.Replace(tmpl, "$Imports", t.Imports.TypeImports(), 1)
	tmpl = strings.Replace(tmpl, "$Declaration", t.dsqlParameterDeclaration(), 1)

	builder := ast2.NewBuilder(options)
	err = t.BusinessLogic.Generate(builder)
	if err != nil {
		return "", err
	}
	tmpl = strings.Replace(tmpl, "$BusinessLogic", builder.String(), 1)
	return tmpl, nil
}
