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
	config, err := json.Marshal(t.Config)
	if err != nil {
		return "", err
	}
	tmpl := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	tmpl = strings.Replace(tmpl, "$Imports", t.Imports.TypeImports(), 1)
	tmpl = strings.Replace(tmpl, "$Declaration", t.dsqlParameterDeclaration(), 1)

	builder := ast2.NewBuilder(ast2.Options{Lang: "dsql"})
	err = t.BusinessLogic.Generate(builder)
	if err != nil {
		return "", err
	}
	tmpl = strings.Replace(tmpl, "$BusinessLogic", builder.String(), 1)
	return tmpl, nil
}
