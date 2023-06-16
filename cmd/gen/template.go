package gen

import (
	_ "embed"
	"encoding/json"
	"github.com/viant/datly/cmd/gen/ast"
	"github.com/viant/datly/cmd/option"
	"strings"
)

type (
	Template struct {
		Config option.RouteConfig
		Imports
		State
		BusinessLogic *ast.Block
	}
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

func (s Template) GenerateDSQL() (string, error) {
	config, err := json.Marshal(s.Config)
	if err != nil {
		return "", err
	}
	tmpl := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	tmpl = strings.Replace(tmpl, "$Import", s.Imports.TypeImports(), 1)
	tmpl = strings.Replace(tmpl, "$Declration", s.State.GenerateDSQLDeclration(), 1)
	builder := ast.NewBuilder(ast.Options{Lang: "dsql"})
	err = s.BusinessLogic.Generate(builder)
	if err != nil {
		return "", err
	}
	tmpl = strings.Replace(tmpl, "$BusinessLogic", builder.String(), 1)
	return tmpl, nil
}
