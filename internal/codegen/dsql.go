package codegen

import (
	_ "embed"
	"encoding/json"
	ast "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

//go:embed tmpl/handler/index.gox
var goIndexTmpl string

func (t *Template) GenerateDSQL(opts ...Option) (string, error) {
	options := Options{}
	options.apply(opts)
	astOptions := options.astOption()
	return t.generateDSQL(astOptions)
}

func (t *Template) generateDSQL(options ast.Options) (string, error) {
	config := t.Config.DSQLSetting()
	configContent, err := json.Marshal(config)
	if err != nil {
		return "", nil
	}
	code := strings.Replace(dsqlTemplate, "$RouteOption", string(configContent), 1)
	var imports, declaration, businessLogic string
	if options.Lang == ast.LangVelty {
		imports = t.Imports.TypeImports()
		declaration = t.State.DsqlParameterDeclaration()
		builder := ast.NewBuilder(options)
		if t.BusinessLogic != nil {
			err = t.BusinessLogic.Generate(builder)
			if err != nil {
				return "", err
			}
		}
		businessLogic = builder.String()
	}
	code = strings.Replace(code, "$Imports", imports, 1)
	code = strings.Replace(code, "$Declaration", declaration, 1)
	code = strings.Replace(code, "$BusinessLogic", businessLogic, 1)
	return code, nil
}
