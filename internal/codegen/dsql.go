package codegen

import (
	_ "embed"
	"encoding/json"
	"github.com/viant/datly/cmd/options"
	ast "github.com/viant/datly/internal/codegen/ast"
	"strings"
)

//go:embed tmpl/dsql.sqlx
var dsqlTemplate string

//go:embed tmpl/handler/handler.gox
var handlerTemplate string

//go:embed tmpl/handler/index.gox
var goIndexTmpl string

func (t *Template) GenerateDSQL(opts ...Option) (string, error) {
	options := Options{}
	options.apply(opts)
	astOptions := options.astOption()
	return t.generateDSQL(astOptions)
}

func (t *Template) GenerateHandler(opts *options.Gen) (string, string, error) {
	fieldNames, localVariableDeclaration := t.State.localStateBasedVariableDefinition()

	index := NewIndexGenerator(t.State)
	builder := ast.NewBuilder(ast.Options{
		Lang:               ast.LangGO,
		CallNotifier:       index.OnCallExpr,
		AssignNotifier:     index.OnAssign,
		SliceItemNotifier:  index.OnSliceItem,
		WithLowerCaseIdent: true,
		OnIfNotifier:       index.OnConditionStmt,
	})

	if err := t.BusinessLogic.Generate(builder); err != nil {
		return "", "", err
	}

	indexContent := strings.Replace(goIndexTmpl, "$PackageName", opts.Package, 1)
	indexContent = strings.ReplaceAll(indexContent, "$Content", index.builder.String())

	handlerContent := strings.Replace(handlerTemplate, "$Package", opts.Package, 1)
	handlerContent = strings.Replace(handlerContent, "$LocalVariable", localVariableDeclaration, 1)

	logic := builder.String()
	handlerContent = strings.Replace(handlerContent, "$BusinessLogic", logic, 1)
	return handlerContent, indexContent, nil
}

func (t *Template) generateDSQL(options ast.Options) (string, error) {
	config, err := json.Marshal(t.Config)
	if err != nil {
		return "", err
	}
	code := strings.Replace(dsqlTemplate, "$RouteOption", string(config), 1)
	var imports, declaration, businessLogic string
	if options.Lang == ast.LangVelty {
		imports = t.Imports.TypeImports()
		declaration = t.dsqlParameterDeclaration()
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
