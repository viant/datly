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
var handlerTemplate string

//go:embed tmpl/index.gox
var indexTemplate string

func (t *Template) GenerateDSQL(opts ...Option) (string, error) {
	options := Options{}
	options.apply(opts)
	astOptions := options.astOption()
	return t.generateDSQL(astOptions)
}

func (t *Template) GenerateHandler(opts *options.Gen) (string, string, error) {
	index := NewIndexGenerator(t.StateType)
	content, err := t.generateDSQL(ast.Options{
		Lang:              ast.LangGO,
		CallNotifier:      index.OnCallExpr,
		AssignNotifier:    index.OnAssign,
		SliceItemNotifier: index.OnSliceItem,
	})

	fmt.Println(t.StateType.String())
	if err != nil {
		return "", "", err
	}

	indexContent := strings.Replace(indexTemplate, "$Package", opts.Package, 1)
	indexLogic := index.builder.String()
	indexContent = strings.Replace(indexContent, "$TypeDeclaration", "", 1)
	indexContent = strings.Replace(indexContent, "$IndexLogic", indexContent, 1)

	localVariableDeclaration := ""
	indexing := ""
	fmt.Print("%v\n", indexLogic)

	handlerContent := strings.Replace(handlerTemplate, "$Package", opts.Package, 1)
	handlerContent = strings.Replace(handlerContent, "$LocalVariable", localVariableDeclaration, 1)
	handlerContent = strings.Replace(handlerContent, "$Indexing", localVariableDeclaration, 1)
	handlerContent = strings.Replace(handlerContent, "$BusinessLogic", content, 1)
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
