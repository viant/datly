package repository

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology/format/text"
	"github.com/viant/xreflect"
	"strings"
)

//go:embed codegen/contract.gox
var contractInit string

func (c *Component) GenerateOutputCode(withEmbed bool, embeds map[string]string) string {
	builder := strings.Builder{}
	input := c.Input.Type.Type()
	registry := c.TypeRegistry()
	output, _ := c.Output.Type.Parameters.ReflectType("", registry.Lookup, state.WithRelation(), state.WithSQL(), state.WithVelty(false))
	var packageTypes []*xreflect.Type
	var importModules = map[string]string{}
	statePkg := c.Output.Type.Package

	setter.SetStringIfEmpty(&statePkg, "state")
	inPackageComponentTypes := indexComponentPackageTypes(c, statePkg)
	for _, def := range c.View.TypeDefinitions() {
		if inPackageComponentTypes[def.Name] {
			continue
		}
		if def.Package != "" && c.ModulePath != "" && strings.Contains(def.DataType, " ") { //complex type
			importModules[def.Package] = c.ModulePath
		}
		packageTypes = append(packageTypes, xreflect.NewType(def.Name, xreflect.WithPackage(def.Package), xreflect.WithTypeDefinition(def.DataType)))
	}

	prefix := c.View.Name

	var options = []xreflect.Option{
		xreflect.WithPackage(statePkg),
		xreflect.WithTypes(xreflect.NewType(prefix+"Output", xreflect.WithReflectType(output))),
		xreflect.WithPackageTypes(packageTypes...),
		xreflect.WithRewriteDoc(),
		xreflect.WithImportModule(importModules),
	}
	if withEmbed {
		options = append(options,
			xreflect.WithImports([]string{"embed", "github.com/viant/datly"}),
			xreflect.WithSQLRewrite(embeds),
			xreflect.WithLinkTag(true),
			xreflect.WithEmbeddedFormatter(func(s string) string {
				return text.CaseFormatUpperCamel.Format(s, text.CaseFormatLowerUnderscore)
			}),
			xreflect.WithSnippetBefore(`
//go:embed sql/*.sql
var embedFS embed.FS

`),

			xreflect.WithVeltyTag(false),
			xreflect.WithSnippetAfter(fmt.Sprintf(`
func defineView(datly *datly.Service) {
	%v
}
`, contractInit)))

	}

	inputState := xreflect.GenerateStruct(prefix+"Input", input.Type(), options...)
	builder.WriteString(inputState)
	result := builder.String()
	result = c.View.Resource().ReverseSubstitutes(result)
	return result
}

func indexComponentPackageTypes(component *Component, inPkg string) map[string]bool {
	thisPackageTypes := map[string]bool{}
	for _, def := range component.View.TypeDefinitions() {
		if def.Package == inPkg {
			thisPackageTypes[def.Name] = true
		}
	}
	return thisPackageTypes
}