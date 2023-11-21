package repository

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology/format/text"
	"github.com/viant/toolbox/data"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

//go:embed codegen/contract.gox
var contractInit string

func (c *Component) GenerateOutputCode(withEmbed bool, embeds map[string]string) string {
	builder := strings.Builder{}
	input := c.Input.Type.Type()
	registry := c.TypeRegistry()

	if viewParameter := c.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); viewParameter != nil {
		aTag := &tags.Tag{}
		aTag.SQL = tags.ViewSQL(c.View.Template.Source)
		aTag.View = &tags.View{Name: c.View.Name}
		viewParameter.Tag = string(aTag.UpdateTag(reflect.StructTag(viewParameter.Tag)))
	}

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

	componentName := state.SanitizeTypeName(c.View.Name)
	var options = []xreflect.Option{
		xreflect.WithPackage(statePkg),
		xreflect.WithTypes(xreflect.NewType(componentName+"Output", xreflect.WithReflectType(output))),
		xreflect.WithPackageTypes(packageTypes...),
		xreflect.WithRewriteDoc(),
		xreflect.WithImportModule(importModules),
	}
	if withEmbed {
		replacer := data.NewMap()
		replacer.Put("Name", componentName)
		replacer.Put("URI", c.URI)
		replacer.Put("Method", c.Method)

		defineComponentFunc := replacer.ExpandAsText(contractInit)
		options = append(options,
			xreflect.WithImports([]string{"embed", "github.com/viant/datly",
				"fmt",
				"context",
				"reflect",
				"github.com/viant/datly/repository",
				"github.com/viant/datly/repository/contract"}),
			xreflect.WithSQLRewrite(embeds),
			xreflect.WithLinkTag(true),
			xreflect.WithEmbeddedFormatter(func(s string) string {
				return text.CaseFormatUpperCamel.Format(s, text.CaseFormatLowerUnderscore)
			}),
			xreflect.WithSnippetBefore(fmt.Sprintf(`
//go:embed sql/*.sql
var %vFS embed.FS

`, componentName)),
			xreflect.WithVeltyTag(false),
			xreflect.WithSnippetAfter(defineComponentFunc))
	}

	inputState := xreflect.GenerateStruct(componentName+"Input", input.Type(), options...)
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
