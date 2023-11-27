package repository

import (
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
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
		packageTypes = append(packageTypes, xreflect.NewType(def.Name, xreflect.WithModulePath(def.ModulePath), xreflect.WithModulePath(def.ModulePath), xreflect.WithPackage(def.Package), xreflect.WithTypeDefinition(def.DataType)))
	}

	componentName := state.SanitizeTypeName(c.View.Name)
	embedURI := text.CaseFormatUpperCamel.Format(componentName, text.CaseFormatLowerUnderscore)
	var options = []xreflect.Option{
		xreflect.WithPackage(statePkg),
		xreflect.WithTypes(xreflect.NewType(componentName+"Output", xreflect.WithReflectType(output))),
		xreflect.WithPackageTypes(packageTypes...),
		xreflect.WithSkipFieldType(func(field *reflect.StructField) bool {
			if aTag, _ := tags.Parse(field.Tag, nil, tags.ParameterTag); aTag != nil && aTag.Parameter != nil {
				return aTag.Parameter.Kind == string(state.KindComponent)
			}
			return false
		}),
		xreflect.WithOnStructField(c.adjustStructField(embedURI, embeds, withEmbed)),
		xreflect.WithImportModule(importModules),
		xreflect.WithRegistry(c.types),
	}

	if withEmbed {
		replacer := data.NewMap()
		replacer.Put("Name", componentName)
		replacer.Put("URI", c.URI)
		replacer.Put("Method", c.Method)
		defineComponentFunc := replacer.ExpandAsText(contractInit)
		options = append(options,
			xreflect.WithImports(c.generatorImports()),
			xreflect.WithSnippetBefore(c.embedTemplate(embedURI, componentName)),
			xreflect.WithSnippetAfter(defineComponentFunc))
	}

	inputState := xreflect.GenerateStruct(componentName+"Input", input.Type(), options...)
	builder.WriteString(inputState)
	result := builder.String()
	result = c.View.Resource().ReverseSubstitutes(result)
	return result
}

func (c *Component) embedTemplate(embedURI string, componentName string) string {
	return fmt.Sprintf(`
//go:embed %v/*.sql
var %vFS embed.FS

`, embedURI, componentName)
}

func (c *Component) generatorImports() []string {
	return []string{"embed",
		"github.com/viant/datly",
		"fmt",
		"context",
		"reflect",
		"github.com/viant/datly/repository",
		"github.com/viant/datly/repository/contract"}
}

func (c *Component) adjustStructField(embedURI string, embeds map[string]string, generateContract bool) func(aField *reflect.StructField, tag *string, typeName *string, doc *string) {
	return func(aField *reflect.StructField, tag, typeName, doc *string) {
		fieldTag := *tag
		if !generateContract {
			fieldTag, _ = xreflect.RemoveTag(fieldTag, "on")
		} else {
			fieldTag, _ = xreflect.RemoveTag(fieldTag, "velty")
		}
		fieldTag, value := xreflect.RemoveTag(fieldTag, "sql")
		if value != "" && generateContract {
			name := *typeName
			setter.SetStringIfEmpty(&name, aField.Name)
			key := text.CaseFormatUpperCamel.Format(name, text.CaseFormatLowerUnderscore) + ".sql"
			embeds[key] = value
			fieldTag += fmt.Sprintf(` sql:"uri=%v/`+key+`" `, embedURI)
		}
		if generateContract {
			if fieldTag, value = xreflect.RemoveTag(fieldTag, "anonymous"); value == "true" {
				aField.Anonymous = true
			}
		}
		if fieldTag, value = xreflect.RemoveTag(fieldTag, "doc"); value != "" {
			*doc = aField.Name + " " + value
		}
		*tag = fieldTag
	}
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
