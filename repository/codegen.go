package repository

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"github.com/viant/toolbox/data"
	"github.com/viant/xreflect"
	"path"
	"reflect"
	"strconv"
	"strings"
)

var generatorKey string

func isGeneratorContext(ctx context.Context) bool {
	return ctx.Value(generatorKey) != nil
}

func WithGeneratorContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, generatorKey, true)
}

//go:embed codegen/contract.gox
var contractInit string

//go:embed codegen/register.gox
var registerInit string

func (c *Component) GenerateOutputCode(ctx context.Context, withDefineComponent, withEmbed bool, embeds map[string]string, namedResources ...string) string {
	builder := strings.Builder{}
	input := c.Input.Type.Type()
	registry := c.TypeRegistry()

	if viewParameter := c.Output.Type.Parameters.LookupByLocation(state.KindOutput, "view"); viewParameter != nil {
		aTag := &tags.Tag{}

		aTag.SQL = tags.NewViewSQL(c.View.Template.Source, "")
		aTag.View = &tags.View{Name: c.View.Name}
		if c.View != nil {
			if c.View.Batch != nil {
				aTag.View.Batch = c.View.Batch.Size
			}
			if c.View.Partitioned != nil {
				aTag.View.PartitionedConcurrency = c.View.Partitioned.Concurrency
				aTag.View.PartitionerType = c.View.Partitioned.DataType
			}
		}
		if tmpl := c.View.Template; tmpl != nil && tmpl.Summary != nil {
			aTag.SummarySQL = tags.ViewSQLSummary(tags.NewViewSQL(tmpl.Summary.Source, ""))
		}
		viewParameter.Tag = string(aTag.UpdateTag(reflect.StructTag(viewParameter.Tag)))
	}

	output, _ := c.Output.Type.Parameters.ReflectType("", registry.Lookup, state.WithRelation(), state.WithSQL(), state.WithVelty(false))
	var importModules = map[string]string{}
	statePkg := c.Output.Type.Package
	setter.SetStringIfEmpty(&statePkg, c.Input.Type.Package)
	setter.SetStringIfEmpty(&statePkg, "state")
	inPackageComponentTypes := indexComponentPackageTypes(c, statePkg)
	packagedTypes := c.buildDependencyTypes(inPackageComponentTypes, importModules)

	componentName := state.SanitizeTypeName(c.View.Name)
	embedURI := text.CaseFormatUpperCamel.Format(componentName, text.CaseFormatLowerUnderscore)
	var options = []xreflect.Option{
		xreflect.WithPackage(statePkg),
		xreflect.WithTypes(xreflect.NewType(componentName+"Output", xreflect.WithReflectType(output))),
		xreflect.WithPackageTypes(packagedTypes...),
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

	replacer := data.NewMap()
	replacer.Put("WithConnector", fmt.Sprintf(`,view.WithConnectorRef("%s")`, c.View.Connector.Name))
	replacer.Put("Name", componentName)
	replacer.Put("URI", c.URI)
	replacer.Put("Method", c.Method)
	replacer.Put("PackageName", c.Output.Type.Package)
	withNamedResource := ""
	if len(namedResources) > 0 {
		for i, elem := range namedResources {
			name := strings.TrimSuffix(path.Base(elem), path.Ext(elem))
			namedResources[i] = strconv.Quote(name)
		}
		withNamedResource = fmt.Sprintf("\n\t\trepository.WithNamedResources(%v),", strings.Join(namedResources, ","))
	}
	replacer.Put("WithNamedResource", withNamedResource)
	snippetBefore := replacer.ExpandAsText(registerInit)
	if withEmbed {
		defineComponentFunc := replacer.ExpandAsText(contractInit)
		if !withDefineComponent {
			defineComponentFunc = ""
		}
		snippetBefore += c.embedTemplate(embedURI, componentName)
		options = append(options,
			xreflect.WithImports(append(c.generatorImports(c.Contract.ModulePath, withDefineComponent), "github.com/viant/datly/view")),
			xreflect.WithSnippetAfter(defineComponentFunc))
	} else {
		replacer.Put("WithConnector", "")
	}
	if snippetBefore != "" {
		options = append(options, xreflect.WithSnippetBefore(snippetBefore))
	}

	inputType := input.Type()
	if inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}

	if inputType.Name() != "" && len(c.Input.Type.Parameters) > 0 { //to allow input dql changes, rebuild input from parameters
		if rawType, _ := c.Input.Type.Parameters.ReflectType(c.Input.Type.Package, registry.Lookup, state.WithSetMarker(), state.WithTypeName(inputType.Name())); rawType != nil {
			inputType = rawType
		}
	}

	if inputType.Name() != "" { //rewrite as inline sturct

		inputType = types.InlineStruct(input.Type(), func(field *reflect.StructField) {
			if markerTag := field.Tag.Get(structology.SetMarkerTag); markerTag != "" {
				field.Type = types.InlineStruct(field.Type, nil)
			}
			if aTag, _ := tags.ParseStateTags(field.Tag, nil); aTag != nil && aTag.Parameter != nil {
				if aTag.Parameter.Kind == string(state.KindComponent) {
					return
				}
			}
			if sType := types.EnsureStruct(field.Type); sType != nil && sType.Name() != "" && sType.PkgPath() == input.Type().PkgPath() {
				field.Type = types.InlineStruct(field.Type, func(sField *reflect.StructField) {
					if innerField := types.EnsureStruct(sField.Type); innerField != nil && innerField.Name() != "" && innerField.PkgPath() == sType.PkgPath() {
						sField.Type = types.InlineStruct(sField.Type, nil)
					}
				})
			}
		})
	}

	inputState := xreflect.GenerateStruct(componentName+"Input", inputType, options...)
	builder.WriteString(inputState)
	result := builder.String()
	result = c.View.Resource().ReverseSubstitutes(result)
	return result
}

func (c *Component) buildDependencyTypes(inPackageComponentTypes map[string]bool, importModules map[string]string) []*xreflect.Type {
	var result []*xreflect.Type
	uniquePackageTypes := map[string]*xreflect.Type{}
	packageModule := map[string]string{}
	for _, def := range c.View.TypeDefinitions() {
		if inPackageComponentTypes[def.Name] {
			continue
		}
		if def.Package != "" && c.ModulePath != "" && strings.Contains(def.DataType, " ") { //complex type
			importModules[def.Package] = c.ModulePath
		}
		aType := xreflect.NewType(def.Name, xreflect.WithModulePath(def.ModulePath), xreflect.WithPackage(def.Package), xreflect.WithTypeDefinition(def.DataType))
		prev, found := uniquePackageTypes[aType.TypeName()]
		if !found {
			uniquePackageTypes[aType.TypeName()] = aType
			result = append(result, aType)
		}
		if _, found := packageModule[aType.Package]; !found {
			packageModule[aType.Package] = aType.ModulePath
		}
		setter.SetStringIfEmpty(&aType.ModulePath, packageModule[aType.Package])
		if prev != nil {
			setter.SetStringIfEmpty(&prev.ModulePath, aType.ModulePath)
		}
	}
	return result
}

func (c *Component) embedTemplate(embedURI string, componentName string) string {
	return fmt.Sprintf(`
//go:embed %v/*.sql
var %vFS embed.FS

`, embedURI, componentName)
}

func (c *Component) generatorImports(modulePath string, component bool) []string {

	checksumModule := "github.com/viant/xdatly/types/custom/checksum"
	index := strings.LastIndex(modulePath, "/pkg/")
	if index != -1 {
		checksumModule = modulePath[:index] + "/pkg/checksum"
	}
	checksumParent, _ := path.Split(checksumModule)
	if !strings.HasSuffix(checksumParent, "dependency") {
		checksumModule = path.Join(checksumParent, "dependency", "checksum")
	}

	ret := []string{"embed",

		"reflect",
		"github.com/viant/xdatly/types/core",
		checksumModule,
	}

	if component {
		ret = append(ret,
			"fmt",
			"context",
			"github.com/viant/datly/repository",
			"github.com/viant/datly/repository/contract",
			"github.com/viant/datly")
	}
	return ret
}

func (c *Component) adjustStructField(embedURI string, embeds map[string]string, generateContract bool) func(aField *reflect.StructField, tag *string, typeName *string, doc *string) {
	return func(aField *reflect.StructField, tag, typeName, doc *string) {
		fieldTag := *tag
		if !generateContract {
			fieldTag, _ = xreflect.RemoveTag(fieldTag, "on")
		} else if !strings.Contains(fieldTag, "parameter:") {
			if strings.Contains(fieldTag, "sqlx:") {
				fieldTag, _ = xreflect.RemoveTag(fieldTag, "velty")
			}
		}
		fieldTag, value := xreflect.RemoveTag(fieldTag, "sql")
		if value != "" && generateContract {
			name := *typeName
			setter.SetStringIfEmpty(&name, aField.Name)
			key := text.CaseFormatUpperCamel.Format(name, text.CaseFormatLowerUnderscore)
			key = strings.ReplaceAll(key, ".", "")
			key += ".sql"
			value = c.View.Resource().ReverseSubstitutes(value)
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
