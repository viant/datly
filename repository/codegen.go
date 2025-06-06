package repository

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
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
			if c.View.PublishParent {
				aTag.View.PublishParent = c.View.PublishParent
			}
			if c.View.Partitioned != nil {
				aTag.View.PartitionedConcurrency = c.View.Partitioned.Concurrency
				aTag.View.PartitionerType = c.View.Partitioned.DataType
			}
			if c.View.RelationalConcurrency != nil {
				aTag.View.RelationalConcurrency = c.View.RelationalConcurrency.Number
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
			xreflect.WithImports(c.generatorImports(c.Contract.ModulePath, withDefineComponent)),
			xreflect.WithSnippetAfter(defineComponentFunc))
	} else {
		replacer.Put("WithConnector", "")
	}
	if snippetBefore != "" {
		options = append(options, xreflect.WithSnippetBefore(snippetBefore))
	}

	inputType := input.Type()
	if inputType != nil && inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}
	inputTypeName := c.Input.Type.Schema.Name
	if len(c.Input.Type.Parameters) > 0 { //to al
		if inputType != nil {
			for _, parameter := range c.Input.Type.Parameters {
				if aField, ok := inputType.FieldByName(parameter.Name); ok {
					parameter.Schema.SetType(aField.Type)
				}
			}
		}
		// low input dql changes, rebuild input from parameters
		if rawType, _ := c.Input.Type.Parameters.ReflectType(c.Input.Type.Package, registry.Lookup, state.WithSetMarker(), state.WithTypeName(inputType.Name())); rawType != nil {
			inputType = rawType
		}
	}

	inputType = types.InlineStruct(inputType, func(field *reflect.StructField) {
		if markerTag := field.Tag.Get(structology.SetMarkerTag); markerTag != "" {
			marketTypeName := inputTypeName + "Has"
			prevTag, _ := xreflect.RemoveTag(string(field.Tag), "typeName")
			field.Tag = reflect.StructTag(prevTag + " typeName:\"" + marketTypeName + "\"")
			field.Type = types.InlineStruct(field.Type, nil)
			return
		}

		aTag, _ := tags.ParseStateTags(field.Tag, nil)
		if aTag != nil && aTag.Parameter != nil {
			if aTag.Parameter.Kind == string(state.KindComponent) {
				return
			}
			if aTag.Parameter.Kind == string(state.KindObject) {
				typeName := aTag.TypeName
				fType := types.EnsureStruct(field.Type)
				if fType != nil {
					typeName = fType.String()
				}
				fieldPkg := ""
				if idx := strings.LastIndex(typeName, "."); idx != -1 {
					fieldPkg = typeName[:idx]
					typeName = fType.Name()
				}
				inTheSamePackage := fieldPkg == statePkg || fieldPkg == ""
				if aTag.Parameter.Kind == string(state.KindObject) && typeName != "" && inTheSamePackage {
					field.Type = types.InlineStruct(field.Type, func(innerField *reflect.StructField) {
						if markerTag := innerField.Tag.Get(structology.SetMarkerTag); markerTag != "" {
							innerType := types.EnsureStruct(innerField.Type)
							if innerType == nil || innerType.Name() == "" {
								return
							}
							innerField.Type = types.InlineStruct(innerField.Type, nil)
							marketTypeName := typeName + "Has"
							innerField.Tag = reflect.StructTag(string(innerField.Tag) + " typeName:\"" + marketTypeName + "\"")
						}
					})
					return
				}

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

	inputState := xreflect.GenerateStruct(componentName+"Input", inputType, options...)
	builder.WriteString(inputState)

	if withEmbed {
		embedderCode := fmt.Sprintf(`
	func (i *%vInput) EmbedFS() *embed.FS {
		return &%vFS
	}`, componentName, componentName)
		builder.WriteString(embedderCode)
	}

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
			c.updatePackageModule(def, importModules)
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

func (c *Component) updatePackageModule(def *view.TypeDefinition, importModules map[string]string) {
	if index := strings.LastIndex(def.Package, "/"); index != -1 {
		pkgAlias := def.Package[index+1:]
		pkgName := def.Package[:index]
		if _, ok := importModules[pkgAlias]; !ok {
			importModules[pkgAlias] = c.ModulePath + "/" + pkgName
		}
		def.Package = pkgAlias
	} else {
		if _, ok := importModules[def.Package]; !ok {
			importModules[def.Package] = c.ModulePath
		}
	}
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
			"github.com/viant/datly/view",
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
			if viewName := extractViewName(aField); viewName != "" {
				key = viewName
			}
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
		//if value := reflect.StructTag(fieldTag).Get("doc"); value != "" {
		//	*doc = aField.Name + " " + value
		//}
		*tag = fieldTag
	}
}

func extractViewName(aField *reflect.StructField) string {
	var viewName string
	if viewTag := aField.Tag.Get("view"); viewTag != "" {
		if viewName = strings.Split(viewTag, ",")[0]; viewName != "" {
		}
	}
	return viewName
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
