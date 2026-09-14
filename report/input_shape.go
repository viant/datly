package report

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func (c *inputCompiler) filterType() (reflect.Type, error) {
	fields := make([]xshape.RuntimeField, 0, len(c.metadata.filters))
	for _, item := range c.metadata.filters {
		typeOf := item.contract.SourceType()
		if typeOf == nil {
			typeOf = item.contract.DestinationType()
		}
		if typeOf == nil {
			return nil, fmt.Errorf("report filter %s has no source type", item.name)
		}
		fields = append(fields, xshape.RuntimeField{
			Name: item.fieldName, Type: presenceType(typeOf), Tag: fieldTag(lowerCamel(item.name), item.description),
		})
	}
	return (xshape.Runtime{}).Struct(fields)
}

func sectionType(fields []field) (reflect.Type, error) {
	result := make([]xshape.RuntimeField, 0, len(fields))
	for _, item := range fields {
		result = append(result, xshape.RuntimeField{
			Name: item.fieldName, TypeExpr: "bool", Tag: fieldTag(lowerCamel(item.publicName), item.description),
		})
	}
	return (xshape.Runtime{}).Struct(result)
}

func inputField(name string, typeOf reflect.Type, description string) xshape.RuntimeField {
	return xshape.RuntimeField{Name: name, Type: typeOf, Tag: fieldTag(lowerCamel(name), description)}
}

func bodyParam(name string, inputType reflect.Type) *spec.Parameter {
	field, _ := xshape.Linked(inputType).StructField(name)
	return &spec.Parameter{Name: name, Source: spec.BindSource{Kind: "body", Name: jsonName(field)}}
}

func jsonName(field reflect.StructField) string {
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "" || name == "-" {
		return lowerCamel(field.Name)
	}
	return name
}

func fieldTag(jsonName, description string) reflect.StructTag {
	tag := `json:"` + jsonName + `,omitempty"`
	if description = strings.TrimSpace(description); description != "" {
		tag += " desc:" + strconv.Quote(description)
	}
	return reflect.StructTag(tag)
}

func presenceType(typeOf reflect.Type) reflect.Type {
	if typeOf.Kind() == reflect.Pointer {
		return typeOf
	}
	return (xshape.Runtime{}).Pointer(typeOf)
}

func filterFieldCompatible(fieldType, sourceType reflect.Type) bool {
	return fieldType == sourceType || fieldType.Kind() == reflect.Pointer && fieldType.Elem() == sourceType
}

func exactFieldIndex(typeOf reflect.Type, name string, expected reflect.Type) ([]int, error) {
	field, err := xshape.Linked(typeOf).StructField(name)
	if err != nil || field.Type != expected {
		return nil, fmt.Errorf("report input %s requires %s field %s", typeOf, expected, name)
	}
	return append([]int(nil), field.Index...), nil
}

func normalizeStructType(typeOf reflect.Type) reflect.Type {
	typeOf = (xshape.Runtime{}).Indirect(typeOf)
	if typeOf == nil || typeOf.Kind() != reflect.Struct {
		return nil
	}
	return typeOf
}

func dereference(typeOf reflect.Type) reflect.Type {
	return (xshape.Runtime{}).Indirect(typeOf)
}

func joinIndex(left, right []int) []int {
	return append(append([]int(nil), left...), right...)
}

func lowerCamel(value string) string {
	if value == "" {
		return ""
	}
	return strings.ToLower(value[:1]) + value[1:]
}

func (c *inputCompiler) resolutionContext() *typecatalog.ResolutionContext {
	result := &typecatalog.ResolutionContext{}
	if c != nil && c.component != nil {
		result.PackagePath = c.component.Key.Scope
		if c.component.TypeContext != nil {
			result.DefaultPackage = c.component.TypeContext.DefaultPackage
			for _, item := range c.component.TypeContext.Imports {
				result.Imports = append(result.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
			}
		}
	}
	return typecatalog.NormalizeContext(result)
}
