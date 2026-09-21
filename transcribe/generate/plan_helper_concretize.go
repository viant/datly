package generate

import (
	"fmt"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

func (r *planResolver) concretizeGeneratedHelperFields() error {
	if r.plan == nil || r.input.Component == nil {
		return nil
	}
	for _, param := range preferDefinedParams(r.input.Component.Parameters) {
		declaration := r.input.Declarations.declaration(param)
		if param == nil || len(declaration.Projection) == 0 {
			continue
		}
		var sourceType string
		for _, field := range r.plan.Input.Fields {
			if field.Name == exportedName(param.Source.Name) {
				sourceType = unwrapQualifiedTypeName(field.Type)
				break
			}
		}
		if sourceType == "" {
			continue
		}
		sourceView := r.plan.ViewByType(sourceType)
		if sourceView != nil && sourceView.Ownership == ViewLinked {
			continue
		}
		for _, child := range strings.FieldsFunc(declaration.NestedChildField, func(char rune) bool { return char == '.' || char == '/' }) {
			if sourceView == nil {
				break
			}
			field, found := sourceView.Field(exportedName(child))
			if !found {
				if r.requireConcreteHelpers {
					return fmt.Errorf("generated helper source path %s has no current field on %s", declaration.NestedChildField, sourceView.Name)
				}
				sourceView = nil
				break
			}
			sourceView = r.plan.ViewByType(unwrapQualifiedTypeName(field.Type))
		}
		if sourceView == nil {
			continue
		}
		helperType, ok := inferHelperType(param, r.input.Declarations)
		if !ok {
			continue
		}
		helper := r.plan.helperType(helperTypeName(helperType))
		if helper == nil {
			continue
		}
		aliasColumns := compositeAliasColumns(param)
		for _, projected := range declaration.Projection {
			valueField := exportedName(projected.Source)
			valueType := ""
			valueExplicit := false
			valueSQLX := ""
			for _, field := range sourceView.Fields {
				if strings.EqualFold(field.Name, valueField) {
					valueType = strings.TrimSpace(field.Type)
					valueExplicit = field.ExplicitType
					if _, ok := reflect.StructTag(field.Tag).Lookup(sqlio.TagSqlx); ok {
						// Helpers need only the physical column mapping. Mutation and
						// validation options belong to the entity field, not to the
						// projected lookup key. The declaration's destination alias is
						// authoritative because CompositeIn targets the derived query,
						// not the underlying physical table.
						valueSQLX = strings.TrimSpace(projected.Name)
					}
					break
				}
			}
			if valueType == "" || valueType == "any" {
				if r.requireConcreteHelpers {
					return fmt.Errorf("generated helper %s projection %s has no current concrete field on %s", helper.Name, projected.Source, sourceView.Name)
				}
				continue
			}
			for index := range helper.Fields {
				if helper.Fields[index].Name != projected.Name {
					continue
				}
				helper.Fields[index].ExplicitType = valueExplicit
				// A generated current-key helper normally retains the entity's
				// physical SQLX column (TENANT_ID). Only keys explicitly renamed by
				// the current view's outer projection address the derived table by
				// their logical output alias (RootKey).
				if aliasColumns[helper.Fields[index].Name] {
					valueSQLX = projected.Name
				}
				if valueSQLX != "" {
					helper.Fields[index].Tag = appendStructTag(helper.Fields[index].Tag, sqlio.TagSqlx, valueSQLX)
				}
				if declaration.isAggregate() {
					helper.Fields[index].Type = "[]" + valueType
				} else {
					helper.Fields[index].Type = valueType
				}
			}
		}
	}
	return nil
}

func compositeAliasColumns(param *spec.Parameter) map[string]bool {
	result := map[string]bool{}
	if param == nil {
		return result
	}
	value := reflect.StructTag(param.Tag).Get("compositeAlias")
	for _, item := range strings.Split(value, ",") {
		if name := strings.TrimSpace(item); name != "" {
			result[name] = true
		}
	}
	return result
}

func (r *planResolver) concretizeHelperFields() error {
	lookup := r.lookup()
	if r.plan == nil || r.input.Component == nil || lookup == nil || len(r.plan.HelperTypes) == 0 {
		return nil
	}
	params := map[string]*spec.Parameter{}
	for _, param := range preferDefinedParams(r.input.Component.Parameters) {
		if param != nil {
			params[generatedParameterName(param)] = param
		}
	}
	for _, param := range preferDefinedParams(r.input.Component.Parameters) {
		if param == nil {
			continue
		}
		helperType, ok := inferHelperType(param, r.input.Declarations)
		if !ok {
			continue
		}
		helper := r.plan.helperType(helperTypeName(helperType))
		if helper == nil {
			continue
		}
		concrete := true
		for _, field := range helper.Fields {
			if field.Type == "" || field.Type == "any" {
				concrete = false
				break
			}
		}
		if concrete {
			continue
		}
		declaration := r.input.Declarations.declaration(param)
		switch {
		case declaration.isAggregate():
			projected := declaration.Projection[0]
			valueType, err := projectedArrayAggValueType(params, param, projected.Source, lookup)
			if err != nil {
				return fmt.Errorf("resolve helper type %q: %w", helper.Name, err)
			}
			if valueType != "" && len(helper.Fields) == 1 && helper.Fields[0].Name == projected.Name {
				helper.Fields[0].Type = "[]" + valueType
			}
		case len(declaration.Projection) > 0:
			projection, err := projectedRowFields(params, param, r.input.Declarations, lookup)
			if err != nil {
				return fmt.Errorf("resolve helper type %q: %w", helper.Name, err)
			}
			for i, projected := range declaration.Projection {
				field := &helper.Fields[i]
				if field.Type != "" && field.Type != "any" {
					continue
				}
				if goType, ok := projection[exportedName(projected.Source)]; ok {
					field.Type = goType
				}
			}
		}
	}
	return nil
}

func (r *planResolver) validateConcreteHelperFields() error {
	return r.plan.validateConcreteHelperFields()
}

func (p *Plan) validateConcreteHelperFields() error {
	if p == nil {
		return nil
	}
	for _, helper := range p.HelperTypes {
		for _, field := range helper.Fields {
			typeName := strings.TrimSpace(field.Type)
			if typeName == "" || typeName == "any" || typeName == "interface{}" || strings.Contains(typeName, "[]any") {
				return fmt.Errorf("generated helper %s field %s has no concrete Go type", helper.Name, field.Name)
			}
		}
	}
	return nil
}

func (r *planResolver) validateHelperFieldNames() error {
	for _, helper := range r.plan.HelperTypes {
		seen := map[string]bool{}
		for _, field := range helper.Fields {
			name := strings.TrimSpace(field.Name)
			if !token.IsIdentifier(name) || !token.IsExported(name) {
				return fmt.Errorf("generated helper %s projection alias %q must be an exported Go identifier", helper.Name, name)
			}
			if seen[name] {
				return fmt.Errorf("generated helper %s projection alias %q is duplicated", helper.Name, name)
			}
			seen[name] = true
		}
	}
	return nil
}

func projectedArrayAggValueType(params map[string]*spec.Parameter, helperParam *spec.Parameter, fieldName string, lookup func(string) (*x.Type, error)) (string, error) {
	sourceParam := params[exportedName(helperParam.Source.Name)]
	if sourceParam == nil {
		return "", nil
	}
	descriptor, err := resolveParamDescriptor(sourceParam, lookup)
	if err != nil || descriptor == nil {
		return "", err
	}
	projection, err := builtinFieldsFromDescriptor(descriptor, "", lookup)
	if err != nil {
		return "", err
	}
	for name, fieldType := range projection {
		if strings.EqualFold(name, exportedName(fieldName)) {
			return fieldType, nil
		}
	}
	return "", nil
}

func (p *Plan) helperType(name string) *HelperType {
	for i := range p.HelperTypes {
		if p.HelperTypes[i].Name == name {
			return &p.HelperTypes[i]
		}
	}
	return nil
}

func projectedRowFields(params map[string]*spec.Parameter, helperParam *spec.Parameter, declarations Declarations, lookup func(string) (*x.Type, error)) (map[string]string, error) {
	sourceParam := params[exportedName(helperParam.Source.Name)]
	if sourceParam == nil {
		return nil, nil
	}
	descriptor, err := resolveParamDescriptor(sourceParam, lookup)
	if err != nil {
		return nil, err
	}
	if descriptor == nil {
		return nil, nil
	}
	projection, err := builtinFieldsFromDescriptor(descriptor, declarations.declaration(helperParam).NestedChildField, lookup)
	if err != nil {
		return nil, err
	}
	return projection, nil
}

func builtinFieldsFromDescriptor(descriptor *x.Type, child string, lookup func(string) (*x.Type, error)) (map[string]string, error) {
	result := map[string]string{}
	if descriptor == nil {
		return result, nil
	}
	path := ""
	if child = strings.TrimSpace(child); child != "" {
		path = exportedName(child)
	}
	fields, err := xshape.New(descriptor, lookup).FieldsAt(path)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		if fieldType, ok := field.BuiltinType(); ok {
			result[field.Name] = fieldType
		}
	}
	return result, nil
}
