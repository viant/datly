package generate

import (
	"fmt"
	"github.com/viant/tagly/format/text"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func preferDefinedParams(params []*spec.Parameter) []*spec.Parameter {
	return spec.EffectiveParameters(params)
}

func resolveInputFields(component *spec.Component, declarations Declarations) ([]Field, error) {
	if component == nil {
		return nil, nil
	}
	var result []Field
	names := map[string]bool{}
	for _, param := range preferDefinedParams(component.Parameters) {
		if param == nil {
			continue
		}
		if param.EmitOutput {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") {
			continue
		}
		field, ok, err := resolveField(param, declarations)
		if err != nil {
			return nil, err
		}
		if ok {
			if component.Settings != nil && component.Settings.Mutation != "" && strings.EqualFold(strings.TrimSpace(param.Source.Kind), "body") {
				field.Tag, err = appendViewTags(field.Tag, component.RootView)
				if err != nil {
					return nil, err
				}
			}
			if names[field.Name] {
				return nil, fmt.Errorf("generated input field %q collides; use distinct selector source names", field.Name)
			}
			names[field.Name] = true
			result = append(result, field)
		}
	}
	return result, nil
}

func canonicalOutputViewType(param *spec.Parameter, fieldType, rootViewType string) string {
	if param == nil || strings.TrimSpace(rootViewType) == "" ||
		!strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") ||
		!strings.EqualFold(strings.TrimSpace(param.Source.Name), "view") {
		return fieldType
	}
	if strings.TrimSpace(param.TypeExpr) != "" || strings.TrimSpace(param.OutputTypeExpr) != "" ||
		defaultTagTypeName(param.Tag) != "" {
		return fieldType
	}
	switch strings.TrimSpace(fieldType) {
	case "View":
		return rootViewType
	case "*View":
		return "*" + rootViewType
	case "[]View":
		return "[]" + rootViewType
	case "[]*View":
		return "[]*" + rootViewType
	default:
		return fieldType
	}
}

func resolveOutputFields(component *spec.Component, declarations Declarations, rootViewType string) ([]Field, error) {
	if component == nil {
		return nil, nil
	}
	var result []Field
	hasViewOutput := false
	hasCustomOutput := false
	for _, param := range preferDefinedParams(component.Parameters) {
		if param == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") && !param.EmitOutput {
			continue
		}
		field, ok, err := resolveField(param, declarations)
		if err != nil {
			return nil, err
		}
		if ok {
			kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
			name := strings.ToLower(strings.TrimSpace(param.Source.Name))
			if param.EmitOutput || kind != "output" || name != "view" && name != "status" && !param.IsDerivedOutput() {
				hasCustomOutput = true
			}
			field.Type = canonicalOutputViewType(param, field.Type, rootViewType)
			if strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") &&
				strings.EqualFold(strings.TrimSpace(param.Source.Name), "view") {
				hasViewOutput = true
				var err error
				field.Tag, err = appendViewTags(field.Tag, component.RootView)
				if err != nil {
					return nil, err
				}
			}
			if param.IsDerivedOutput() {
				relation := outputRelation(component.RootView, param.Name)
				if relation == nil || relation.View == nil {
					return nil, fmt.Errorf("output relation %s has no canonical derived view", param.Name)
				}
				field.Tag, err = appendViewTags(field.Tag, relation.View)
				if err != nil {
					return nil, err
				}
			}
			result = append(result, field)
		}
	}
	if !hasViewOutput && !hasCustomOutput && component.RootView != nil {
		cardinality := component.RootView.Cardinality
		viewType := "[]*" + rootViewType
		if cardinality == spec.CardinalityOne {
			viewType = "*" + rootViewType
		}
		viewTag := `parameter:"view,kind=output,in=view"`
		if cardinality != "" {
			viewTag = `parameter:"view,kind=output,in=view,cardinality=` + canonicalCardinality(cardinality) + `"`
		}
		var err error
		viewTag, err = appendViewTags(viewTag, component.RootView)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 {
			result = append(result, Field{
				Name:   "Status",
				Type:   "string",
				Tag:    `parameter:"status,kind=output,in=status"`,
				Source: "output",
			})
		}
		result = append(result, Field{
			Name:   "Data",
			Type:   viewType,
			Tag:    viewTag,
			Source: "output",
		})
	}
	return result, nil
}

func outputRelation(root *spec.View, holder string) *spec.Relation {
	if root == nil {
		return nil
	}
	for _, relation := range root.Relations {
		if relation != nil && relation.Kind == spec.RelationKindDerived &&
			strings.EqualFold(strings.TrimSpace(relation.Holder), strings.TrimSpace(holder)) {
			return relation
		}
	}
	return nil
}

func canonicalCardinality(value spec.Cardinality) string {
	if value == spec.CardinalityOne {
		return "One"
	}
	return "Many"
}

func resolveField(param *spec.Parameter, declarations Declarations) (Field, bool, error) {
	name := generatedParameterName(param)
	if name == "" {
		return Field{}, false, nil
	}
	typ := strings.TrimSpace(param.OutputTypeExpr)
	if typ == "" {
		typ = strings.TrimSpace(param.TypeExpr)
	}
	if typ == "" {
		typ = defaultTagPlaceholderTypeName(param.Tag)
	}
	if typ == "" {
		if helperType, ok := inferHelperType(param, declarations); ok {
			typ = helperType
		}
	}
	if typ == "" {
		typ = inferFieldType(param, declarations)
	}
	tagName := strings.TrimSpace(param.Name)
	if tagName == "" {
		tagName = strings.TrimSpace(param.Source.Name)
	}
	metadata, err := canonicalFieldMetadata(param)
	if err != nil {
		return Field{}, false, fmt.Errorf("parameter %s metadata: %w", param.Name, err)
	}
	result := Field{
		Name:      name,
		Type:      typ,
		Tag:       fieldTag(param, tagName, metadata),
		Source:    strings.TrimSpace(param.Source.Kind),
		Anonymous: false,
	}
	if statusOutput(param) && anonymousTagEnabled(param.Tag) {
		markAnonymousOutputField(&result)
	}
	return result, true, nil
}

func statusOutput(param *spec.Parameter) bool {
	if param == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output") && strings.EqualFold(strings.TrimSpace(param.Source.Name), "status")
}

func markAnonymousOutputField(field *Field) {
	if field == nil || !embeddableFieldType(field.Type) || !anonymousTagEnabled(field.Tag) {
		return
	}
	field.Anonymous = true
	clearOutputParameterTagName(field, "dataType")
	field.Tag = withoutStructTags(field.Tag, "anonymous")
}

func clearOutputParameterTagName(field *Field, removeParts ...string) {
	if field == nil {
		return
	}
	tag := reflect.StructTag(field.Tag).Get("parameter")
	if tag == "" {
		return
	}
	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return
	}
	parts[0] = ""
	parts = withoutParameterTagParts(parts, removeParts...)
	field.Tag = replaceStructTag(field.Tag, "parameter", strings.Join(parts, ","))
}

func withoutParameterTagParts(parts []string, names ...string) []string {
	remove := make(map[string]bool, len(names))
	for _, name := range names {
		remove[name] = true
	}
	result := parts[:0]
	for index, part := range parts {
		if index == 0 {
			result = append(result, part)
			continue
		}
		name, _, _ := strings.Cut(part, "=")
		if remove[strings.TrimSpace(name)] {
			continue
		}
		result = append(result, part)
	}
	return result
}

type structTagValue struct {
	name  string
	value string
}

func canonicalFieldMetadata(param *spec.Parameter) ([]structTagValue, error) {
	if param == nil {
		return nil, nil
	}
	var result []structTagValue
	if param.MCP != nil {
		result = append(result, structTagValue{name: "mcpEnabled", value: fmt.Sprint(*param.MCP)})
	}
	if param.PathMCP != nil {
		result = append(result, structTagValue{name: "pathMcpEnabled", value: fmt.Sprint(*param.PathMCP)})
	}
	if param.Codec != nil {
		value, err := (dtag.Codec{
			Body: param.Codec.Body, Arguments: append([]string(nil), param.Codec.Args...), OutputType: param.Codec.OutputType,
		}).Value()
		if err != nil {
			return nil, err
		}
		result = append(result, structTagValue{name: dtag.CodecName, value: value})
	}
	for _, predicate := range param.Predicates {
		value, err := dtag.PredicateValue(predicate)
		if err != nil {
			return nil, err
		}
		if value != "" {
			result = append(result, structTagValue{name: dtag.PredicateName, value: value})
		}
	}
	if param.QuerySelector != nil {
		value, err := (dtag.QuerySelector{View: param.QuerySelector.View}).Value()
		if err != nil {
			return nil, err
		}
		result = append(result, structTagValue{name: dtag.QuerySelectorName, value: value})
	}
	for _, item := range []structTagValue{
		{name: dtag.DescriptionName, value: param.Description},
		{name: dtag.ExampleName, value: param.Example},
	} {
		if strings.TrimSpace(item.value) != "" {
			result = append(result, item)
		}
	}
	return result, nil
}

func defaultTagPlaceholderTypeName(tag string) string {
	typeName := defaultTagTypeName(tag)
	if typeName == "" {
		return ""
	}
	base := unwrapQualifiedTypeName(typeName)
	if base == "" {
		return ""
	}
	if index := strings.LastIndex(base, "."); index != -1 {
		base = base[index+1:]
	}
	return strings.TrimSpace(base)
}

// generatedParameterName keeps explicit source identity for selectors whose
// logical properties repeat across views. Binding consumes the source tag.
func generatedParameterName(param *spec.Parameter) string {
	if param.QuerySelector != nil && strings.TrimSpace(param.Source.Name) != "" {
		source := strings.TrimLeft(strings.TrimSpace(param.Source.Name), "_")
		return text.DetectCaseFormat(source).Format(source, text.CaseFormatUpperCamel)
	}
	return exportedName(param.Name)
}
