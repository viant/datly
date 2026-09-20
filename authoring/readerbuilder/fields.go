package readerbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/transcribe/dql"
)

func editField(source string, operation OperationType, mutation *Field) (string, error) {
	if mutation == nil {
		return "", fmt.Errorf("field is required")
	}
	target := strings.TrimSpace(mutation.ExistingName)
	if target == "" {
		target = strings.TrimSpace(mutation.Name)
	}
	if target == "" {
		return "", fmt.Errorf("existing field name is required")
	}
	declarations, err := dql.DeclarationOccurrences(source)
	if err != nil {
		return "", err
	}
	var matches []dql.DeclarationOccurrence
	for _, occurrence := range declarations {
		if occurrence.Parameter != nil && strings.EqualFold(occurrence.Parameter.Name, target) {
			matches = append(matches, occurrence)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("field %q resolves to %d declarations; exactly one is required", target, len(matches))
	}
	occurrence := matches[0]
	parameter := occurrence.Parameter
	if parameter == nil || strings.EqualFold(parameter.Source.Kind, "output") || parameter.IsDerivedOutput() {
		return "", fmt.Errorf("field %q is not an editable input declaration", target)
	}
	switch operation {
	case OperationRemoveField:
		span := occurrence.Span
		for span.End < len(source) && (source[span.End] == '\r' || source[span.End] == '\n') {
			span.End++
		}
		return dql.ApplyPatch(source, span, "")
	case OperationUpdateField:
	default:
		return "", fmt.Errorf("unsupported field operation %q", operation)
	}
	name := strings.TrimSpace(mutation.Name)
	if name == "" {
		name = parameter.Name
	}
	if name != parameter.Name {
		renamed, renameErr := renameFieldReferences(source, parameter.Name, name, declarations)
		if renameErr != nil {
			return "", renameErr
		}
		copy := *mutation
		copy.ExistingName = name
		copy.Name = name
		return editField(renamed, operation, &copy)
	}
	typeExpr := firstNonBlank(mutation.Type, parameter.TypeExpr)
	sourceKind := firstNonBlank(mutation.SourceKind, parameter.Source.Kind)
	sourceName := firstNonBlank(mutation.SourceName, parameter.Source.Name)
	if !validIdentifier(name) || typeExpr == "" || !validIdentifier(sourceKind) || sourceName == "" || strings.ContainsAny(typeExpr+sourceName, "\r\n)") {
		return "", fmt.Errorf("field name, type, sourceKind, and sourceName are invalid")
	}
	if occurrence.HeadSpan.End <= occurrence.HeadSpan.Start {
		return "", fmt.Errorf("field %q does not expose an editable declaration head", target)
	}
	patches := []sourcePatch{{span: occurrence.HeadSpan, text: fmt.Sprintf("$%s<%s>(%s/%s)", name, typeExpr, sourceKind, sourceName)}}
	patches = append(patches, optionPatches(occurrence, mutation)...)
	return applySourcePatches(source, patches)
}

func renameFieldReferences(source, from, to string, declarations []dql.DeclarationOccurrence) (string, error) {
	if !validIdentifier(to) {
		return "", fmt.Errorf("new field name %q is not an identifier", to)
	}
	for _, occurrence := range declarations {
		if occurrence.Parameter != nil && !strings.EqualFold(occurrence.Parameter.Name, from) && strings.EqualFold(occurrence.Parameter.Name, to) {
			return "", fmt.Errorf("field %q already exists", to)
		}
	}
	needle := "$" + from
	var result strings.Builder
	result.Grow(len(source) + 16)
	quote := byte(0)
	for index := 0; index < len(source); {
		ch := source[index]
		if quote != 0 {
			result.WriteByte(ch)
			index++
			if ch == '\\' && index < len(source) {
				result.WriteByte(source[index])
				index++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			result.WriteByte(ch)
			index++
			continue
		}
		if strings.HasPrefix(source[index:], needle) && identifierBoundary(source, index+len(needle)) {
			result.WriteByte('$')
			result.WriteString(to)
			index += len(needle)
			continue
		}
		result.WriteByte(ch)
		index++
	}
	if quote != 0 {
		return "", fmt.Errorf("source contains an unterminated quoted value")
	}
	return result.String(), nil
}

func identifierBoundary(source string, index int) bool {
	if index >= len(source) {
		return true
	}
	ch := source[index]
	return ch != '_' && !(ch >= 'a' && ch <= 'z') && !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9')
}

func optionPatches(occurrence dql.DeclarationOccurrence, mutation *Field) []sourcePatch {
	var patches []sourcePatch
	requiredSeen := false
	selectorSeen := false
	valueSeen := false
	selector := ""
	if mutation.UpdateQuerySelector != nil {
		selector = strings.TrimSpace(*mutation.UpdateQuerySelector)
	}
	for _, option := range occurrence.Options {
		switch strings.ToLower(strings.TrimSpace(option.Name)) {
		case "required", "optional":
			if mutation.Required == nil {
				continue
			}
			text := ""
			if !requiredSeen {
				if *mutation.Required {
					text = ".Required()"
				} else {
					text = ".Optional()"
				}
				requiredSeen = true
			}
			patches = append(patches, sourcePatch{span: option.Span, text: text})
		case "queryselector":
			if mutation.UpdateQuerySelector == nil {
				continue
			}
			text := ""
			if !selectorSeen && selector != "" {
				text = ".QuerySelector(" + strconv.Quote(selector) + ")"
			}
			selectorSeen = true
			patches = append(patches, sourcePatch{span: option.Span, text: text})
		case "value":
			if !mutation.UpdateValue {
				continue
			}
			text := ""
			if !valueSeen && mutation.Value != nil {
				text = ".Value(" + strconv.Quote(*mutation.Value) + ")"
			}
			valueSeen = true
			patches = append(patches, sourcePatch{span: option.Span, text: text})
		}
	}
	insert := ""
	if mutation.Required != nil && !requiredSeen {
		if *mutation.Required {
			insert += ".Required()"
		} else {
			insert += ".Optional()"
		}
	}
	if mutation.UpdateQuerySelector != nil && selector != "" && !selectorSeen {
		insert += ".QuerySelector(" + strconv.Quote(selector) + ")"
	}
	if mutation.UpdateValue && mutation.Value != nil && !valueSeen {
		insert += ".Value(" + strconv.Quote(*mutation.Value) + ")"
	}
	if insert != "" {
		patches = append(patches, sourcePatch{span: dql.SourceSpan{Start: occurrence.OptionInsert, End: occurrence.OptionInsert}, text: insert})
	}
	return patches
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
