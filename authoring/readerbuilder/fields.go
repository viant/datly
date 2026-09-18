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
	if !strings.EqualFold(name, parameter.Name) {
		return "", fmt.Errorf("field rename requires a dedicated reference-aware operation")
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

func optionPatches(occurrence dql.DeclarationOccurrence, mutation *Field) []sourcePatch {
	var patches []sourcePatch
	requiredSeen := false
	selectorSeen := false
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
