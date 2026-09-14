package generate

import (
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/tagly/tags"
)

func tagTypeName(tag string) string {
	parsed := tags.NewTags(strings.TrimSpace(tag)).Lookup("typeName")
	if parsed == nil {
		return ""
	}
	return strings.TrimSpace(string(parsed.Values))
}

func fieldTag(param *spec.Parameter, tagName string, metadata []structTagValue) string {
	kind := strings.TrimSpace(param.Source.Kind)
	inName := strings.TrimSpace(param.Source.Name)
	if param.IsDerivedOutput() {
		inName = "derived"
	}
	encode := func(value string) string {
		value = strings.TrimSpace(value)
		if !strings.ContainsAny(value, ",='\\") {
			return value
		}
		value = strings.ReplaceAll(value, `\`, `\\`)
		value = strings.ReplaceAll(value, `'`, `\'`)
		return "'" + value + "'"
	}
	if param.EmitOutput {
		kind = "output"
		if inName == "" {
			inName = "body"
		}
	}
	parts := []string{tagName, "kind=" + encode(kind), "in=" + encode(inName)}
	appendValue := func(key, value string) {
		if value != "" {
			parts = append(parts, key+"="+encode(value))
		}
	}
	appendValue("when", param.When)
	appendValue("scope", param.Scope)
	appendValue("with", param.With)
	appendValue("dataType", param.TypeExpr)
	appendValue("cardinality", param.Cardinality)
	if param.Activation != nil {
		appendValue("uri", param.Activation.URI)
	}
	appendValue("resource", param.ResourceRef)
	appendValue("errorMessage", param.ErrorMessage)
	if param.Value != nil {
		parts = append(parts, "value="+encode(*param.Value))
	}
	if param.ErrorStatusCode != 0 {
		parts = append(parts, "errorCode="+strconv.Itoa(param.ErrorStatusCode))
	}
	if param.Required != nil {
		parts = append(parts, "required="+strconv.FormatBool(*param.Required))
	}
	if param.Cacheable != nil {
		parts = append(parts, "cacheable="+strconv.FormatBool(*param.Cacheable))
	}
	for _, limit := range []struct {
		name  string
		value *int
	}{
		{"minAllowedRecords", param.MinAllowedRecords}, {"maxAllowedRecords", param.MaxAllowedRecords}, {"expectedReturned", param.ExpectedReturned},
	} {
		if limit.value != nil {
			parts = append(parts, limit.name+"="+strconv.Itoa(*limit.value))
		}
	}
	if param.Async {
		parts = append(parts, "async=true")
	}
	base := "parameter:" + strconv.Quote(strings.Join(parts, ","))
	extraTags := contractFieldTags(param.Tag)
	if extraTags != "" {
		base += " " + extraTags
	}
	for _, item := range metadata {
		base = appendStructTag(base, item.name, item.value)
	}
	return base
}

func contractFieldTags(raw string) string {
	parsed := tags.NewTags(strings.TrimSpace(raw))
	filtered := parsed[:0]
	for _, item := range parsed {
		if item == nil || isCanonicalParameterTag(item.Name) {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered.Stringify()
}

func isCanonicalParameterTag(name string) bool {
	switch name {
	case "parameter", "bind", "codec", "predicate", "querySelector", "desc", "example":
		return true
	default:
		return false
	}
}

func withoutStructTags(raw string, names ...string) string {
	remove := make(map[string]bool, len(names))
	for _, name := range names {
		remove[name] = true
	}
	parsed := tags.NewTags(strings.TrimSpace(raw))
	filtered := parsed[:0]
	for _, item := range parsed {
		if item == nil || remove[item.Name] {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered.Stringify()
}

func defaultTagTypeName(tag string) string {
	typ := strings.TrimSpace(tagTypeName(tag))
	if typ == "" {
		return ""
	}
	return typ
}
