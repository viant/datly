package openapi

import (
	"fmt"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/tagly/format"
)

func projectInputAliases(object *openapi3.Schema, canonical string, property *openapi3.Schema, policy *format.Tag, required bool) bool {
	if policy.InputNames == "" {
		return required
	}
	conservative := policy.InputNamesCase != "" || policy.InputNamesPriority != ""
	description := fmt.Sprintf("Native input alias for %s. Names=%s; priority=%s; kinds=%s; case=%s; exclusions=%s. The runtime enforces this policy. OpenAPI 3.0 cannot fully express folded names and canonical precedence; required presence and winning-value validation remain runtime responsibilities.", canonical, policy.InputNames, policy.InputNamesPriority, policy.InputNamesKinds, policy.InputNamesCase, policy.InputNamesExclude)
	excluded := map[string]bool{}
	for _, name := range strings.Split(policy.InputNamesExclude, "|") {
		excluded[name] = true
	}
	choices := openapi3.SchemaList{{Required: []string{canonical}}}
	for _, alias := range strings.Split(policy.InputNames, "|") {
		if alias == canonical || excluded[alias] {
			continue
		}
		admitted, aliasSchema := property, property
		if conservative {
			aliasSchema = &openapi3.Schema{Description: description}
		} else if policy.InputNamesKinds != "" {
			var allowed openapi3.SchemaList
			for _, kind := range strings.Split(policy.InputNamesKinds, "|") {
				if kind == "null" {
					allowed = append(allowed, &openapi3.Schema{Type: "object", Nullable: true, Enum: []any{nil}})
				} else {
					allowed = append(allowed, &openapi3.Schema{Type: kind})
				}
			}
			kinds := &openapi3.Schema{AnyOf: allowed}
			admitted = &openapi3.Schema{AllOf: openapi3.SchemaList{property, kinds}}
			aliasSchema = &openapi3.Schema{AnyOf: openapi3.SchemaList{admitted, {Not: kinds}}}
		}
		object.Properties[alias] = aliasSchema
		choices = append(choices, &openapi3.Schema{Required: []string{alias}, Properties: openapi3.Schemas{alias: admitted}})
	}
	if conservative {
		if policy.InputNamesPriority == "alias" {
			object.Properties[canonical] = &openapi3.Schema{Description: description}
		}
		object.Description = strings.TrimSpace(object.Description + " " + description)
		return false
	}
	if required && len(choices) > 1 {
		object.AllOf = append(object.AllOf, &openapi3.Schema{AnyOf: choices})
		return false
	}
	return required
}
