package tags

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strings"
)

const ParameterTag = "parameter"

type Parameter struct {
	Name     string `tag:"name,omitempty"`
	Kind     string `tag:"kind,omitempty"`  //parameter location kind
	In       string `tag:"in,omitempty"`    //parameter location name
	When     string `tag:"when,omitempty"`  //condition to evaluate
	Scope    string `tag:"scope,omitempty"` //input/output/async
	Lazy     bool   `tag:"lazy,omitempty"`
	DataType string `tag:"dataType,omitempty"` //parameter input type
	With     string `tag:"with,omitempty"`     //optional auxiliary type name holding parameters
	Required bool   `tag:"required,omitempty"`
}

func (t *Tag) updatedParameter(key string, value string) (err error) {
	tag := t.Parameter
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "kind":
		tag.Kind = strings.TrimSpace(value)
	case "in":
		tag.In = strings.Trim(strings.TrimSpace(value), "{}")
	case "when":
		tag.When = strings.TrimSpace(value)
	case "scope":
		tag.Scope = strings.TrimSpace(value)
	case "datatype":
		tag.DataType = strings.TrimSpace(value)
	case "lazy":
		tag.Lazy = strings.TrimSpace(value) == "true" || value == ""
	case "with":
		tag.With = value
	case "required":
		tag.Required = true
	default:
		return fmt.Errorf("invalid paramerer tag key: '%s'", key)
	}
	return err
}

func (p *Parameter) Tag() *tags.Tag {
	builder := &strings.Builder{}
	builder.WriteString(p.Name)
	appendNonEmpty(builder, "kind", p.Kind)
	appendNonEmpty(builder, "in", p.In)
	appendNonEmpty(builder, "when", p.When)
	appendNonEmpty(builder, "with", p.With)
	appendNonEmpty(builder, "scope", p.Scope)
	appendNonEmpty(builder, "dataType", p.DataType)
	if p.Lazy {
		appendNonEmpty(builder, "lazy", "true")
	}

	if p.Required {
		appendNonEmpty(builder, "required", "true")
	}
	return &tags.Tag{Name: ParameterTag, Values: tags.Values(builder.String())}
}
