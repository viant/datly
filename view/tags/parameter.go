package tags

import (
	"fmt"
	"github.com/viant/structology/tags"
	"strings"
)

const ParameterTag = "parameter"

type Parameter struct {
	Name       string
	Kind       string //parameter location kind
	In         string //parameter location name
	When       string //condition to evaluate
	Scope      string //input/output/async
	Lazy       bool
	Selector   string
	Parameters []string
}

func (t *Tag) updatedParameter(key string, value string) (err error) {
	tag := t.Parameter
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "kind":
		tag.Kind = strings.TrimSpace(value)
	case "in":
		tag.In = strings.TrimSpace(value)
	case "when":
		tag.When = strings.TrimSpace(value)
	case "scope":
		tag.Scope = strings.TrimSpace(value)
	case "lazy":
		tag.Lazy = strings.TrimSpace(value) == "true" || value == ""
	case "selector":
		tag.Selector = strings.TrimSpace(value)
	case "parameters":
		tag.Parameters = strings.Split(strings.Trim(value, "{}'"), ",")
	default:
		return fmt.Errorf("invalid paramerer tag key %s", key)
	}
	return err
}

func (p *Parameter) Tag() *tags.Tag {
	builder := &strings.Builder{}
	builder.WriteString(p.Name)
	appendNonEmpty(builder, "kind", p.Kind)
	appendNonEmpty(builder, "in", p.In)
	appendNonEmpty(builder, "selector", p.Selector)
	appendNonEmpty(builder, "when", p.When)
	appendNonEmpty(builder, "scope", p.Scope)
	if p.Lazy {
		appendNonEmpty(builder, "lazy", "true")
	}
	if len(p.Parameters) > 0 {
		appendNonEmpty(builder, "parameters", "{"+strings.Join(p.Parameters, ",")+"}")
	}
	return &tags.Tag{Name: ParameterTag, Values: tags.Values(builder.String())}
}
