package componenttag

import (
	"fmt"
	"reflect"
	"strings"

	tagtags "github.com/viant/tagly/tags"
)

const TagName = "component"

type Component struct {
	Name       string
	Path       string
	Method     string
	Connector  string
	Marshaller string
	Handler    string
	Input      string
	Output     string
	View       string
	Source     string
	Summary    string
}

type Tag struct {
	Component *Component
}

func (c *Component) Tag() *tagtags.Tag {
	if c == nil {
		return nil
	}
	builder := &strings.Builder{}
	builder.WriteString(c.Name)
	appendNonEmpty(builder, "path", c.Path)
	appendNonEmpty(builder, "method", c.Method)
	appendNonEmpty(builder, "connector", c.Connector)
	appendNonEmpty(builder, "marshaller", c.Marshaller)
	appendNonEmpty(builder, "handler", c.Handler)
	appendNonEmpty(builder, "input", c.Input)
	appendNonEmpty(builder, "output", c.Output)
	appendNonEmpty(builder, "view", c.View)
	appendNonEmpty(builder, "source", c.Source)
	appendNonEmpty(builder, "summary", c.Summary)
	return &tagtags.Tag{Name: TagName, Values: tagtags.Values(builder.String())}
}

func Parse(tag reflect.StructTag) (*Tag, error) {
	tagValue, ok := tag.Lookup(TagName)
	if !ok {
		return &Tag{}, nil
	}
	name, values := tagtags.Values(tagValue).Name()
	component := &Component{Name: name}
	if err := values.MatchPairs(func(key, value string) error {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "name":
			component.Name = strings.TrimSpace(value)
		case "path":
			component.Path = strings.TrimSpace(value)
		case "method":
			component.Method = strings.TrimSpace(value)
		case "connector":
			component.Connector = strings.TrimSpace(value)
		case "marshaller":
			component.Marshaller = strings.TrimSpace(value)
		case "handler":
			component.Handler = strings.TrimSpace(value)
		case "input":
			component.Input = strings.TrimSpace(value)
		case "output":
			component.Output = strings.TrimSpace(value)
		case "view":
			component.View = strings.TrimSpace(value)
		case "source":
			component.Source = strings.TrimSpace(value)
		case "summary":
			component.Summary = strings.TrimSpace(value)
		default:
			return fmt.Errorf("unsupported component tag option: '%s'", key)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &Tag{Component: component}, nil
}

func appendNonEmpty(builder *strings.Builder, key, value string) {
	if value == "" {
		return
	}
	builder.WriteString(",")
	builder.WriteString(key)
	builder.WriteString("=")
	builder.WriteString(value)
}
