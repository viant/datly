package tags

import (
	"fmt"
	"github.com/viant/tagly/tags"
	"strings"
)

// HandlerTag Handler tag
const HandlerTag = "handler"

type Handler struct {
	Name      string   `tag:"name,omitempty"`
	Arguments []string `tag:"arguments,omitempty"`
}

func (t *Tag) updatedHandler(key string, value string) (err error) {
	tag := t.Handler
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	default:
		if value != "" {
			return fmt.Errorf("invalid argument %s", value)
		}
		tag.Arguments = append(tag.Arguments, key)
	}
	return err
}

func (p *Handler) Tag() *tags.Tag {
	builder := &strings.Builder{}
	if p == nil {
		return nil
	}
	builder.WriteString(p.Name)
	for _, arg := range p.Arguments {
		builder.WriteString(",")
		builder.WriteString(arg)
	}
	return &tags.Tag{Name: HandlerTag, Values: tags.Values(builder.String())}
}
