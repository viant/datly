package tags

import (
	_ "embed"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/structology/tags"
	"strings"
)

const (
	ViewTag = "view"
)

type (

	//Tag represent basic view tag
	View struct {
		Name       string
		Table      string
		Parameters []string //parameter references
		Connector  string
	}
)

func (t *Tag) updateView(key string, value string) error {
	tag := t.View
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "table":
		tag.Table = strings.TrimSpace(value)
	case "connector":
		tag.Connector = strings.TrimSpace(value)
	case "parameters":
		parameters := strings.Trim(value, "{}'\"")
		for _, parameter := range strings.Split(parameters, ",") {
			tag.Parameters = append(tag.Parameters, strings.TrimSpace(parameter))
		}
	default:
		return fmt.Errorf("unsupported view tag option: %s", key)
	}
	return nil
}

func (t *Tag) getOptions() []storage.Option {
	var options []storage.Option
	if t.embed != nil {
		options = append(options, t.embed)
	}
	return options
}

func (v *View) Tag() *tags.Tag {
	builder := &strings.Builder{}
	if v == nil {
		return nil
	}
	builder.WriteString(v.Name)
	appendNonEmpty(builder, "table", v.Table)
	appendNonEmpty(builder, "connector", v.Connector)
	if len(v.Parameters) > 0 {
		appendNonEmpty(builder, "parameters", "{"+strings.Join(v.Parameters, ",")+"}")
	}
	return &tags.Tag{Name: ViewTag, Values: tags.Values(builder.String())}
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
