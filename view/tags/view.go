package tags

import (
	_ "embed"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/tagly/tags"
	"strconv"
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
		Limit      *int
		Match      string
		BatchSize  int
	}
)

func (t *Tag) updateView(key string, value string) error {
	tag := t.View
	switch strings.ToLower(key) {
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "match":
		tag.Match = strings.TrimSpace(value)
	case "batch":
		var err error
		tag.BatchSize, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
	case "limit":
		limit, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		tag.Limit = &limit
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
	if v.Limit != nil {
		appendNonEmpty(builder, "limit", strconv.Itoa(*v.Limit))
	}
	appendNonEmpty(builder, "table", v.Table)
	if v.BatchSize > 0 {
		appendNonEmpty(builder, "batch", strconv.Itoa(v.BatchSize))
	}
	appendNonEmpty(builder, "connector", v.Connector)
	appendNonEmpty(builder, "match", v.Match)
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
