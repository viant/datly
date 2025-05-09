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

	//View represent basic view tag
	View struct {
		Name                   string
		Table                  string
		CustomTag              string
		Parameters             []string //parameter references
		Connector              string
		Cache                  string
		Limit                  *int
		Match                  string
		Batch                  int
		PublishParent          bool
		PartitionerType        string
		PartitionedConcurrency int
		RelationalConcurrency  int
	}
)

func (t *Tag) updateView(key string, value string) error {
	tag := t.View
	switch strings.ToLower(key) {
	case "tag":
		tag.CustomTag = strings.TrimSpace(value)
	case "name":
		tag.Name = strings.TrimSpace(value)
	case "match":
		tag.Match = strings.TrimSpace(value)
	case "publishparent":
		tag.PublishParent = strings.TrimSpace(value) == "true"
	case "batch":
		var err error
		tag.Batch, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
	case "cache":
		tag.Cache = strings.TrimSpace(value)
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
	case "partitioner":
		tag.PartitionerType = strings.TrimSpace(value)
	case "concurrency":
		concurrency, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		tag.PartitionedConcurrency = concurrency
	case "relationalconcurrency":
		rConcurrency, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		tag.RelationalConcurrency = rConcurrency
	case "parameters":
		parameters := strings.Trim(value, "{}'\"")
		for _, parameter := range strings.Split(parameters, ",") {
			tag.Parameters = append(tag.Parameters, strings.TrimSpace(parameter))
		}
	default:
		return fmt.Errorf("unsupported view tag option: '%s'", key)
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
	if v.Batch > 0 {
		appendNonEmpty(builder, "batch", strconv.Itoa(v.Batch))
	}
	if v.RelationalConcurrency > 0 {
		appendNonEmpty(builder, "relationalConcurrency", strconv.Itoa(v.RelationalConcurrency))
	}
	if v.PublishParent {
		appendNonEmpty(builder, "publishParent", strconv.FormatBool(v.PublishParent))
	}
	appendNonEmpty(builder, "connector", v.Connector)
	appendNonEmpty(builder, "cache", v.Cache)
	appendNonEmpty(builder, "match", v.Match)
	if len(v.Parameters) > 0 {
		appendNonEmpty(builder, "parameters", "{"+strings.Join(v.Parameters, ",")+"}")
	}
	if v.PartitionerType != "" {
		appendNonEmpty(builder, "partitioner", v.PartitionerType)
		if v.PartitionedConcurrency > 0 {
			appendNonEmpty(builder, "concurrency", strconv.Itoa(v.PartitionedConcurrency))
		}
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
