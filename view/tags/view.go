package tags

import (
	_ "embed"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/tagly/tags"
	"sort"
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
		SummaryURI             string
		TypeName               string
		Dest                   string
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
		Groupable              *bool
		SelectorNamespace      string
		SelectorCriteria       *bool
		SelectorProjection     *bool
		SelectorOrderBy        *bool
		SelectorOffset         *bool
		SelectorPage           *bool
		SelectorFilterable     []string
		SelectorOrderByColumns map[string]string
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
	case "summaryuri":
		tag.SummaryURI = strings.TrimSpace(value)
	case "type":
		tag.TypeName = strings.TrimSpace(value)
	case "dest":
		tag.Dest = strings.TrimSpace(value)
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
	case "groupable":
		tag.Groupable = parseBoolPointer(value)
	case "selectornamespace":
		tag.SelectorNamespace = strings.TrimSpace(value)
	case "selectorcriteria":
		tag.SelectorCriteria = parseBoolPointer(value)
	case "selectorprojection":
		tag.SelectorProjection = parseBoolPointer(value)
	case "selectororderby":
		tag.SelectorOrderBy = parseBoolPointer(value)
	case "selectoroffset":
		tag.SelectorOffset = parseBoolPointer(value)
	case "selectorpage":
		tag.SelectorPage = parseBoolPointer(value)
	case "selectorfilterable":
		tag.SelectorFilterable = parseTagList(value)
	case "selectororderbycolumns":
		tag.SelectorOrderByColumns = parseTagMap(value)
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
	appendNonEmpty(builder, "summaryURI", v.SummaryURI)
	appendNonEmpty(builder, "type", v.TypeName)
	appendNonEmpty(builder, "dest", v.Dest)
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
	appendBool(builder, "groupable", v.Groupable)
	appendNonEmpty(builder, "selectorNamespace", v.SelectorNamespace)
	appendBool(builder, "selectorCriteria", v.SelectorCriteria)
	appendBool(builder, "selectorProjection", v.SelectorProjection)
	appendBool(builder, "selectorOrderBy", v.SelectorOrderBy)
	appendBool(builder, "selectorOffset", v.SelectorOffset)
	appendBool(builder, "selectorPage", v.SelectorPage)
	if len(v.SelectorFilterable) > 0 {
		appendNonEmpty(builder, "selectorFilterable", "{"+strings.Join(v.SelectorFilterable, ",")+"}")
	}
	if len(v.SelectorOrderByColumns) > 0 {
		keys := make([]string, 0, len(v.SelectorOrderByColumns))
		for key := range v.SelectorOrderByColumns {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		pairs := make([]string, 0, len(keys))
		for _, key := range keys {
			pairs = append(pairs, key+":"+v.SelectorOrderByColumns[key])
		}
		appendNonEmpty(builder, "selectorOrderByColumns", "{"+strings.Join(pairs, ",")+"}")
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

func appendBool(builder *strings.Builder, key string, value *bool) {
	if value == nil {
		return
	}
	appendNonEmpty(builder, key, strconv.FormatBool(*value))
}

func parseBoolPointer(value string) *bool {
	v := true
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "true", "1":
		v = true
	case "false", "0":
		v = false
	}
	return &v
}

func parseTagList(value string) []string {
	value = strings.Trim(value, "{}'\" ")
	if value == "" {
		return nil
	}
	items := strings.Split(value, ",")
	ret := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			ret = append(ret, item)
		}
	}
	return ret
}

func parseTagMap(value string) map[string]string {
	value = strings.Trim(value, "{}'\" ")
	if value == "" {
		return nil
	}
	ret := map[string]string{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, mapped, ok := strings.Cut(item, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		mapped = strings.TrimSpace(mapped)
		if key != "" && mapped != "" {
			ret[key] = mapped
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}
