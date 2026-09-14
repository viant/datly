package tag

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Value formats view metadata using the same grammar accepted by ParseView.
func (v View) Value() (string, error) {
	if v.BatchConcurrency < 0 {
		return "", fmt.Errorf("view option batchConcurrency must be a non-negative integer")
	}
	var values []string
	if name := strings.TrimSpace(v.Name); name != "" {
		if err := validateTagToken("view name", name); err != nil {
			return "", err
		}
		values = append(values, name)
	}
	appendPair := func(name, value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		if name == "table" {
			if strings.ContainsAny(value, "\n\r") {
				return fmt.Errorf("view table contains an unsupported delimiter")
			}
		} else {
			if err := validateTagToken("view "+name, value); err != nil {
				return err
			}
		}
		values = append(values, name+"="+encodeScalarValue(value))
		return nil
	}
	for _, item := range []struct{ name, value string }{
		{"type", v.TypeName}, {"dest", v.Dest}, {"entityHooks", v.EntityHooks}, {"uri", v.URI}, {"connector", v.Connector}, {"table", v.Table},
		{"cache", v.Cache}, {"cacheWarmup", v.CacheWarmup},
		{"orderBy", v.OrderBy}, {"match", v.Match},
	} {
		if err := appendPair(item.name, item.value); err != nil {
			return "", err
		}
	}
	if v.Limit != nil {
		values = append(values, "limit="+strconv.Itoa(*v.Limit))
	}
	if v.Offset != nil {
		values = append(values, "offset="+strconv.Itoa(*v.Offset))
	}
	if v.Batch > 0 {
		values = append(values, "batch="+strconv.Itoa(v.Batch))
	}
	if v.BatchConcurrency > 0 {
		values = append(values, "batchConcurrency="+strconv.Itoa(v.BatchConcurrency))
	}
	if v.PublishParent {
		values = append(values, "publishParent=true")
	}
	if v.Auxiliary {
		values = append(values, "auxiliary=true")
	}
	if v.RelationalConcurrency > 0 {
		values = append(values, "relationalConcurrency="+strconv.Itoa(v.RelationalConcurrency))
	}
	if v.AllowNulls != nil {
		values = append(values, "allowNulls="+strconv.FormatBool(*v.AllowNulls))
	}
	if v.Groupable != nil {
		values = append(values, "groupable="+strconv.FormatBool(*v.Groupable))
	}
	if v.Partitioning != nil {
		if err := appendPair("partitioner", v.Partitioning.Type); err != nil {
			return "", err
		}
		if v.Partitioning.Concurrency > 0 {
			values = append(values, "concurrency="+strconv.Itoa(v.Partitioning.Concurrency))
		}
	}
	if v.Selector != nil {
		if err := appendPair("selectorNamespace", v.Selector.Namespace); err != nil {
			return "", err
		}
		appendFlag := func(name string, enabled bool) {
			if enabled {
				values = append(values, name+"=true")
			}
		}
		appendFlag("selectorProjection", v.Selector.AllowFields)
		appendFlag("selectorOrderBy", v.Selector.AllowOrderBy)
		appendFlag("selectorCriteria", v.Selector.AllowCriteria)
		appendFlag("selectorLimit", v.Selector.AllowLimit)
		appendFlag("selectorOffset", v.Selector.AllowOffset)
		appendFlag("selectorPage", v.Selector.AllowPage)
		if len(v.Selector.SQLMethods) > 0 {
			encoded, err := encodeSQLMethods(v.Selector.SQLMethods)
			if err != nil {
				return "", err
			}
			values = append(values, "selectorSQLMethods="+encodeScalarValue(encoded))
		}
		if len(v.Selector.Filterable) > 0 {
			items := make([]string, 0, len(v.Selector.Filterable))
			for _, item := range v.Selector.Filterable {
				items = append(items, string(item))
			}
			values = append(values, "selectorFilterable={"+strings.Join(items, ",")+"}")
		}
		if len(v.Selector.Orderable) > 0 {
			items := make([]string, 0, len(v.Selector.Orderable))
			for _, item := range v.Selector.Orderable {
				items = append(items, string(item))
			}
			values = append(values, "selectorOrderable={"+strings.Join(items, ",")+"}")
		}
		if err := appendPair("selectorDefaultOrder", v.Selector.DefaultOrder); err != nil {
			return "", err
		}
		if v.Selector.DefaultLimit > 0 {
			values = append(values, "selectorDefaultLimit="+strconv.Itoa(v.Selector.DefaultLimit))
		}
		if v.Selector.NoLimit {
			values = append(values, "selectorNoLimit=true")
		}
		if len(v.Selector.OrderAliases) > 0 {
			aliases := make([]string, 0, len(v.Selector.OrderAliases))
			for alias, target := range v.Selector.OrderAliases {
				aliases = append(aliases, alias+":"+string(target))
			}
			sort.Strings(aliases)
			values = append(values, "selectorOrderByColumns={"+strings.Join(aliases, ",")+"}")
		}
	}
	return strings.Join(values, ","), nil
}

func validateTagToken(name, value string) error {
	if strings.ContainsAny(value, "\"`\n\r") {
		return fmt.Errorf("%s contains an unsupported delimiter", name)
	}
	return nil
}
