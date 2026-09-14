package dql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

func parseCacheSettings(args []string, tail string) (*spec.CacheSettings, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("invalid cache directive: missing arguments")
	}
	ret := &spec.CacheSettings{}
	first := strings.TrimSpace(trimQuote(args[0]))
	switch strings.ToLower(first) {
	case "true":
		ret.Enabled = true
	case "false":
		ret.Enabled = false
	default:
		ret.Name = first
		ret.Enabled = true
	}
	if len(args) > 1 {
		ret.TTL = trimQuote(args[1])
	}
	if strings.TrimSpace(tail) == "" {
		return ret, nil
	}
	cursor := newOptionCursor(tail)
	for cursor.next() {
		name, optArgs := cursor.option()
		switch {
		case strings.EqualFold(name, "WithProvider"):
			if len(optArgs) > 0 {
				ret.Provider = trimQuote(optArgs[len(optArgs)-1])
			}
		case strings.EqualFold(name, "WithLocation"):
			if len(optArgs) > 0 {
				ret.Location = trimQuote(optArgs[len(optArgs)-1])
			}
		case strings.EqualFold(name, "WithTimeToLiveMs"):
			if len(optArgs) > 0 {
				if ttlMs, ok := parseIntArg(optArgs[len(optArgs)-1]); ok {
					ret.TimeToLiveMs = ttlMs
				}
			}
		}
	}
	return ret, nil
}

func parseCacheWarmupSettings(args []string) (*spec.CacheWarmupSettings, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("invalid cache_warmup directive: missing index column")
	}
	ret := &spec.CacheWarmupSettings{
		IndexColumn: trimQuote(args[0]),
	}
	current := &spec.CacheWarmupCase{}
	for _, raw := range args[1:] {
		raw = trimQuote(raw)
		name, value, ok := splitWarmupOption(raw)
		if !ok {
			return nil, fmt.Errorf("invalid warmup parameter %q, expected name=value1,value2", raw)
		}
		switch strings.ToLower(name) {
		case "connector":
			if value == "" {
				return nil, fmt.Errorf("warmup connector was empty")
			}
			ret.Connector = value
		case "indexparameter", "index_param", "indexparam":
			if value == "" {
				return nil, fmt.Errorf("warmup index parameter was empty")
			}
			ret.IndexParameter = value
		case "indexmeta", "index_meta":
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "true", "1", "yes", "on":
				ret.IndexMeta = true
			case "false", "0", "no", "off":
				ret.IndexMeta = false
			default:
				return nil, fmt.Errorf("invalid warmup index meta %q", value)
			}
		default:
			values := strings.Split(value, ",")
			param := &spec.CacheWarmupParam{Name: name}
			for _, item := range values {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				param.Values = append(param.Values, item)
			}
			if len(param.Values) == 0 {
				return nil, fmt.Errorf("warmup parameter %q has no values", name)
			}
			current.Set = append(current.Set, param)
		}
	}
	if len(current.Set) > 0 {
		ret.Cases = append(ret.Cases, current)
	}
	return ret, nil
}

func mergeCacheWarmupSettings(current, incoming *spec.CacheWarmupSettings) (*spec.CacheWarmupSettings, error) {
	if current == nil {
		return incoming, nil
	}
	if incoming == nil {
		return current, nil
	}
	if current.IndexColumn != "" && incoming.IndexColumn != "" && !strings.EqualFold(strings.TrimSpace(current.IndexColumn), strings.TrimSpace(incoming.IndexColumn)) {
		return nil, fmt.Errorf("conflicting cache warmup index column: %s != %s", current.IndexColumn, incoming.IndexColumn)
	}
	if current.IndexParameter != "" && incoming.IndexParameter != "" && !strings.EqualFold(strings.TrimSpace(current.IndexParameter), strings.TrimSpace(incoming.IndexParameter)) {
		return nil, fmt.Errorf("conflicting cache warmup index parameter: %s != %s", current.IndexParameter, incoming.IndexParameter)
	}
	if current.Connector != "" && incoming.Connector != "" && !strings.EqualFold(strings.TrimSpace(current.Connector), strings.TrimSpace(incoming.Connector)) {
		return nil, fmt.Errorf("conflicting cache warmup connector: %s != %s", current.Connector, incoming.Connector)
	}
	if current.IndexMeta != incoming.IndexMeta && (current.IndexMeta || incoming.IndexMeta) {
		return nil, fmt.Errorf("conflicting cache warmup index meta: %t != %t", current.IndexMeta, incoming.IndexMeta)
	}
	merged := *current
	if merged.IndexColumn == "" {
		merged.IndexColumn = incoming.IndexColumn
	}
	if merged.IndexParameter == "" {
		merged.IndexParameter = incoming.IndexParameter
	}
	if merged.Connector == "" {
		merged.Connector = incoming.Connector
	}
	merged.IndexMeta = merged.IndexMeta || incoming.IndexMeta
	if incoming.Cases != nil {
		merged.Cases = append(append([]*spec.CacheWarmupCase{}, current.Cases...), incoming.Cases...)
	}
	return &merged, nil
}

func splitWarmupOption(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}
