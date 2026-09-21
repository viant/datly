package dql

import (
	"fmt"
	"strconv"
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
		case "name":
			if value == "" {
				return nil, fmt.Errorf("warmup name was empty")
			}
			if strings.Contains(value, ",") {
				return nil, fmt.Errorf("warmup name %q must be a single name", value)
			}
			ret.Name = value
		case "priority":
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return nil, fmt.Errorf("invalid warmup priority %q", value)
			}
			ret.Priority = parsed
		case "caserefs", "case_refs":
			for _, ref := range strings.Split(value, ",") {
				if ref = strings.TrimSpace(ref); ref != "" {
					ret.CaseRefs = append(ret.CaseRefs, ref)
				}
			}
			if len(ret.CaseRefs) == 0 {
				return nil, fmt.Errorf("warmup caseRefs has no values")
			}
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
		case "limit":
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsed < 0 {
				return nil, fmt.Errorf("invalid warmup limit %q", value)
			}
			ret.Limit = &parsed
		case "maxcases", "max_cases":
			parsed, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil || parsed < 0 {
				return nil, fmt.Errorf("invalid warmup max cases %q", value)
			}
			ret.MaxCases = &parsed
		case "fieldnames", "field_names", "fields":
			for _, field := range strings.Split(value, ",") {
				if field = strings.TrimSpace(field); field != "" {
					ret.FieldNames = append(ret.FieldNames, field)
				}
			}
			if len(ret.FieldNames) == 0 {
				return nil, fmt.Errorf("warmup field names were empty")
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

// parseCacheWarmupCases parses one $cache_warmup_cases declaration: a named
// reusable case set that warmups reference through CaseRefs.
func parseCacheWarmupCases(args []string) (string, *spec.CacheWarmupCase, error) {
	if len(args) < 2 {
		return "", nil, fmt.Errorf("invalid cache_warmup_cases directive: expected set name and at least one parameter")
	}
	name := strings.TrimSpace(trimQuote(args[0]))
	if name == "" {
		return "", nil, fmt.Errorf("warmup case set name was empty")
	}
	current := &spec.CacheWarmupCase{}
	for _, raw := range args[1:] {
		raw = trimQuote(raw)
		optName, value, ok := splitWarmupOption(raw)
		if !ok {
			return "", nil, fmt.Errorf("invalid warmup parameter %q, expected name=value1,value2", raw)
		}
		switch strings.ToLower(optName) {
		case "fieldnames", "field_names", "fields":
			for _, field := range strings.Split(value, ",") {
				if field = strings.TrimSpace(field); field != "" {
					current.FieldNames = append(current.FieldNames, field)
				}
			}
			if len(current.FieldNames) == 0 {
				return "", nil, fmt.Errorf("warmup field names were empty")
			}
		default:
			param := &spec.CacheWarmupParam{Name: optName}
			for _, item := range strings.Split(value, ",") {
				if item = strings.TrimSpace(item); item != "" {
					param.Values = append(param.Values, item)
				}
			}
			if len(param.Values) == 0 {
				return "", nil, fmt.Errorf("warmup parameter %q has no values", optName)
			}
			current.Set = append(current.Set, param)
		}
	}
	if len(current.Set) == 0 {
		return "", nil, fmt.Errorf("warmup case set %q has no parameters", name)
	}
	return name, current, nil
}

// appendCacheWarmupSettings applies one $cache_warmup declaration additively.
// A declaration matching an existing warmup's index identity merges into that
// warmup, preserving the singular contract; a different index identity appends
// a new warmup, keeping declaration order: singular first, then plural.
func appendCacheWarmupSettings(cache *spec.CacheSettings, incoming *spec.CacheWarmupSettings) error {
	if incoming == nil {
		return nil
	}
	if cache.Warmup == nil && len(cache.Warmups) == 0 {
		cache.Warmup = incoming
		return nil
	}
	if cache.Warmup != nil && sameCacheWarmupIndexIdentity(cache.Warmup, incoming) {
		merged, err := mergeCacheWarmupSettings(cache.Warmup, incoming)
		if err != nil {
			return err
		}
		cache.Warmup = merged
		return nil
	}
	for i, existing := range cache.Warmups {
		if existing != nil && sameCacheWarmupIndexIdentity(existing, incoming) {
			merged, err := mergeCacheWarmupSettings(existing, incoming)
			if err != nil {
				return err
			}
			cache.Warmups[i] = merged
			return nil
		}
	}
	cache.Warmups = append(cache.Warmups, incoming)
	return nil
}

// sameCacheWarmupIndexIdentity reports whether two declarations target the same
// warmup slot: neither the index column nor the index parameter explicitly differs.
func sameCacheWarmupIndexIdentity(current, incoming *spec.CacheWarmupSettings) bool {
	sameField := func(left, right string) bool {
		left, right = strings.TrimSpace(left), strings.TrimSpace(right)
		return left == "" || right == "" || strings.EqualFold(left, right)
	}
	return sameField(current.IndexColumn, incoming.IndexColumn) && sameField(current.IndexParameter, incoming.IndexParameter)
}

func mergeCacheWarmupSettings(current, incoming *spec.CacheWarmupSettings) (*spec.CacheWarmupSettings, error) {
	if current == nil {
		return incoming, nil
	}
	if incoming == nil {
		return current, nil
	}
	if current.Name != "" && incoming.Name != "" && !strings.EqualFold(strings.TrimSpace(current.Name), strings.TrimSpace(incoming.Name)) {
		return nil, fmt.Errorf("conflicting cache warmup name: %s != %s", current.Name, incoming.Name)
	}
	if current.Priority != 0 && incoming.Priority != 0 && current.Priority != incoming.Priority {
		return nil, fmt.Errorf("conflicting cache warmup priority: %d != %d", current.Priority, incoming.Priority)
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
	if merged.Name == "" {
		merged.Name = incoming.Name
	}
	if merged.Priority == 0 {
		merged.Priority = incoming.Priority
	}
	for _, ref := range incoming.CaseRefs {
		known := false
		for _, existing := range merged.CaseRefs {
			if strings.EqualFold(existing, ref) {
				known = true
				break
			}
		}
		if !known {
			merged.CaseRefs = append(append([]string(nil), merged.CaseRefs...), ref)
		}
	}
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
	if incoming.Limit != nil {
		if merged.Limit != nil && *merged.Limit != *incoming.Limit {
			return nil, fmt.Errorf("conflicting cache warmup limit: %d != %d", *merged.Limit, *incoming.Limit)
		}
		value := *incoming.Limit
		merged.Limit = &value
	}
	if incoming.MaxCases != nil {
		if merged.MaxCases != nil && *merged.MaxCases != *incoming.MaxCases {
			return nil, fmt.Errorf("conflicting cache warmup max cases: %d != %d", *merged.MaxCases, *incoming.MaxCases)
		}
		value := *incoming.MaxCases
		merged.MaxCases = &value
	}
	if len(incoming.FieldNames) > 0 {
		if len(merged.FieldNames) > 0 && strings.Join(merged.FieldNames, "\x00") != strings.Join(incoming.FieldNames, "\x00") {
			return nil, fmt.Errorf("conflicting cache warmup field names")
		}
		merged.FieldNames = append([]string(nil), incoming.FieldNames...)
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
