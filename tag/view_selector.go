package tag

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	tagly "github.com/viant/tagly/tags"
)

func (v *View) applySelectorOption(key, value string) (bool, error) {
	switch key {
	case "selectornamespace":
		v.ensureSelector().Namespace = value
	case "selectorprojection":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowFields = parsed
		return true, err
	case "selectororderby":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowOrderBy = parsed
		return true, err
	case "selectorcriteria":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowCriteria = parsed
		return true, err
	case "selectorlimit":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowLimit = parsed
		return true, err
	case "selectoroffset":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowOffset = parsed
		return true, err
	case "selectorpage":
		parsed, err := parseBool(key, value)
		v.ensureSelector().AllowPage = parsed
		return true, err
	case "selectorfilterable":
		paths, err := parseFieldPaths(value)
		v.ensureSelector().Filterable = paths
		return true, err
	case "selectorsqlmethods":
		methods, err := parseSQLMethods(value)
		v.ensureSelector().SQLMethods = methods
		return true, err
	case "selectororderable":
		paths, err := parseFieldPaths(value)
		v.ensureSelector().Orderable = paths
		return true, err
	case "selectordefaultorder":
		v.ensureSelector().DefaultOrder = value
	case "selectordefaultlimit":
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || parsed < 0 {
			return true, fmt.Errorf("selector default limit must be a non-negative integer")
		}
		v.ensureSelector().DefaultLimit = parsed
	case "selectornolimit":
		parsed, err := parseBool(key, value)
		v.ensureSelector().NoLimit = parsed
		return true, err
	case "selectororderbycolumns":
		aliases, err := parseFieldPathMap(value)
		v.ensureSelector().OrderAliases = aliases
		return true, err
	default:
		return false, nil
	}
	return true, nil
}

func (v *View) ensureSelector() *spec.Selector {
	if v.Selector == nil {
		v.Selector = &spec.Selector{}
	}
	return v.Selector
}

func parseFieldPaths(value string) ([]spec.FieldPath, error) {
	value = strings.Trim(strings.TrimSpace(value), "{}")
	if value == "" {
		return nil, nil
	}
	var result []spec.FieldPath
	err := tagly.Values(value).Match(func(item string) error {
		item = strings.TrimSpace(item)
		if item == "" {
			return fmt.Errorf("selector field path is required")
		}
		result = append(result, spec.FieldPath(item))
		return nil
	})
	return result, err
}

func parseFieldPathMap(value string) (map[string]spec.FieldPath, error) {
	value = strings.Trim(strings.TrimSpace(value), "{}")
	if value == "" {
		return nil, nil
	}
	result := map[string]spec.FieldPath{}
	err := tagly.Values(value).Match(func(item string) error {
		alias, field, ok := strings.Cut(strings.TrimSpace(item), ":")
		alias = strings.TrimSpace(alias)
		field = strings.TrimSpace(field)
		if !ok || alias == "" || field == "" || strings.Contains(field, ":") {
			return fmt.Errorf("invalid selector order alias %q", item)
		}
		if _, exists := result[alias]; exists {
			return fmt.Errorf("duplicate selector order alias %q", alias)
		}
		result[alias] = spec.FieldPath(field)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
