package tag

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	tagly "github.com/viant/tagly/tags"
)

const ViewName = "view"

type View struct {
	Name                  string
	TypeName              string
	Dest                  string
	EntityHooks           string
	URI                   string
	Connector             string
	Table                 string
	Cache                 string
	CacheWarmup           string
	OrderBy               string
	Limit                 *int
	Offset                *int
	Batch                 int
	BatchConcurrency      int
	Match                 string
	PublishParent         bool
	RelationalConcurrency int
	AllowNulls            *bool
	Groupable             *bool
	Auxiliary             bool
	Partitioning          *spec.Partitioning
	Selector              *spec.Selector
}

func ParseView(value string) (*View, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	name, values := tagly.Values(value).Name()
	result := &View{Name: strings.TrimSpace(name)}
	entityHooksSeen := false
	err := values.MatchRawPairs(func(key, value string) error {
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "table" {
			if decoded, err := strconv.Unquote(value); err == nil {
				value = decoded
			}
		}
		value = strings.TrimSpace(value)
		decoded, err := decodeScalarValue(value)
		if err != nil {
			return fmt.Errorf("view option %s: %w", key, err)
		}
		value = decoded
		switch key {
		case "name":
			result.Name = value
		case "type":
			result.TypeName = value
		case "dest":
			result.Dest = value
		case "entityhooks":
			if entityHooksSeen {
				return fmt.Errorf("view option %s is declared more than once", key)
			}
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("view option %s requires a non-empty string", key)
			}
			entityHooksSeen = true
			result.EntityHooks = value
		case "uri":
			result.URI = value
		case "connector":
			result.Connector = value
		case "table":
			result.Table = value
		case "cache":
			result.Cache = value
		case "cachewarmup":
			result.CacheWarmup = value
		case "orderby":
			result.OrderBy = value
		case "limit":
			parsed, err := nonNegativeInt(key, value)
			result.Limit = &parsed
			return err
		case "offset":
			parsed, err := nonNegativeInt(key, value)
			result.Offset = &parsed
			return err
		case "match":
			switch strings.ToLower(value) {
			case string(spec.MatchReadAll), string(spec.MatchReadMatched), string(spec.MatchReadDerived):
				result.Match = strings.ToLower(value)
			default:
				return fmt.Errorf("view option match must be read_all, read_matched, or read_derived, got %q", value)
			}
		case "batch":
			parsed, err := nonNegativeInt(key, value)
			result.Batch = parsed
			return err
		case "batchconcurrency":
			parsed, err := nonNegativeInt(key, value)
			result.BatchConcurrency = parsed
			return err
		case "publishparent":
			parsed, err := parseBool(key, value)
			result.PublishParent = parsed
			return err
		case "relationalconcurrency":
			parsed, err := nonNegativeInt(key, value)
			result.RelationalConcurrency = parsed
			return err
		case "allownulls":
			parsed, err := parseBool(key, value)
			result.AllowNulls = &parsed
			return err
		case "groupable":
			parsed, err := parseBool(key, value)
			result.Groupable = &parsed
			return err
		case "auxiliary":
			parsed, err := parseBool(key, value)
			result.Auxiliary = parsed
			return err
		case "partitioner":
			result.ensurePartitioning().Type = value
		case "concurrency":
			parsed, err := nonNegativeInt(key, value)
			result.ensurePartitioning().Concurrency = parsed
			return err
		default:
			handled, err := result.applySelectorOption(key, value)
			if handled {
				return err
			}
			return fmt.Errorf("unsupported view tag option %q", key)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if result.Partitioning != nil && strings.TrimSpace(result.Partitioning.Type) == "" {
		return result, fmt.Errorf("view option concurrency requires partitioner")
	}
	return result, nil
}

func (v *View) ensurePartitioning() *spec.Partitioning {
	if v.Partitioning == nil {
		v.Partitioning = &spec.Partitioning{}
	}
	return v.Partitioning
}

func nonNegativeInt(name, value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("view option %s must be a non-negative integer, got %q", name, value)
	}
	return parsed, nil
}

func parseBool(name, value string) (bool, error) {
	switch value {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("tag option %s must be boolean, got %q", name, value)
	}
}
