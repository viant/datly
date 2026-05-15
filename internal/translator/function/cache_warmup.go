package function

import (
	"fmt"
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

type cacheWarmup struct{}

func (c *cacheWarmup) Apply(args []string, column *sqlparser.Column, resource *view.Resource, aView *view.View) error {
	if _, err := convertArguments(c, args); err != nil {
		return err
	}
	if aView.Cache == nil {
		return fmt.Errorf("cache_warmup requires cache to be configured first")
	}

	warmup := &view.Warmup{IndexColumn: args[0]}
	parameters := &view.CacheParameters{}
	for _, raw := range args[1:] {
		if connector, ok, err := parseWarmupConnector(raw); ok || err != nil {
			if err != nil {
				return err
			}
			warmup.Connector = connector
			continue
		}
		if indexParameter, ok, err := parseWarmupIndexParameter(raw); ok || err != nil {
			if err != nil {
				return err
			}
			warmup.IndexParameter = indexParameter
			continue
		}
		param, err := parseWarmupParam(raw)
		if err != nil {
			return err
		}
		parameters.Set = append(parameters.Set, param)
	}
	if len(parameters.Set) > 0 {
		warmup.Cases = append(warmup.Cases, parameters)
	}
	aView.Cache.Warmup = warmup
	return nil
}

func parseWarmupConnector(raw string) (*view.Connector, bool, error) {
	name, value, ok := splitWarmupOption(raw)
	if !ok {
		return nil, false, nil
	}
	switch strings.ToLower(name) {
	case "connector":
	default:
		return nil, false, nil
	}
	if value == "" {
		return nil, true, fmt.Errorf("warmup connector was empty")
	}
	if strings.Contains(value, ",") {
		return nil, true, fmt.Errorf("warmup connector %q must be a single connector name", value)
	}
	return view.NewRefConnector(value), true, nil
}

func parseWarmupIndexParameter(raw string) (string, bool, error) {
	name, value, ok := splitWarmupOption(raw)
	if !ok {
		return "", false, nil
	}
	switch strings.ToLower(name) {
	case "indexparameter", "index_param", "indexparam":
	default:
		return "", false, nil
	}
	if value == "" {
		return "", true, fmt.Errorf("warmup index parameter was empty")
	}
	if strings.Contains(value, ",") {
		return "", true, fmt.Errorf("warmup index parameter %q must be a single parameter name", value)
	}
	return value, true, nil
}

func parseWarmupParam(raw string) (*view.ParamValue, error) {
	name, rawValues, ok := splitWarmupOption(raw)
	if !ok {
		return nil, fmt.Errorf("invalid warmup parameter %q, expected name=value1,value2", raw)
	}
	if name == "" {
		return nil, fmt.Errorf("warmup parameter name was empty")
	}

	values := strings.Split(rawValues, ",")
	result := &view.ParamValue{Name: name, Values: make([]interface{}, 0, len(values)), ExcludeDefault: true}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		result.Values = append(result.Values, value)
	}
	if len(result.Values) == 0 {
		return nil, fmt.Errorf("warmup parameter %q has no values", name)
	}
	return result, nil
}

func splitWarmupOption(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}

func (c *cacheWarmup) Name() string {
	return "cache_warmup"
}

func (c *cacheWarmup) Description() string {
	return "set view.Cache.Warmup connector and parameter permutations"
}

func (c *cacheWarmup) Arguments() []*Argument {
	return []*Argument{
		{
			Name:        "indexColumn",
			Description: "cache warmup index column",
			Required:    true,
			DataType:    "string",
		},
	}
}
