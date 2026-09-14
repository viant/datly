package dql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
)

// This file holds the per-directive value parsers that parseComponentSettings
// delegates to (report/cube, mcp, marshal/unmarshal) plus the format value
// normalizer. The cache directives live in component_settings_cache.go; the
// parse switch and spec conversion stay in component_settings.go.

func parseReportSettings(name string, args []string) (*spec.ReportSettings, error) {
	ret := &spec.ReportSettings{Enabled: true}
	if len(args) > 0 {
		ret.LinkedInputType = trimQuote(args[0])
	}
	if len(args) > 1 {
		ret.InputLayout = &spec.ReportInputLayout{Dimensions: trimQuote(args[1])}
	}
	if len(args) > 2 {
		ret.InputLayout.Measures = trimQuote(args[2])
	}
	if len(args) > 3 {
		ret.InputLayout.Filters = trimQuote(args[3])
	}
	if len(args) > 4 {
		ret.InputLayout.OrderBy = trimQuote(args[4])
	}
	if len(args) > 5 {
		ret.InputLayout.Limit = trimQuote(args[5])
	}
	if len(args) > 6 {
		ret.InputLayout.Offset = trimQuote(args[6])
	}
	if len(args) > 7 {
		enabled, err := strconv.ParseBool(strings.TrimSpace(trimQuote(args[7])))
		if err != nil {
			return nil, fmt.Errorf("invalid %s MCP tool flag: %w", name, err)
		}
		ret.MCPTool = &enabled
	}
	return ret, nil
}

func parseMCPExposure(args []string) (*spec.MCPExposure, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("invalid mcp directive: missing name")
	}
	name := strings.TrimSpace(trimQuote(args[0]))
	if name == "" {
		return nil, fmt.Errorf("invalid mcp directive: empty name")
	}
	ret := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: name}
	if len(args) > 1 {
		ret.Description = trimQuote(args[1])
	}
	if len(args) > 2 {
		ret.DescriptionPath = trimQuote(args[2])
	}
	return ret, nil
}

func parseMarshalLikeDirective(name string, args []string) (string, string, error) {
	if len(args) < 2 {
		return "", "", fmt.Errorf("invalid %s directive: expected mime and type", name)
	}
	return trimQuote(args[0]), trimQuote(args[1]), nil
}

func normalizeFormat(input string) string {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "tabular_json":
		return "tabular"
	default:
		return strings.TrimSpace(input)
	}
}
