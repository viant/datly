package tag

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	tagly "github.com/viant/tagly/tags"
	xdocs "github.com/viant/xdatly/docs"
)

const ComponentName = "component"

const (
	RouteNameTag    = "routeName"
	APIKeyHeaderTag = "apiKeyHeader"
	APIKeyValueTag  = "apiKeyValue"
	MCPTag          = "mcp"
)

// Component is the metadata carried by a component holder field.
type Component struct {
	Documentation         xdocs.Source
	Name                  string
	RouteName             string
	Path                  string
	Method                string
	Connector             string
	Marshaller            string
	Handler               string
	APIKeyHeader          string
	APIKeyValue           string
	Input                 string
	Output                string
	View                  string
	Source                string
	Description           string
	Example               string
	Report                bool
	ReportCompose         *spec.CubeComposeSettings
	ReportMCPTool         *bool
	ReportLinkedInputType string
	ReportDimensions      string
	ReportMeasures        string
	ReportFilters         string
	ReportOrderBy         string
	ReportLimit           string
	ReportOffset          string
	MCP                   []*spec.MCPExposure
	Settings              Settings
}

func (c Component) ValidateRoute() error {
	if strings.TrimSpace(c.Path) == "" {
		return fmt.Errorf("component tag path is required")
	}
	if strings.TrimSpace(c.Method) == "" {
		return fmt.Errorf("component tag method is required")
	}
	return nil
}

func (c Component) value() string {
	builder := &strings.Builder{}
	builder.WriteString(strings.TrimSpace(c.Name))
	appendNonEmpty(builder, "path", c.Path)
	appendNonEmpty(builder, "method", c.Method)
	appendNonEmpty(builder, "connector", c.Connector)
	appendNonEmpty(builder, "marshaller", c.Marshaller)
	appendNonEmpty(builder, "handler", c.Handler)
	appendNonEmpty(builder, "input", c.Input)
	appendNonEmpty(builder, "output", c.Output)
	appendNonEmpty(builder, "view", c.View)
	appendNonEmpty(builder, "source", c.Source)
	if c.Report {
		appendNonEmpty(builder, "report", "true")
	}
	if c.ReportCompose != nil {
		appendNonEmpty(builder, "reportCompose", strconv.FormatBool(c.ReportCompose.Enabled))
		appendNonEmpty(builder, "reportComposeMaxCubes", strconv.Itoa(c.ReportCompose.MaxCubes))
		appendNonEmpty(builder, "reportComposeMaxLimit", strconv.Itoa(c.ReportCompose.MaxLimit))
		appendNonEmpty(builder, "reportComposeTimeoutMs", strconv.Itoa(c.ReportCompose.TimeoutMs))
		if c.ReportCompose.MCPTool != nil {
			appendNonEmpty(builder, "reportComposeMCPTool", strconv.FormatBool(*c.ReportCompose.MCPTool))
		}
	}
	if c.ReportMCPTool != nil {
		appendNonEmpty(builder, "reportMCPTool", strconv.FormatBool(*c.ReportMCPTool))
	}
	appendNonEmpty(builder, "reportLinkedInputType", c.ReportLinkedInputType)
	appendNonEmpty(builder, "reportDimensions", c.ReportDimensions)
	appendNonEmpty(builder, "reportMeasures", c.ReportMeasures)
	appendNonEmpty(builder, "reportFilters", c.ReportFilters)
	appendNonEmpty(builder, "reportOrderBy", c.ReportOrderBy)
	appendNonEmpty(builder, "reportLimit", c.ReportLimit)
	appendNonEmpty(builder, "reportOffset", c.ReportOffset)
	return builder.String()
}

func (c Component) StructTag() (string, error) {
	if err := validateComponentValues(c); err != nil {
		return "", err
	}
	value := c.value()
	if value == "" {
		return "", nil
	}
	result := ComponentName + ":" + strconv.Quote(value)
	if c.RouteName != "" {
		result += " " + RouteNameTag + ":" + strconv.Quote(c.RouteName)
	}
	if c.APIKeyHeader != "" {
		result += " " + APIKeyHeaderTag + ":" + strconv.Quote(c.APIKeyHeader)
	}
	if c.APIKeyValue != "" {
		result += " " + APIKeyValueTag + ":" + strconv.Quote(c.APIKeyValue)
	}
	if len(c.Documentation.Substitutes) > 0 {
		data, _ := json.Marshal(c.Documentation.Substitutes)
		result += " docSubstitutes:" + strconv.Quote(string(data))
	}
	if c.Documentation.DocURL != "" {
		result += " docURL:" + strconv.Quote(c.Documentation.DocURL)
	}
	if c.Documentation.BaseURL != "" {
		result += " docBaseURL:" + strconv.Quote(c.Documentation.BaseURL)
	}
	if len(c.Documentation.GlobalURLs) > 0 {
		data, _ := json.Marshal(c.Documentation.GlobalURLs)
		result += " docGlobalURLs:" + strconv.Quote(string(data))
	}
	if len(c.Documentation.DocURLs) > 0 {
		data, _ := json.Marshal(c.Documentation.DocURLs)
		result += " docURLs:" + strconv.Quote(string(data))
	}
	if c.Description != "" {
		result += " " + DescriptionName + ":" + strconv.Quote(c.Description)
	}
	if c.Example != "" {
		result += " " + ExampleName + ":" + strconv.Quote(c.Example)
	}
	if len(c.MCP) > 0 {
		value, err := json.Marshal(c.MCP)
		if err != nil {
			return "", fmt.Errorf("marshal MCP route tag: %w", err)
		}
		result += " " + MCPTag + ":" + strconv.Quote(string(value))
	}
	settingsTag, err := c.Settings.StructTag()
	if err != nil {
		return "", err
	}
	if settingsTag != "" {
		result += " " + settingsTag
	}
	return result, nil
}

func ParseComponent(structTag reflect.StructTag) (Component, bool, error) {
	value, ok := structTag.Lookup(ComponentName)
	if !ok {
		return Component{}, false, nil
	}
	parsed, parseErr := ParseComponentValue(value)
	parsed.RouteName = structTag.Get(RouteNameTag)
	parsed.APIKeyHeader = structTag.Get(APIKeyHeaderTag)
	parsed.APIKeyValue = structTag.Get(APIKeyValueTag)
	parsed.Description = strings.TrimSpace(structTag.Get(DescriptionName))
	parsed.Example = strings.TrimSpace(structTag.Get(ExampleName))
	parsed.Documentation.DocURL = structTag.Get("docURL")
	if value, ok := structTag.Lookup("docGlobalURLs"); ok {
		if err := json.Unmarshal([]byte(value), &parsed.Documentation.GlobalURLs); err != nil {
			return parsed, true, err
		}
	}
	parsed.Documentation.BaseURL = structTag.Get("docBaseURL")
	if value, ok := structTag.Lookup("docSubstitutes"); ok {
		if err := json.Unmarshal([]byte(value), &parsed.Documentation.Substitutes); err != nil {
			return parsed, true, fmt.Errorf("parse documentation substitutions: %w", err)
		}
	}
	if value, ok := structTag.Lookup("docURLs"); ok {
		if err := json.Unmarshal([]byte(value), &parsed.Documentation.DocURLs); err != nil {
			return parsed, true, fmt.Errorf("parse documentation URLs: %w", err)
		}
	}
	if value, exists := structTag.Lookup(MCPTag); exists {
		if err := json.Unmarshal([]byte(value), &parsed.MCP); err != nil {
			return parsed, true, fmt.Errorf("parse MCP route tag: %w", err)
		}
	}
	settings, settingsErr := ParseSettings(structTag)
	parsed.Settings = settings
	if parseErr != nil {
		return parsed, true, parseErr
	}
	return parsed, true, settingsErr
}

func ParseComponentValue(value string) (Component, error) {
	var result Component
	name, values := tagly.Values(value).Name()
	result.Name = strings.TrimSpace(name)
	err := values.MatchPairs(func(key, value string) error {
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "name":
			result.Name = value
		case "path":
			result.Path = value
		case "method":
			result.Method = value
		case "connector":
			result.Connector = value
		case "marshaller":
			result.Marshaller = value
		case "handler":
			result.Handler = value
		case "input":
			result.Input = value
		case "output":
			result.Output = value
		case "view":
			result.View = value
		case "source":
			result.Source = value
		case "report":
			parsed, err := parseBool("report", value)
			result.Report = parsed
			return err
		case "reportmcptool":
			parsed, err := parseBool("reportMCPTool", value)
			result.ReportMCPTool = &parsed
			return err
		case "reportcompose", "reportcomposemcptool", "reportcomposemaxcubes", "reportcomposemaxlimit", "reportcomposetimeoutms":
			key = strings.ToLower(key)
			if result.ReportCompose == nil {
				result.ReportCompose = &spec.CubeComposeSettings{}
			}
			if key == "reportcompose" || key == "reportcomposemcptool" {
				parsed, err := parseBool(key, value)
				if err != nil {
					return err
				}
				if key == "reportcompose" {
					result.ReportCompose.Enabled = parsed
				} else {
					result.ReportCompose.MCPTool = &parsed
				}
			} else {
				parsed, err := strconv.Atoi(value)
				if err != nil || parsed < 0 {
					return fmt.Errorf("%s requires a non-negative integer", key)
				}
				switch key {
				case "reportcomposemaxcubes":
					result.ReportCompose.MaxCubes = parsed
				case "reportcomposemaxlimit":
					result.ReportCompose.MaxLimit = parsed
				case "reportcomposetimeoutms":
					result.ReportCompose.TimeoutMs = parsed
				}
			}
		case "reportlinkedinputtype":
			result.ReportLinkedInputType = value
		case "reportdimensions":
			result.ReportDimensions = value
		case "reportmeasures":
			result.ReportMeasures = value
		case "reportfilters":
			result.ReportFilters = value
		case "reportorderby":
			result.ReportOrderBy = value
		case "reportlimit":
			result.ReportLimit = value
		case "reportoffset":
			result.ReportOffset = value
		default:
			return fmt.Errorf("unsupported component tag key %q", key)
		}
		return nil
	})
	return result, err
}

func validateComponentValues(component Component) error {
	if strings.ContainsAny(component.Name, ",=\"`") {
		return fmt.Errorf("component tag name contains an unsupported delimiter")
	}
	values := map[string]string{
		"path": component.Path, "method": component.Method,
		"connector": component.Connector, "marshaller": component.Marshaller,
		"handler": component.Handler, "input": component.Input, "output": component.Output,
		"view": component.View, "source": component.Source,
		"reportLinkedInputType": component.ReportLinkedInputType, "reportDimensions": component.ReportDimensions,
		"reportMeasures": component.ReportMeasures, "reportFilters": component.ReportFilters,
		"reportOrderBy": component.ReportOrderBy, "reportLimit": component.ReportLimit,
		"reportOffset": component.ReportOffset,
	}
	for name, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && strings.ContainsAny(value, ",\"`") {
			return fmt.Errorf("component tag %s contains an unsupported delimiter", name)
		}
	}
	return nil
}

func appendNonEmpty(builder *strings.Builder, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	builder.WriteString(",")
	builder.WriteString(key)
	builder.WriteString("=")
	builder.WriteString(value)
}
