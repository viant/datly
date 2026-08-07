package report

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view/state"
)

type Config struct {
	Enabled    bool
	MCPTool    *bool
	Input      string
	Dimensions string
	Measures   string
	Filters    string
	OrderBy    string
	Limit      string
	Offset     string
}

type Metadata struct {
	InputName     string
	BodyFieldName string
	DimensionsKey string
	MeasuresKey   string
	FiltersKey    string
	Dimensions    []*Field
	Measures      []*Field
	Filters       []*Filter
	OrderBy       string
	Limit         string
	Offset        string
}

type Field struct {
	Name        string
	FieldName   string
	Section     string
	Description string
}

type Filter struct {
	Name        string
	FieldName   string
	Section     string
	Description string
	Parameter   *state.Parameter
}

func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	ret := *c
	return &ret
}

func (c *Config) Normalize() *Config {
	if c == nil {
		return nil
	}
	ret := c.Clone()
	ret.Input = strings.TrimSpace(ret.Input)
	ret.Dimensions = defaultString(ret.Dimensions, "Dimensions")
	ret.Measures = defaultString(ret.Measures, "Measures")
	ret.Filters = defaultString(ret.Filters, "Filters")
	ret.OrderBy = defaultString(ret.OrderBy, "OrderBy")
	ret.Limit = defaultString(ret.Limit, "Limit")
	ret.Offset = defaultString(ret.Offset, "Offset")
	return ret
}

func (c *Config) MCPToolEnabled() bool {
	if c == nil || c.MCPTool == nil {
		return true
	}
	return *c.MCPTool
}

func (c *Config) InputTypeName(componentName, inputName, viewName string) string {
	if c != nil && strings.TrimSpace(c.Input) != "" {
		return strings.TrimSpace(c.Input)
	}
	switch {
	case strings.TrimSpace(inputName) != "":
		return state.SanitizeTypeName(strings.TrimSpace(inputName) + "ReportInput")
	case strings.TrimSpace(componentName) != "":
		return state.SanitizeTypeName(strings.TrimSpace(componentName) + "ReportInput")
	default:
		return state.SanitizeTypeName(strings.TrimSpace(viewName) + "ReportInput")
	}
}

func (m *Metadata) ValidateSelection() error {
	if m == nil {
		return fmt.Errorf("report metadata was empty")
	}
	if len(m.Dimensions) == 0 && len(m.Measures) == 0 {
		return fmt.Errorf("report metadata had no selectable dimensions or measures")
	}
	return nil
}

func (f *Filter) SchemaType() reflect.Type {
	if f == nil || f.Parameter == nil || f.Parameter.Schema == nil {
		return nil
	}
	return f.Parameter.OutputType()
}

func defaultString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
