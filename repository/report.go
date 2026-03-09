package repository

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/view/state"
)

type Report struct {
	Enabled    bool   `json:",omitempty" yaml:"Enabled,omitempty"`
	Input      string `json:",omitempty" yaml:"Input,omitempty"`
	Dimensions string `json:",omitempty" yaml:"Dimensions,omitempty"`
	Measures   string `json:",omitempty" yaml:"Measures,omitempty"`
	Filters    string `json:",omitempty" yaml:"Filters,omitempty"`
	OrderBy    string `json:",omitempty" yaml:"OrderBy,omitempty"`
	Limit      string `json:",omitempty" yaml:"Limit,omitempty"`
	Offset     string `json:",omitempty" yaml:"Offset,omitempty"`
}

type ReportMetadata struct {
	InputName     string
	BodyFieldName string
	DimensionsKey string
	MeasuresKey   string
	FiltersKey    string
	Dimensions    []*ReportField
	Measures      []*ReportField
	Filters       []*ReportFilter
	OrderBy       string
	Limit         string
	Offset        string
}

type ReportField struct {
	Name        string
	FieldName   string
	Section     string
	Description string
}

type ReportFilter struct {
	Name        string
	FieldName   string
	Section     string
	Description string
	Parameter   *state.Parameter
}

func (r *Report) Clone() *Report {
	if r == nil {
		return nil
	}
	ret := *r
	return &ret
}

func (r *Report) normalize() *Report {
	if r == nil {
		return nil
	}
	ret := r.Clone()
	ret.Input = strings.TrimSpace(ret.Input)
	ret.Dimensions = defaultString(ret.Dimensions, "Dimensions")
	ret.Measures = defaultString(ret.Measures, "Measures")
	ret.Filters = defaultString(ret.Filters, "Filters")
	ret.OrderBy = defaultString(ret.OrderBy, "OrderBy")
	ret.Limit = defaultString(ret.Limit, "Limit")
	ret.Offset = defaultString(ret.Offset, "Offset")
	return ret
}

func (r *Report) inputTypeName(componentName, inputName, viewName string) string {
	if r != nil && strings.TrimSpace(r.Input) != "" {
		return strings.TrimSpace(r.Input)
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

func (r *ReportMetadata) validateSelection() error {
	if r == nil {
		return fmt.Errorf("report metadata was empty")
	}
	if len(r.Dimensions) == 0 && len(r.Measures) == 0 {
		return fmt.Errorf("report metadata had no selectable dimensions or measures")
	}
	return nil
}

func (r *ReportFilter) schemaType() reflect.Type {
	if r == nil || r.Parameter == nil || r.Parameter.Schema == nil {
		return nil
	}
	return r.Parameter.OutputType()
}

func defaultString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
