package report

import (
	"fmt"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

const (
	defaultDimensions = "Dimensions"
	defaultMeasures   = "Measures"
	defaultFilters    = "Filters"
	defaultOrder      = "OrderBy"
	defaultLimit      = "Limit"
	defaultOffset     = "Offset"
)

type metadata struct {
	settings    spec.ReportSettings
	inputLayout spec.ReportInputLayout
	dimensions  []field
	measures    []field
	filters     []filterField
	holders     map[string][]string
}

type field struct {
	name        string
	publicName  string
	sqlName     string
	fieldName   string
	description string
}

type filterField struct {
	field
	contract registry.InputField
}

func compileMetadata(component *spec.Component, contract *registry.RouteInputContract) (*metadata, error) {
	if component == nil || component.RootView == nil {
		return nil, fmt.Errorf("report source component and root view are required")
	}
	if contract == nil {
		return nil, fmt.Errorf("report source route input contract is required")
	}
	var authored *spec.ReportSettings
	if component.Settings != nil {
		authored = component.Settings.Report
	}
	settings, inputLayout := normalizeSettings(authored)
	result := &metadata{settings: settings, inputLayout: inputLayout, holders: map[string][]string{}}
	projection, err := (bootstrap.ViewProjection{View: component.RootView}).Columns()
	if err != nil {
		return nil, fmt.Errorf("cube source projection: %w", err)
	}
	fieldNames := map[string]string{}
	for _, projected := range projection {
		column := projected.Column
		item := field{
			name: projected.Selector, publicName: strings.TrimSpace(column.Name), sqlName: projected.Name,
			fieldName: typecatalog.ExportedFieldName(column.Name), description: strings.TrimSpace(column.Source),
		}
		if item.fieldName == "" {
			return nil, fmt.Errorf("report column %q has no exported field identity", column.Name)
		}
		if previous := fieldNames[item.fieldName]; previous != "" {
			return nil, fmt.Errorf("report columns %q and %q map to field %s", previous, item.name, item.fieldName)
		}
		fieldNames[item.fieldName] = item.name
		if column.Groupable != nil && *column.Groupable {
			result.dimensions = append(result.dimensions, item)
		} else {
			result.measures = append(result.measures, item)
		}
	}
	if len(result.dimensions) == 0 && len(result.measures) == 0 {
		return nil, fmt.Errorf("report source has no selectable dimensions or measures")
	}
	for _, inputField := range contract.Fields() {
		binding := inputField.Binding()
		param, ok := binding.Extension.(*spec.Parameter)
		if !ok || param == nil || len(param.Predicates) == 0 || param.QuerySelector != nil {
			continue
		}
		name := strings.TrimSpace(param.Name)
		fieldName := typecatalog.ExportedFieldName(name)
		if name == "" || fieldName == "" {
			return nil, fmt.Errorf("report filter has no canonical field identity")
		}
		for _, existing := range result.filters {
			if existing.fieldName == fieldName {
				return nil, fmt.Errorf("report filters %q and %q map to field %s", existing.name, name, fieldName)
			}
		}
		result.filters = append(result.filters, filterField{
			field:    field{name: name, fieldName: fieldName, description: param.Description},
			contract: inputField,
		})
	}
	result.compileRelationHolders(component.RootView)
	return result, nil
}

func (m *metadata) compileRelationHolders(view *spec.View) {
	for _, relation := range view.Relations {
		if relation == nil || strings.TrimSpace(relation.Holder) == "" {
			continue
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			for _, dimension := range m.dimensions {
				column := findColumn(view.Columns, dimension.publicName)
				if column == nil || !columnMatchesLink(column, link) {
					continue
				}
				m.holders[dimension.name] = appendUnique(m.holders[dimension.name], relation.Holder)
			}
		}
	}
}

func normalizeSettings(source *spec.ReportSettings) (spec.ReportSettings, spec.ReportInputLayout) {
	result := spec.ReportSettings{}
	layout := spec.ReportInputLayout{}
	if source != nil {
		result = *source
		if source.InputLayout != nil {
			layout = *source.InputLayout
			result.InputLayout = &layout
		}
		if source.MCPTool != nil {
			enabled := *source.MCPTool
			result.MCPTool = &enabled
		}
	}
	result.LinkedInputType = strings.TrimSpace(result.LinkedInputType)
	layout.Dimensions = defaultName(layout.Dimensions, defaultDimensions)
	layout.Measures = defaultName(layout.Measures, defaultMeasures)
	layout.Filters = defaultName(layout.Filters, defaultFilters)
	layout.OrderBy = defaultName(layout.OrderBy, defaultOrder)
	layout.Limit = defaultName(layout.Limit, defaultLimit)
	layout.Offset = defaultName(layout.Offset, defaultOffset)
	return result, layout
}

func defaultName(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

func findColumn(columns []*spec.Column, name string) *spec.Column {
	for _, column := range columns {
		if column != nil && strings.EqualFold(strings.TrimSpace(column.Name), strings.TrimSpace(name)) {
			return column
		}
	}
	return nil
}

func columnMatchesLink(column *spec.Column, link *spec.RelationLink) bool {
	for _, candidate := range []string{column.Name, column.Source} {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(link.ParentColumn)) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
