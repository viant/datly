package view

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlx/io/read/cache"
)

type ProjectionField struct {
	Name         string   `json:",omitempty" yaml:",omitempty"`
	FieldName    string   `json:",omitempty" yaml:",omitempty"`
	ColumnName   string   `json:",omitempty" yaml:",omitempty"`
	Source       string   `json:",omitempty" yaml:",omitempty"`
	DimensionKey string   `json:",omitempty" yaml:",omitempty"`
	MeasureKey   string   `json:",omitempty" yaml:",omitempty"`
	Lookup       []string `json:",omitempty" yaml:",omitempty"`
}

func ProjectionColumnsForOutput(aView *View, output interface{}) ([]string, error) {
	if output == nil {
		return nil, nil
	}
	return ProjectionColumnsForType(aView, ProjectionOutputStructType(reflect.TypeOf(output)))
}

func ProjectionColumnsForNames(aView *View, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	columns := make([]string, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("output projection for view %s contains empty field name", aView.Name)
		}
		column, ok := aView.ColumnByName(name)
		if !ok {
			return nil, fmt.Errorf("failed to map output field %s to view %s column", name, aView.Name)
		}
		if seen[column.Name] {
			continue
		}
		seen[column.Name] = true
		columns = append(columns, column.Name)
	}
	return columns, nil
}

func ProjectionFieldsForNames(aView *View, names []string) ([]ProjectionField, error) {
	if len(names) == 0 {
		return ProjectionFieldsForColumns(aView, aView.Columns), nil
	}
	fields := make([]ProjectionField, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("output projection for view %s contains empty field name", aView.Name)
		}
		column, ok := aView.ColumnByName(name)
		if !ok {
			return nil, fmt.Errorf("failed to map output field %s to view %s column", name, aView.Name)
		}
		if seen[column.Name] {
			continue
		}
		seen[column.Name] = true
		fields = append(fields, ProjectionFieldForViewColumn(aView, column))
	}
	return fields, nil
}

func ProjectionFieldsForColumns(aView *View, columns Columns) []ProjectionField {
	if len(columns) == 0 {
		return nil
	}
	fields := make([]ProjectionField, 0, len(columns))
	seen := map[string]bool{}
	for _, column := range columns {
		if column == nil || column.Name == "" || seen[column.Name] {
			continue
		}
		seen[column.Name] = true
		fields = append(fields, ProjectionFieldForViewColumn(aView, column))
	}
	return fields
}

func ProjectionFieldForColumn(column *Column) ProjectionField {
	if column == nil {
		return ProjectionField{}
	}
	fieldName := column.FieldName()
	if fieldName == "" {
		fieldName = column.Name
	}
	source := projectionFieldSource(column)
	return ProjectionField{
		Name:       column.Name,
		FieldName:  fieldName,
		ColumnName: column.Name,
		Source:     source,
		Lookup:     projectionFieldLookup(column.Name, fieldName, column.DatabaseColumn),
	}
}

func ProjectionFieldForViewColumn(aView *View, column *Column) ProjectionField {
	field := ProjectionFieldForColumn(column)
	if aView == nil || column == nil || !aView.Groupable {
		return field
	}
	if column.Groupable {
		field.MeasureKey = ""
		field.DimensionKey = strings.TrimSpace(column.Name)
		return field
	}
	field.DimensionKey = ""
	field.MeasureKey = strings.TrimSpace(column.Name)
	return field
}

func SQLXProjectionFields(fields []ProjectionField) []cache.ProjectionField {
	if fields == nil {
		return nil
	}
	result := make([]cache.ProjectionField, 0, len(fields))
	for _, field := range fields {
		result = append(result, cache.ProjectionField{
			Name:         field.Name,
			FieldName:    field.FieldName,
			ColumnName:   field.ColumnName,
			Source:       field.Source,
			DimensionKey: field.DimensionKey,
			MeasureKey:   field.MeasureKey,
			Lookup:       append([]string(nil), field.Lookup...),
		})
	}
	return result
}

func ProjectionColumnsForType(aView *View, rType reflect.Type) ([]string, error) {
	if aView == nil || rType == nil {
		return nil, nil
	}
	var result []string
	seen := map[string]bool{}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		if skipProjectionField(field) {
			continue
		}
		if field.Anonymous {
			if nested := ProjectionStructType(field.Type); nested != nil {
				columns, err := ProjectionColumnsForType(aView, nested)
				if err != nil {
					return nil, err
				}
				for _, column := range columns {
					if !seen[column] {
						result = append(result, column)
						seen[column] = true
					}
				}
				continue
			}
		}
		column, err := projectionColumnForField(aView, field)
		if err != nil {
			return nil, err
		}
		if !seen[column.Name] {
			result = append(result, column.Name)
			seen[column.Name] = true
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("output projection for view %s did not map any columns", aView.Name)
	}
	return result, nil
}

func ProjectionStructType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
		for rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	return rType
}

func ProjectionOutputStructType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Slice && rType.Kind() != reflect.Array {
		return nil
	}
	return ProjectionStructType(rType)
}

func projectionColumnForField(aView *View, field reflect.StructField) (*Column, error) {
	for _, candidate := range projectionFieldCandidates(field) {
		if column, ok := aView.ColumnByName(candidate); ok {
			return column, nil
		}
	}
	return nil, fmt.Errorf("failed to map output field %s to a column in view %s", field.Name, aView.Name)
}

func projectionFieldCandidates(field reflect.StructField) []string {
	var result []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || value == "-" {
			return
		}
		for _, item := range strings.Split(value, "|") {
			item = strings.TrimSpace(item)
			if item == "" || item == "-" {
				continue
			}
			result = append(result, item)
		}
	}
	add(tagName(field.Tag.Get("sqlx")))
	add(tagName(field.Tag.Get("source")))
	add(tagName(field.Tag.Get("json")))
	add(field.Name)
	return result
}

func skipProjectionField(field reflect.StructField) bool {
	return tagName(field.Tag.Get("json")) == "-" || tagName(field.Tag.Get("sqlx")) == "-"
}

func tagName(tag string) string {
	if index := strings.Index(tag, ","); index != -1 {
		tag = tag[:index]
	}
	return strings.TrimSpace(tag)
}

func projectionFieldSource(column *Column) string {
	if column == nil || column.Tag == "" {
		return ""
	}
	return reflect.StructTag(column.Tag).Get("source")
}

func projectionFieldLookup(values ...string) []string {
	seen := map[string]bool{}
	var result []string
	for _, value := range values {
		for _, candidate := range projectionFieldLookupCandidates(value) {
			if seen[candidate] {
				continue
			}
			seen[candidate] = true
			result = append(result, candidate)
		}
	}
	return result
}

func projectionFieldLookupCandidates(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	values := []string{value}
	if index := strings.LastIndex(value, "."); index != -1 && index+1 < len(value) {
		values = append(values, value[index+1:])
	}
	result := make([]string, 0, len(values)*3)
	for _, candidate := range values {
		result = append(result, candidate)
		normalized := normalizeProjectionFieldName(candidate)
		if normalized != "" && normalized != candidate {
			result = append(result, normalized)
		}
		lower := strings.ToLower(candidate)
		if lower != candidate && lower != normalized {
			result = append(result, lower)
		}
	}
	return result
}

func normalizeProjectionFieldName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, ".", "")
	return value
}
