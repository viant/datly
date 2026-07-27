package view

import (
	"fmt"
	"reflect"
	"strings"
)

func ProjectionColumnsForOutput(aView *View, output interface{}) ([]string, error) {
	if output == nil {
		return nil, nil
	}
	return ProjectionColumnsForType(aView, ProjectionOutputStructType(reflect.TypeOf(output)))
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
