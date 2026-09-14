package compiler

import (
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	sqlxio "github.com/viant/sqlx/io"
)

// columnsFromType resolves immutable column metadata from a reader row type.
// Type and tag interpretation belongs to reader compilation, not data metadata.
func columnsFromType(rowType reflect.Type) ([]*data.Column, error) {
	for rowType != nil && rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType == nil || rowType.Kind() != reflect.Struct {
		return nil, nil
	}
	var result []*data.Column
	for _, field := range reflect.VisibleFields(rowType) {
		if !field.IsExported() {
			continue
		}
		metadata, err := dtag.ParseField(field)
		if err != nil {
			return nil, err
		}
		if len(metadata.Relation) > 0 || metadata.Self != nil {
			continue
		}
		sqlxTag := sqlxio.ParseTag(field.Tag)
		if sqlxTag != nil && sqlxTag.Transient {
			continue
		}
		fieldType := field.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if field.Anonymous && fieldType.Kind() == reflect.Struct && (sqlxTag == nil || sqlxTag.Ns == "") {
			continue
		}
		columnName := metadata.Source
		if sqlxTag != nil && sqlxTag.Name() != "" {
			columnName = sqlxTag.Name()
		}
		if columnName == "" {
			columnName = field.Name
		}
		column := &data.Column{
			Name:      field.Name,
			Column:    columnName,
			Groupable: metadata.Groupable,
			Tag:       string(field.Tag),
		}
		nullable := field.Type.Kind() == reflect.Ptr
		column.ConfigureNullability(nullable, field.Type.Kind().String())
		if sqlxTag != nil {
			column.DataType = strings.TrimSpace(sqlxTag.DataType)
		}
		if metadata.Codec != nil {
			column.Codec = &spec.Codec{Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Arguments...)}
		}
		result = append(result, column)
	}
	return result, nil
}

func reconcileColumns(typed, canonical []*data.Column) []*data.Column {
	if len(typed) == 0 || len(canonical) == 0 {
		return typed
	}
	for _, column := range typed {
		metadata := matchingColumn(canonical, column)
		if metadata == nil {
			continue
		}
		column.Groupable = column.Groupable || metadata.Groupable
		if metadata.Nullable {
			// Typed nullable columns are pointer destinations. Canonical scalar
			// fallbacks must not erase the NULL those destinations can represent.
			if !column.Nullable && column.NullFallback == "" {
				column.NullFallback = metadata.NullFallback
			}
			column.Nullable = true
		}
		if column.Expression == "" {
			column.Expression = metadata.Expression
		}
		if column.DataType == "" {
			column.DataType = metadata.DataType
		}
		if column.Codec == nil && metadata.Codec != nil {
			column.Codec = &spec.Codec{Body: metadata.Codec.Body, Args: append([]string(nil), metadata.Codec.Args...), OutputType: metadata.Codec.OutputType}
		}
	}
	return typed
}

func matchingColumn(columns []*data.Column, candidate *data.Column) *data.Column {
	if candidate == nil {
		return nil
	}
	candidateNames := map[string]bool{}
	for _, name := range []string{candidate.Name, candidate.Column} {
		if name = strings.ToLower(strings.TrimSpace(name)); name != "" {
			candidateNames[name] = true
		}
	}
	for _, column := range columns {
		if column == nil {
			continue
		}
		for _, name := range []string{column.Name, column.Column} {
			if candidateNames[strings.ToLower(strings.TrimSpace(name))] && strings.TrimSpace(name) != "" {
				return column
			}
		}
	}
	return nil
}
