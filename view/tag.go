package view

import (
	"github.com/viant/datly/view/state"
	vtags "github.com/viant/datly/view/tags"
	"reflect"
	"strings"

	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
)

func generateFieldTag(column *Column, viewCaseFormat text.CaseFormat, doc state.Documentation, table string) string {
	result := tags.NewTags(strings.TrimSpace(column.Tag))
	columnName := column.Name
	sqlxTagValue := columnName
	if column.Codec != nil && column.DataType != "" {
		sqlxTagValue += ",type=" + column.DataType
	}
	result.SetIfNotFound("sqlx", sqlxTagValue)
	//I think we do not need it
	result.SetIfNotFound("velty", generateVelyTagValue(columnName, viewCaseFormat))
	var aTag *tags.Tag
	if column.FormatTag != nil {
		aTag = tags.NewTag("format", column.FormatTag)
		result.SetTag(aTag)
	}

	if aTag := column.Tag; aTag != "" {
		rTag := reflect.StructTag(aTag)
		if src, ok := rTag.Lookup("source"); ok {
			result.Set("source", src)
		}
	}

	if column.Codec != nil {
		args := strings.Builder{}
		for _, arg := range column.Codec.Args {
			args.WriteByte(',')
			args.WriteString(arg)
		}
		result.Set("codec", column.Codec.Name+args.String())
	}

	if aTag != nil {
		result.SetTag(aTag)
	}
	if doc != nil {
		description, ok := doc.ColumnDescription(table, columnName)
		if ok {
			result.Set(vtags.DescriptionTag, description)
		}
		example, ok := doc.ColumnExample(table, columnName)
		if ok {
			result.Set(vtags.ExampleTag, example)
		}
	}

	if column.Codec != nil {
		result.SetTag(aTag)
	}

	return result.Stringify()
}

func generateVelyTagValue(columnName string, viewCaseFormat text.CaseFormat) string {
	names := columnName
	if aFieldName := state.StructFieldName(viewCaseFormat, columnName); aFieldName != names {
		names = names + "|" + aFieldName
	}
	return `names=` + names
}
