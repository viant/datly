package view

import (
	"github.com/viant/datly/view/state"
	"github.com/viant/structology/format/text"
	"github.com/viant/structology/tags"
)

func generateFieldTag(column *Column, viewCaseFormat text.CaseFormat) string {
	result := tags.NewTags(column.Tag)
	columnName := column.Name
	//TODO possible add validate tag ?
	result.SetIfNotFound("sqlx", "name="+columnName)
	result.SetIfNotFound("velty", generateVelyTagValue(columnName, viewCaseFormat))
	if column.FormatTag != nil {
		aTag := tags.NewTag("format", column.FormatTag)
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
