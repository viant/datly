package shared

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

func MatchField(rType reflect.Type, name string, sourceCase text.CaseFormat) *xunsafe.Field {
	rType = Elem(rType)

	upperCamelName := sourceCase.Format(name, text.CaseFormatUpperCamel)
	field := xunsafe.FieldByName(rType, upperCamelName)
	if field != nil {
		return field
	}
	name = strings.ToLower(name)
	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag)
		if tag.Column == "" {
			if name == strings.ToLower(sField.Name) {
				return xunsafe.NewField(sField)
			}
		}
		if strings.ToLower(sField.Name) == name {
			return xunsafe.NewField(sField)
		}
		if tag.Transient {
			continue
		}

		if doesTagMatch(tag, name) {
			return xunsafe.NewField(sField)
		}
	}
	//TODO are these needed?
	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag)
		if tag.Column == "" || tag.Transient {
			continue
		}
		if doesTagMatch(tag, name) {
			return xunsafe.NewField(sField)
		}
	}

	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag)
		if tag.Column != "" {
			continue
		}
		nameToLower := strings.ToLower(sField.Name)
		if name == nameToLower {
			return xunsafe.NewField(sField)
		}
		if doesTagMatch(tag, sField.Name) {
			return xunsafe.NewField(sField)
		}
	}

	return nil
}

func doesTagMatch(tag *io.Tag, columnName string) bool {
	columnName = strings.ToLower(columnName)
	columnName = strings.ReplaceAll(columnName, "_", "")

	tagName := strings.ToLower(tag.Column)
	tagName = strings.ReplaceAll(tagName, "_", "")

	return columnName == tagName
}
