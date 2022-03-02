package shared

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

func MatchField(rType reflect.Type, name string, sourceCase format.Case) *xunsafe.Field {
	field := xunsafe.FieldByName(rType, sourceCase.Format(name, format.CaseUpperCamel))
	if field != nil {
		return field
	}
	name = strings.ToLower(name)
	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag.Get(option.TagSqlx))
		if tag.Column == "" {
			if name == strings.ToLower(sField.Name) {
				return xunsafe.NewField(sField)
			}
		}
		if tag.Transient {
			continue
		}

		if name == strings.ToLower(tag.Column) {
			return xunsafe.NewField(sField)
		}
	}
	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag.Get(option.TagSqlx))
		if tag.Column == "" || tag.Transient {
			continue
		}
		if name == strings.ToLower(tag.Column) {
			return xunsafe.NewField(sField)
		}
	}

	for i := 0; i < rType.NumField(); i++ {
		sField := rType.Field(i)
		tag := io.ParseTag(sField.Tag.Get(option.TagSqlx))
		if tag.Column != "" {
			continue
		}
		nameToLower := strings.ToLower(sField.Name)
		if name == nameToLower {
			return xunsafe.NewField(sField)
		}
		if strings.ReplaceAll(name, "_", "") == strings.ReplaceAll(nameToLower, "_", "") {
			return xunsafe.NewField(sField)
		}
	}

	fmt.Printf("not found field for name:%v, %v \n", name, rType.String())
	return nil
}
