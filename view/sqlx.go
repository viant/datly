package view

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

type SqlxNamer struct {
}

func (s *SqlxNamer) Names(rField reflect.StructField) []string {
	sqlxTag := io.ParseTag(rField.Tag.Get(option.TagSqlx))
	if sqlxTag.Column == "" {
		return []string{rField.Name}
	}

	return strings.Split(sqlxTag.Column, "|")
}
