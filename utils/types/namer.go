package types

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/velty"
	"reflect"
	"strings"
)

type Namer interface {
	Names(rField reflect.StructField) []string
}

type (
	SqlxNamer struct {
	}
	VeltyNamer struct {
	}
)

func (s *SqlxNamer) Names(rField reflect.StructField) []string {
	sqlxTag := io.ParseTag(rField.Tag.Get(option.TagSqlx))
	if sqlxTag.Column == "" {
		return []string{rField.Name}
	}

	return strings.Split(sqlxTag.Column, "|")
}

func (v *VeltyNamer) Names(rField reflect.StructField) []string {
	veltyTag := velty.Parse(rField.Tag.Get("velty"))
	if len(veltyTag.Names) == 0 {
		return []string{rField.Name}
	}

	return veltyTag.Names
}
