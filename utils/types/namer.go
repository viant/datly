package types

import (
	"github.com/viant/sqlx/io"
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
	FieldNamer struct {
	}
)

func (s *FieldNamer) Names(rField reflect.StructField) []string {
	return []string{rField.Name}
}

func (s *SqlxNamer) Names(rField reflect.StructField) []string {
	sqlxTag := io.ParseTag(rField.Tag)
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
