package db

import (
	"github.com/viant/sqlx/option"
	"reflect"
)

type SqlSource interface {
	CreateTable(recordType reflect.Type, tableName string, tagName option.Tag, autogeneratePk bool) (*Table, error)
	RecordType(recordType reflect.Type, tagName option.Tag) (reflect.Type, error)
}
