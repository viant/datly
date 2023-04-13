package router

import (
	"github.com/viant/sqlx/io"
	"unsafe"
)

func init() {
	asyncRecordMatcher = io.NewMatcher("sqlx", func(column io.Column) func(pointer unsafe.Pointer) interface{} {
		return nil
	})
}
