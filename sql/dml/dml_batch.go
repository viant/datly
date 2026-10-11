package dml

import (
	"reflect"

	"github.com/viant/sqlx/metadata/info"
)

func supportsInsertBatching(dialect *info.Dialect) bool {
	return dialect != nil && dialect.Insert.MultiValues()
}

func batchInsertPayload(operations []*dataOperation) any {
	itemType := reflect.TypeOf(operations[0].data)
	payload := reflect.MakeSlice(reflect.SliceOf(itemType), 0, len(operations))
	for _, operation := range operations {
		payload = reflect.Append(payload, reflect.ValueOf(operation.data))
	}
	return payload.Interface()
}

func canBatchInsert(operations []*dataOperation) bool {
	if len(operations) < 2 {
		return false
	}
	for _, operation := range operations {
		// A collection is already a SQLX payload. Batching collections adds an
		// unsupported outer slice; preserve the existing per-payload execution.
		typ := reflect.TypeOf(operation.data)
		if typ == nil {
			return false
		}
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct {
			return false
		}
		if insertNeedsGeneratedIDBackfill(operation.data) {
			return false
		}
	}
	return true
}
