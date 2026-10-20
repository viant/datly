package dml

import (
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"reflect"
)

type batchQueueOptions struct {
	size     int
	affected *int64
	suffix   string
	ignore   bool
}

// InsertWithBatchSize queues a source slice with SQLX's explicit batch option.
// The existing owner controls execution and completion; results appear at drain.
func (d *Data) InsertWithBatchSize(table string, rows any, size int, affected *int64) error {
	return d.appendBatch(table, rows, size, affected, dataOpInsert, "")
}

// DeleteWithBatchSize retains SQLX's composite-key batch deletion mechanism.
func (d *Data) DeleteWithBatchSize(table string, rows any, size int, affected *int64) error {
	return d.appendBatch(table, rows, size, affected, dataOpDelete, "")
}

func (d *Data) appendBatch(table string, rows any, size int, affected *int64, kind dataOperationKind, suffix string, ignore ...bool) error {
	if size <= 0 || affected == nil {
		return fmt.Errorf("batch size must be positive and affected-row destination nonnil")
	}
	value := reflect.ValueOf(rows)
	// Bound the SQLX buffer to supplied rows, retaining the declared chunk limit.
	if value.IsValid() && value.Kind() == reflect.Slice && value.Len() > 0 && size > value.Len() {
		size = value.Len()
	}
	return d.appendWithQueueContract(table, rows, kind, rhandler.SourceSlice, batchQueueOptions{size: size, affected: affected, suffix: suffix, ignore: len(ignore) > 0 && ignore[0]})
}

// InsertWithBatchSizeAndOnDuplicateKeySQL queues one source slice with an immutable SQLX constructor suffix.
func (d *Data) InsertWithBatchSizeAndOnDuplicateKeySQL(table string, rows any, size int, affected *int64, suffix string) error {
	return d.appendBatch(table, rows, size, affected, dataOpInsert, suffix)
}

// InsertIgnoreWithBatchSize queues one source slice using MySQL INSERT IGNORE.
// Execution and completion remain with the existing transaction owner.
func (d *Data) InsertIgnoreWithBatchSize(table string, rows any, size int, affected *int64) error {
	return d.appendBatch(table, rows, size, affected, dataOpInsert, "", true)
}
