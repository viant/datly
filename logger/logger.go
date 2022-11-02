package logger

import (
	"time"
)

type ColumnsDetection func(sql, source string)
type Log func(message string, args ...interface{})
type ObjectReconciling func(dst, item, parent interface{}, index int)
type ReadingData func(duration time.Duration, sql string, read int, params []interface{}, err error)
type ReadTime func(viewName string, start *time.Time, end *time.Time, err error)

type Logger interface {
	ColumnsDetection() ColumnsDetection
	ObjectReconciling() ObjectReconciling
	ReadingData() ReadingData
	ViewReadTime() ReadTime
	OverallReadTime() ReadTime
	Log() Log
}
