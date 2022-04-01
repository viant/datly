package logger

import (
	"os"
	"time"
)

type Logger interface {
	ColumnsDetection(sql, source string)
	ObjectReconciling(dst, item, parent interface{}, index int)
	ReadingData(duration time.Duration, sql string, read int, params []interface{}, err error)
	ReadTime(duration time.Duration, err error)
}

func NewLogger() Logger {
	if os.Getenv("DATLY_DEBUG") == "" {
		return &nopLogger{}
	}
	return &defaultLogger{}
}
