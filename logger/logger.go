package logger

import (
	"os"
	"time"
)

type Adapter struct {
	logger Logger
	wasSet bool
}

func (l *Adapter) ColumnsDetection(sql, source string) {
	if l.logger == nil {
		return
	}

	l.logger.ColumnsDetection(sql, source)
}

func (l *Adapter) ObjectReconciling(dst, item, parent interface{}, index int) {
	if l.logger == nil {
		return
	}

	l.logger.ObjectReconciling(dst, item, parent, index)
}

func (l *Adapter) ReadingData(duration time.Duration, sql string, read int, params []interface{}, err error) {
	if l.logger == nil {
		return
	}

	l.logger.ReadingData(duration, sql, read, params, err)
}

func (l *Adapter) ReadTime(duration time.Duration, err error) {
	if l.logger == nil {
		return
	}

	l.logger.ReadTime(duration, err)
}

func LoggerOf(logger Logger) *Adapter {
	return &Adapter{
		logger: logger,
	}
}

type Logger interface {
	ColumnsDetection(sql, source string)
	ObjectReconciling(dst, item, parent interface{}, index int)
	ReadingData(duration time.Duration, sql string, read int, params []interface{}, err error)
	ReadTime(duration time.Duration, err error)
}

func New() *Adapter {
	if os.Getenv("DATLY_DEBUG") == "" {
		return LoggerOf(nil)
	}
	return LoggerOf(&defaultLogger{})
}
