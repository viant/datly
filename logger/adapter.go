package logger

import (
	"github.com/viant/datly/shared"
	"os"
	"time"
)

type Adapter struct {
	shared.Reference

	readTime          ReadTime
	readingData       ReadingData
	objectReconciling ObjectReconciling
	columnsDetection  ColumnsDetection

	wasSet bool
}

func (l *Adapter) ColumnsDetection(sql, source string) {
	if l.columnsDetection == nil {
		return
	}

	l.columnsDetection(sql, source)
}

func (l *Adapter) ObjectReconciling(dst, item, parent interface{}, index int) {
	if l.objectReconciling == nil {
		return
	}

	l.objectReconciling(dst, item, parent, index)
}

func (l *Adapter) ReadingData(duration time.Duration, sql string, read int, params []interface{}, err error) {
	if l.readingData == nil {
		return
	}

	l.readingData(duration, sql, read, params, err)
}

func (l *Adapter) ReadTime(viewName string, start, end *time.Time, err error) {
	if l.readTime == nil {
		return
	}

	l.readTime(viewName, start, end, err)
}

func NewLogger(logger Logger) *Adapter {
	if logger == nil {
		return &Adapter{}
	}

	return &Adapter{
		Reference:         shared.Reference{},
		readTime:          logger.ViewReadTime(),
		readingData:       logger.ReadingData(),
		objectReconciling: logger.ObjectReconciling(),
		columnsDetection:  logger.ColumnsDetection(),
	}
}

func Default() *Adapter {
	if os.Getenv("DATLY_DEBUG") == "" {
		return NewLogger(nil)
	}
	return NewLogger(&defaultLogger{})
}
