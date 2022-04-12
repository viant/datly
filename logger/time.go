package logger

import (
	"time"
)

type TimeLogger struct {
	view          time.Duration
	global        time.Duration
	defaultLogger defaultLogger
}

func (t *TimeLogger) OverallReadTime() ReadTime {
	return func(viewName string, start *time.Time, end *time.Time, err error) {
		if end.Sub(*start) < t.global {
			return
		}

		t.defaultLogger.logOverallReadTime(viewName, start, end, err)
	}
}

func NewTimeLogger(view, global time.Duration) *TimeLogger {
	return &TimeLogger{
		view:   view,
		global: global,
	}
}

func (t *TimeLogger) ColumnsDetection() ColumnsDetection {
	return nil
}

func (t *TimeLogger) ObjectReconciling() ObjectReconciling {
	return nil
}

func (t *TimeLogger) ReadingData() ReadingData {
	return nil
}

func (t *TimeLogger) ViewReadTime() ReadTime {
	return func(viewName string, start *time.Time, end *time.Time, err error) {
		if end.Sub(*start) < t.view {
			return
		}

		t.defaultLogger.logReadTime(viewName, start, end, err)
	}
}
