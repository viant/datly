package logger

import (
	"time"
)

type nopLogger struct{}

func (n *nopLogger) ColumnsDetection(_, _ string) {}

func (n *nopLogger) ObjectReconciling(_, _, _ interface{}, _ int) {}

func (n *nopLogger) ReadingData(_ time.Duration, _ string, _ int, _ []interface{}, _ error) {}
