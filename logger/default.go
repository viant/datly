package logger

import (
	"fmt"
	"time"
)

type defaultLogger struct {
}

func (d *defaultLogger) OverallReadTime() ReadTime {
	return d.logOverallReadTime
}

func (d *defaultLogger) ViewReadTime() ReadTime {
	return d.logReadTime
}

func (d *defaultLogger) logReadTime(viewName string, start *time.Time, end *time.Time, err error) {
	fmt.Printf("[LOGGER] Reading and reconciling data from View %v took: %v, err: %v\n", viewName, end.Sub(*start), err)
}

func (d *defaultLogger) ColumnsDetection() ColumnsDetection {
	return func(sql, source string) {
		fmt.Printf("[LOGGER] table columns SQL: %v, source: %v \n", sql, source)
	}
}

func (d *defaultLogger) ObjectReconciling() ObjectReconciling {
	return func(dst, item, parent interface{}, index int) {
		fmt.Printf("[LOGGER] reconciling src:(%T):%+v with dest: (%T):%+v  pos:%v, item:(%T):%+v \n", item, item, dst, dst, index, parent, parent)
	}
}

func (d *defaultLogger) ReadingData() ReadingData {
	return func(duration time.Duration, sql string, read int, params []interface{}, err error) {
		fmt.Printf("[LOGGER] reading data took %v, SQL: %v, params: %v, read: %v, err: %v \n", duration, sql, params, read, err)
	}
}

func (d *defaultLogger) logOverallReadTime(viewName string, start *time.Time, end *time.Time, err error) {
	fmt.Printf("[LOGGER] Overall reading data from main View %v took: %v, err: %v\n", viewName, end.Sub(*start), err)
}
