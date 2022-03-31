package logger

import (
	"fmt"
	"time"
)

type defaultLogger struct {
}

func (d *defaultLogger) ColumnsDetection(sql, source string) {
	fmt.Printf("[LOGGER] table columns SQL: %v, source: %v \n", sql, source)
}

func (d *defaultLogger) ObjectReconciling(dst, item, parent interface{}, index int) {
	fmt.Printf("[LOGGER] reconciling src:(%T):%+v with dest: (%T):%+v  pos:%v, item:(%T):%+v \n", item, item, dst, dst, index, parent, parent)
}

func (d *defaultLogger) ReadingData(duration time.Duration, sql string, read int, params []interface{}, err error) {
	fmt.Printf("[LOGGER] reading data took %v, SQL: %v, params: %v, read: %v, err: %v \n", duration, sql, params, read, err)
}
