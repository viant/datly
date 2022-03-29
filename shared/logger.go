package shared

import (
	"fmt"
	"os"
)

var logFn func(format string, args []interface{})

func init() {
	if os.Getenv("DATLY_DEBUG") == "" {
		logFn = func(format string, args []interface{}) {}
	} else {
		logFn = func(format string, args []interface{}) {
			fmt.Printf("[Logger] "+format+"\n", args...)
		}
	}
}

func Log(message string, args ...interface{}) {
	logFn(message, args)
}
