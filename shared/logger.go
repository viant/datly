package shared

import (
	"fmt"
	"os"
)

func Log(message string, args ...interface{}) {
	if os.Getenv("DATLY_DEBUG") == "" {
		return
	}
	fmt.Printf(message, args...)
}
