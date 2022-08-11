package tests

import (
	"fmt"
	"os"
)

var loggingHeaderEnabled = os.Getenv("ENABLE_LOGGING") == "true"

func LogHeader(header string) {
	//if !loggingHeaderEnabled {
	//	return
	//}

	fmt.Println(header)
}
