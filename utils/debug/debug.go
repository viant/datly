package debug

import "os"

var Enabled = os.Getenv("DATLY_DEBUG") != ""

func SetEnabled(value bool) {
	Enabled = value
}
