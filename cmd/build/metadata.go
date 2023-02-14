package build

import (
	"runtime"
	"time"
)

var (
	GoVersion = runtime.Version()
	BuildTime time.Time
)
