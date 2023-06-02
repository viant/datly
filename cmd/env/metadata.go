package env

import (
	"github.com/viant/pgo/build"
	"runtime"
	"strings"
	"time"
)

var (
	GoVersion string
	BuildTime time.Time
	BuildType BuildTypeKind
)

func init() {
	GoVersion = strings.Replace(runtime.Version(), "go", "", 1)
	aRuntime := build.Runtime{}
	aRuntime.Init()
}
