package build

import (
	"os/exec"
	"runtime"
	"time"
)

var (
	GoVersion string
	BuildTime time.Time
)

func init() {
	GoVersion = GolangVersion()
}

func GolangVersion() string {
	command := exec.Command("go", "version")
	output, err := command.CombinedOutput()
	if err != nil {
		return runtime.Version()
	}

	return string(output)
}
