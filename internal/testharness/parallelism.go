package testharness

import (
	"flag"
	"runtime"
	"strconv"
)

// CapFixtureParallelism limits -test.parallel for packages whose tests spawn
// `go test` builds of generated modules. Each spawned build already uses every
// core for compiling and linking, so fanning parallel tests out to NumCPU
// oversubscribes the machine and slows every fixture down. The cap keeps the
// linker saturated instead. An explicit -test.parallel on the command line is
// respected. Call it from TestMain before m.Run().
func CapFixtureParallelism() {
	if !flag.Parsed() {
		flag.Parse()
	}
	explicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.parallel" {
			explicit = true
		}
	})
	if explicit {
		return
	}
	limit := runtime.NumCPU() / 4
	if limit < 2 {
		limit = 2
	}
	_ = flag.Set("test.parallel", strconv.Itoa(limit))
}
