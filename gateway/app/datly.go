package main

import (
	"github.com/viant/datly/gateway/runtime/standalone"
	"os"
)

var Version string

func main() {
	standalone.RunApp(Version, os.Args[1:])

}
