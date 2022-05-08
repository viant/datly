package main

import (
	"github.com/viant/datly/gateway/runtime/standalone"
	"os"
)

//Version app version passed with ldflags i.e -ldflags="-X 'main.Version=v1.0.0'"
var Version = "development"

func main() {
	standalone.RunApp(Version, os.Args[1:])
}
