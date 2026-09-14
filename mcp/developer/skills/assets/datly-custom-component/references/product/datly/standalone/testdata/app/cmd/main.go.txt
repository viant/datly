package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exports, err := records.Exports()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit((command.Service{Registry: exports, Version: "standalone-fixture"}).Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
