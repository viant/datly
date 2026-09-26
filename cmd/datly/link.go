package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/viant/datly/project/build"
)

func linkCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "sync" {
		fmt.Fprintln(stderr, "usage: datly link sync [-dir project] [-tags tags] [Go package patterns]")
		return 2
	}
	flags := flag.NewFlagSet("link sync", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", ".", "project module root")
	tags := flags.String("tags", "", "Go build tags")
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	result, err := (build.Service{}).SyncLinks(ctx, build.LinkRequest{Dir: *dir, Tags: *tags, Packages: flags.Args()})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
