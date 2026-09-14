package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/viant/datly/project/build"
	"io"
	"strings"
)

func projectCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", ".", "project module root")
	var init build.InitRequest
	var request build.Request
	if args[0] == "init" {
		flags.StringVar(&init.Module, "module", "", "new Go module path")
		init.Pins = map[string]string{}
		init.Local = map[string]string{}
		flags.Func("pin", "exact module@version (repeatable)", func(s string) error {
			k, v, ok := strings.Cut(s, "@")
			if !ok {
				return fmt.Errorf("expected module@version")
			}
			init.Pins[k] = v
			return nil
		})
		flags.Func("local", "explicit development module=directory (repeatable)", func(s string) error {
			k, v, ok := strings.Cut(s, "=")
			if !ok {
				return fmt.Errorf("expected module=directory")
			}
			init.Local[k] = v
			return nil
		})
	} else {
		flags.StringVar(&request.Output, "o", "", "output binary")
		flags.StringVar(&request.Tags, "tags", "", "Go build tags")
	}
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	var err error
	if args[0] == "init" {
		if flags.NArg() != 0 {
			return 2
		}
		init.Dir = *dir
		err = (build.Service{}).Init(ctx, init)
	} else {
		request.Dir = *dir
		request.Packages = flags.Args()
		var result *build.Result
		result, err = (build.Service{}).Build(ctx, request)
		if err == nil {
			err = json.NewEncoder(stdout).Encode(result)
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
