// Package command provides executable standalone commands for stock and linked applications.
package command

import (
	"context"
	"flag"
	"fmt"
	"io"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
)

type Service struct {
	Workspace *xmodule.Workspace
	Registry  *x.Registry
	Version   string
}

func (s Service) Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "run" && args[0] != "start" {
		fmt.Fprintln(stderr, "usage: datly run|start -conf configuration-URL")
		return 2
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	var location, constantURL string
	flags.StringVar(&constantURL, "const", "", "instance constants YAML/JSON file (overrides ConstURL)")
	flags.StringVar(&location, "conf", "", "standalone JSON/YAML configuration URL")
	flags.StringVar(&location, "c", "", "standalone JSON/YAML configuration URL")
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if location == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "a configuration URL and no positional arguments are required")
		return 2
	}
	cfg, err := (config.Loader{ConstURL: constantURL}).Load(ctx, location)
	if err == nil {
		if s.Version != "" {
			cfg.Version = s.Version
		}
		access, resolveErr := cfg.ResolveConstants()
		err = resolveErr
		if err == nil {
			err = access.Validate()
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	server, err := standalone.New(ctx, standalone.Options{Config: cfg, Registry: s.Registry, Workspace: s.Workspace, Diagnostics: stderr})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err = server.Serve(ctx, stdout); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
