package cmd

import (
	"context"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/viant/datly/cmd/command"
	soptions "github.com/viant/datly/cmd/options"
)

func New(version string, args soptions.Arguments) error {
	options, err := buildOptions(args)
	if err != nil {
		return err
	}
	if options.Version {
		fmt.Printf("Datly: version: %v\n", version)
		return nil
	}
	if err := options.Init(context.Background()); err != nil {
		return err
	}
	cmd := command.New()
	return cmd.Exec(context.Background(), options)

}

func buildOptions(args soptions.Arguments) (*soptions.Options, error) {
	var opts *soptions.Options
	if (args.SubMode() || args.IsHelp()) && !args.IsLegacy() {
		opts = soptions.NewOptions(args)
		if _, err := flags.ParseArgs(opts, args); err != nil {
			return nil, err
		}
		if args.IsHelp() {
			return nil, nil
		}

	} else {
		options := &Options{}
		if _, err := flags.ParseArgs(options, args); err != nil {
			return nil, err
		}
		opts = options.BuildOption()
	}
	return opts, nil
}
