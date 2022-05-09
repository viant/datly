package standalone

import (
	"context"
	"github.com/jessevdk/go-flags"
	"log"
)

func RunApp(version string, args []string) error {
	options := &Options{}
	_, err := flags.ParseArgs(options, args)
	if err != nil {
		log.Fatal(err)
	}

	if isHelpOption(args) {
		return nil
	}

	if options.Version {
		log.Printf("RuleIndexer: Version: %v\n", version)
		return nil
	}

	ctx := context.Background()
	config, err := NewConfigFromURL(ctx, options.ConfigURL)
	if err != nil {
		log.Fatal(err)
	}
	config.Version = version
	srv, err := New(config)
	if err != nil {
		log.Fatal(err)
	}
	return srv.ListenAndServe()
}

func isHelpOption(args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
