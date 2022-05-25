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

	if options.Version {
		log.Printf("RuleIndexer: Version: %v\n", version)
		return nil
	}

	configURL := options.ConfigURL
	srv, err := NewWithURL(configURL, version)
	if err != nil {
		log.Fatal(err)
	}
	return srv.ListenAndServe()
}

//NewWithURL create service with config URL
func NewWithURL(configURL, version string) (*Server, error) {
	ctx := context.Background()
	config, err := NewConfigFromURL(ctx, configURL)
	if err != nil {
		return nil, err
	}
	config.Version = version
	srv, err := New(config)
	if err != nil {
		return nil, err
	}
	return srv, nil
}
