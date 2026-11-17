package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/viant/datly/transcribe/presencegen"
)

func main() {
	var cfg presencegen.Config
	var recursive bool
	flag.StringVar(&cfg.SourceDir, "source", ".", "directory containing the generated entity package")
	flag.StringVar(&cfg.Entry, "entry", "", "root entity type to traverse")
	flag.StringVar(&cfg.Output, "output", "", "output file (defaults to <entry>_presence.go in source)")
	flag.BoolVar(&recursive, "recursive", false, "discover and generate every patch component below source")
	flag.Parse()

	if recursive {
		if cfg.Entry != "" || cfg.Output != "" {
			fmt.Fprintln(os.Stderr, "presencegen: -entry and -output cannot be used with -recursive")
			os.Exit(2)
		}
		if err := presencegen.RunRecursive(cfg.SourceDir, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "presencegen: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if cfg.Entry == "" {
		fmt.Fprintln(os.Stderr, "presencegen: -entry is required unless -recursive is used")
		os.Exit(2)
	}
	if err := presencegen.Run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "presencegen: %v\n", err)
		os.Exit(1)
	}
}
