package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

func generationCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("gen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", ".", "project root; DQL/package metadata controls artifact destinations")
	operation := flags.String("op", "", "operation: get, patch, post or put")
	language := flags.String("lang", "go", "handler language: go or velty")
	var schema schemaOptions
	schema.flags(flags)
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 || (*operation != "get" && *operation != "patch" && *operation != "post" && *operation != "put") || (*language != "go" && *language != "velty") {
		fmt.Fprintln(stderr, "gen requires -op get|patch|post|put, -lang go|velty and one module-qualified source package")
		return 2
	}
	if err := schema.validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	discovery := &transcribe.Discovery{BaseDir: *directory, Include: flags.Args()}
	if schema.enabled {
		discovery.Connector = schema.connector
		discovery.ColumnRefiner = column.New(&schema)
		defer schema.close()
	}
	project, err := discovery.Compile(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if len(project.Components) != 1 {
		fmt.Fprintf(stderr, "gen requires exactly one component in the selected source package; found %d\n", len(project.Components))
		return 1
	}
	generated, err := (transcribe.Generator{Operation: *operation, Language: transcribe.HandlerTarget(*language)}).Generate(ctx, transcribe.GenerationRequest{Compiled: project.Components[0], Destination: *directory})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "Generated %s %s: %d artifacts\n", *language, *operation, len(generated.Result.Files))
	return 0
}
