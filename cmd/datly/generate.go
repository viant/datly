package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/viant/datly/constant"
	"io"

	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

func generationCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	name := args[0]
	operationValue := ""
	arguments := args[1:]
	if name == "transcribe" {
		if len(arguments) == 0 {
			fmt.Fprintln(stderr, "usage: datly transcribe get|patch|post|put [options] module/package")
			return 2
		}
		if arguments[0] != "-h" && arguments[0] != "--help" {
			operationValue, arguments = arguments[0], arguments[1:]
			if operationValue != "get" && operationValue != "patch" && operationValue != "post" && operationValue != "put" {
				fmt.Fprintln(stderr, "transcribe requires operation get|patch|post|put before options")
				return 2
			}
		}
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	if name == "transcribe" {
		flags.Usage = func() {
			fmt.Fprintln(stderr, "usage: datly transcribe get|patch|post|put [options] module/package")
			flags.PrintDefaults()
		}
	}
	constantURL := flags.String("const", "", "instance constants YAML/JSON file")
	directory := flags.String("dir", ".", "project root; DQL/package metadata controls artifact destinations")
	operation := &operationValue
	language := flags.String("lang", "go", "handler language: go or velty")
	var schema schemaOptions
	schema.flags(flags)
	if err := flags.Parse(arguments); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 || (*operation != "get" && *operation != "patch" && *operation != "post" && *operation != "put") || (*language != "go" && *language != "velty") {
		fmt.Fprintln(stderr, "transcribe requires get|patch|post|put, optional -lang go|velty and one module-qualified source package")
		return 2
	}
	if err := schema.validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	instance, err := (constant.Loader{}).Load(ctx, *constantURL)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	resolvedDirectory, err := instance.Path(*directory)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	schema.Const = instance
	discovery := &transcribe.Discovery{Const: instance, BaseDir: resolvedDirectory, Include: flags.Args()}
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
		fmt.Fprintf(stderr, "transcribe requires exactly one component in the selected source package; found %d\n", len(project.Components))
		return 1
	}
	generated, err := (transcribe.Generator{Operation: *operation, Language: transcribe.HandlerTarget(*language)}).Generate(ctx, transcribe.GenerationRequest{Compiled: project.Components[0], Destination: resolvedDirectory})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "Generated %s %s: %d artifacts\n", *language, *operation, len(generated.Result.Files))
	return 0
}
