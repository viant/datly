// Package developer provides the shared stock and custom-linked authoring CLI.
package developer

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/viant/aerospike"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/constant"
	"io"

	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

type Service struct {
	// Runtime preserves the embedding application's injected runtime services,
	// providers, authenticator, workspace and exposure policy.
	Runtime command.Service
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return (Service{}).Run(ctx, args, stdout, stderr)
}

func (s Service) Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "transcribe" {
		return Transcribe(ctx, args, stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "init" || args[0] == "build") {
		return projectCommand(ctx, args, stdout, stderr)
	}
	if len(args) > 0 && args[0] == "link" {
		return linkCommand(ctx, args[1:], stdout, stderr)
	}
	if len(args) > 0 && (args[0] == "run" || args[0] == "start") {
		return s.Runtime.Run(ctx, args, stdout, stderr)
	}
	if len(args) == 0 || args[0] != "validate" {
		fmt.Fprintln(stderr, "usage: datly transcribe patch -dir project module/package; datly init|build [-dir project] [-link-package internal/dependencylink]; datly link sync [-dir project] [-link-package internal/dependencylink] [-tags tags]; datly run|start -conf configuration-URL; datly validate [-dir project-directory] [-format text|json] [-schema -connector name -driver driver -dsn connection] module/package [...]")
		return 2
	}
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	constantURL := flags.String("const", "", "instance constants YAML/JSON file")
	base := flags.String("dir", ".", "project directory")
	format := flags.String("format", "text", "output format: text or json")
	var schema schemaOptions
	schema.flags(flags)
	var modules, excludes []string
	flags.Func("module-dir", "additional local module directory (repeatable)", func(value string) error {
		modules = append(modules, value)
		return nil
	})
	flags.Func("exclude", "excluded component package pattern (repeatable; private type dependencies remain available)", func(value string) error {
		excludes = append(excludes, value)
		return nil
	})
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() == 0 || (*format != "text" && *format != "json") {
		fmt.Fprintln(stderr, "explicit module-qualified package patterns and text or json format are required")
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
	schema.Const = instance
	validator := &transcribe.Validator{Const: instance, BaseDir: *base, Include: flags.Args(), ModuleDirs: modules, Exclude: excludes}
	if schema.enabled {
		validator.Connector, validator.ColumnRefiner = schema.connector, column.New(&schema)
		defer schema.close()
	}
	report, err := validator.Validate(ctx)
	if *format == "json" {
		if encodeErr := json.NewEncoder(stdout).Encode(report); encodeErr != nil {
			fmt.Fprintln(stderr, encodeErr)
			return 1
		}
	} else {
		var output bytes.Buffer
		for _, diagnostic := range report.Diagnostics {
			if diagnostic.Path != "" {
				fmt.Fprintf(&output, "%s: ", diagnostic.Path)
			}
			if diagnostic.Span.Start.Line > 0 {
				fmt.Fprintln(&output, diagnostic.Error())
			} else {
				fmt.Fprintln(&output, diagnostic.Message)
			}
		}
		for _, stage := range report.Completed {
			fmt.Fprintf(&output, "Checked: %s\n", stage)
		}
		for _, inspection := range report.Schema {
			fmt.Fprintf(&output, "Inspected: %s view %s connector %s", inspection.Component, inspection.View, inspection.Connector)
			if inspection.Table != "" {
				fmt.Fprintf(&output, " table %s", inspection.Table)
			}
			fmt.Fprintln(&output)
		}
		for _, stage := range report.Skipped {
			fmt.Fprintf(&output, "Not checked: %s\n", stage)
		}
		label := "Static validation"
		if schema.enabled {
			label = "Schema-aware authoring validation"
		}
		fmt.Fprintf(&output, "%s passed: %t (%d components)\n", label, report.Valid, len(report.Components))
		if _, writeErr := io.Copy(stdout, &output); writeErr != nil {
			fmt.Fprintln(stderr, writeErr)
			return 1
		}
	}
	if err != nil {
		return 1
	}
	return 0
}
