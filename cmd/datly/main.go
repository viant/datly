// Package main provides the stock Datly application-developer executable.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/viant/datly/cmd/developer"
	"github.com/viant/datly/transcribe"
	_ "github.com/viant/gsheets/metadata" // Register Google Sheets for the stock CLI.
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(developer.Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return developer.Run(ctx, args, stdout, stderr)
}
func generationCommand(ctx context.Context, args []string, stdout, stderr io.Writer, handlers ...*transcribe.HandlerBinding) int {
	return developer.Transcribe(ctx, args, stdout, stderr, handlers...)
}
