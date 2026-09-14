// Package main serves the Datly developer MCP service over stdio.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/viant/datly/mcp/developer"
	"github.com/viant/datly/mcp/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	location := flag.String("config", "", "operator-owned developer configuration JSON")
	flag.Parse()
	if *location == "" {
		fmt.Fprintln(os.Stderr, "-config is required")
		os.Exit(2)
	}
	file, err := os.Open(*location)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var config developer.Config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&config)
	file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid developer configuration")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	service, err := developer.New(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	transport, err := server.New(server.Config{Service: service, Transport: server.TransportConfig{Kind: server.TransportStdio}})
	if err == nil {
		err = transport.Serve(ctx)
	}
	cleanup := service.Shutdown(context.Background())
	if err != nil || cleanup != nil {
		fmt.Fprintln(os.Stderr, err, cleanup)
		os.Exit(1)
	}
}
