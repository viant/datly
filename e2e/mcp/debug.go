package main

import (
	"context"
	"fmt"
	"github.com/viant/jsonrpc/transport/client/stdio"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp/client"
	"github.com/viant/toolbox"
	"log"
	"path/filepath"
	"strings"
	"time"
)

func main() {

	var baseDir = toolbox.CallerDirectory(3)
	configURL := filepath.Join(baseDir, "../local/autogen/Datly/config.json")
	datlyBin := filepath.Join(baseDir, "../../cmd/datly/datly") // Path to datly binary for stdio client
	args := []string{
		"mcp",
		"-c",
		configURL,
		"-z",
		"/tmp/jobs/datly",
	}
	fmt.Println(args)
	fmt.Println("Starting MCP client with args:", datlyBin+strings.Join(args, " "))

	transport, err := stdio.New(datlyBin, stdio.WithArguments(strings.Join(args, " ")))
	if err != nil {
		log.Fatalf("Failed to create stdio transport: %v", err)
	}
	c := client.New("datly-debug", "0.1", transport)
	defer c.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize the client
	fmt.Println("Initializing client...")
	initResult, err := c.Initialize(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	fmt.Printf(
		"Initialized with server: %s %s\n\n",
		initResult.ServerInfo.Name,
		initResult.ServerInfo.Version,
	)

	readRequest := &schema.ReadResourceRequestParams{Uri: "datly://localhost/v1/api/dev/vendors/12345"}
	_, _ = c.ReadResource(ctx, readRequest) // ensure the client is initialized before proceeding

	// List Tools
	fmt.Println("Listing available tools...")
	tools, err := c.ListResourceTemplates(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to list tools: %v", err)
	}
	for _, tool := range tools.ResourceTemplates {
		desc := ""
		if tool.Description != nil {
			desc = *tool.Description
		}
		fmt.Printf("- %s: %s\n", tool.Name, desc)
	}
}
