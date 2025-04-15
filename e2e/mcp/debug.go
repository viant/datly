package main

import (
	"context"
	"fmt"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
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

	c, err := client.NewStdioMCPClient(datlyBin, []string{}, args...)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initialize the client
	fmt.Println("Initializing client...")
	initRequest := mcp.InitializeRequest{}
	initRequest.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initRequest.Params.ClientInfo = mcp.Implementation{
		Name:    "example-client",
		Version: "1.0.0",
	}

	initResult, err := c.Initialize(ctx, initRequest)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	fmt.Printf(
		"Initialized with server: %s %s\n\n",
		initResult.ServerInfo.Name,
		initResult.ServerInfo.Version,
	)

	readRequest := mcp.ReadResourceRequest{
		Request: mcp.Request{
			Method: string(mcp.MethodResourcesRead),
		},
	}
	readRequest.Params.URI = "datly://localhost/v1/api/dev/vendors/{vendorID}"
	readRequest.Params.Arguments = map[string]interface{}{
		"vendorID": "12345", // Example vendor ID to read
	}

	c.ReadResource(ctx, readRequest) // ensure the client is initialized before proceeding

	// List Tools
	fmt.Println("Listing available tools...")
	toolsRequest := mcp.ListResourceTemplatesRequest{}
	tools, err := c.ListResourceTemplates(ctx, toolsRequest)
	if err != nil {
		log.Fatalf("Failed to list tools: %v", err)
	}
	for _, tool := range tools.ResourceTemplates {
		fmt.Printf("- %s: %s\n", tool.Name, tool.Description)
	}
}
