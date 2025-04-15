package mcp

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/mcp/extension"
	"github.com/viant/mcp/protocol/server"
	"github.com/viant/mcp/schema"
	"strconv"
)

type Server struct {
	server    *server.Server // The underlying MCP server instance
	options   *options.Mcp
	extension *extension.Integration
}

func (s *Server) init() error {

	stream := false
	var newImplementer = extension.New(s.extension)
	var options = []server.Option{
		server.WithNewImplementer(newImplementer),
		server.WithImplementation(schema.Implementation{"Datly", "0.1"}),
		server.WithCapabilities(schema.ServerCapabilities{
			Resources: &schema.ServerCapabilitiesResources{ListChanged: &stream},
			Tools:     &schema.ServerCapabilitiesTools{ListChanged: &stream},
		}),
	}
	srv, err := server.New(options...)
	if err != nil {
		return err
	}
	s.server = srv
	return nil
}

func (s *Server) ListenAndServe() error {
	if s.options.Port == nil {
		stdio := s.server.Stdio(context.Background())
		err := stdio.ListenAndServe()
		if err != nil {
			fmt.Printf("Server error: %v\n", err)
			return err
		}
	} else {
		httpServer := s.server.HTTP(context.Background(), ":"+strconv.Itoa(*s.options.Port))
		err := httpServer.ListenAndServe()
		if err != nil {
			// Handle error starting the SSE server
			fmt.Printf("Failed to start SSE server: %v\n", err)
			return err
		}
		defer httpServer.Shutdown(context.Background()) // Ensure the server shuts down gracefully
	}
	return nil
}

func NewServer(extension *extension.Integration, mcp *options.Mcp) (*Server, error) {
	if extension == nil {
		return nil, nil
	}
	s := &Server{
		options:   mcp,
		extension: extension,
	}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}
