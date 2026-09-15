package server

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	upstream "github.com/viant/mcp/server"
	"github.com/viant/mcp/server/auth"
)

type TransportKind string

const (
	TransportStdio      TransportKind = "stdio"
	TransportSSE        TransportKind = "sse"
	TransportStreamable TransportKind = "streamable"
)

type ServerService interface {
	Service
	Authorization() *authorization.Policy
}

type Config struct {
	ResourceAuthorizer auth.ResourceAuthorizer
	Service            ServerService
	Source             Source
	binding            *sourceBinding
	Implementation     schema.Implementation
	ProtocolVersion    string
	LoggerName         string
	Transport          TransportConfig
}

type TransportConfig struct {
	// Zero selects safe listener defaults; negative explicitly disables a deadline.
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	Kind              TransportKind
	Address           string
	SSEURI            string
	SSEMessageURI     string
	StreamableURI     string
	RootRedirect      bool
	CORS              *upstream.Cors
}

func (c Config) normalized() (Config, error) {
	if c.Source != nil {
		c.binding = &sourceBinding{source: c.Source}
	}
	if c.Source != nil && c.Service != nil {
		return Config{}, fmt.Errorf("MCP service and source are mutually exclusive")
	}
	if c.Source == nil && (c.Service == nil || c.Service.Registry() == nil) {
		return Config{}, fmt.Errorf("MCP service and registry are required")
	}
	if c.Transport.Kind == "" {
		c.Transport.Kind = TransportStdio
	}
	switch c.Transport.Kind {
	case TransportStdio:
	case TransportSSE, TransportStreamable:
		if strings.TrimSpace(c.Transport.Address) == "" {
			c.Transport.Address = "127.0.0.1:5000"
		}
	default:
		return Config{}, fmt.Errorf("unsupported MCP transport %q", c.Transport.Kind)
	}
	if err := validateTransportURIs(c.Transport); err != nil {
		return Config{}, err
	}
	return c, nil
}

func validateTransportURIs(transport TransportConfig) error {
	values := map[string]string{
		"SSE URI":         defaultURI(transport.SSEURI, "/sse"),
		"SSE message URI": defaultURI(transport.SSEMessageURI, "/message"),
		"streamable URI":  defaultURI(transport.StreamableURI, "/mcp"),
	}
	seen := map[string]string{}
	for name, value := range values {
		parsed, err := url.Parse(value)
		if err != nil || !strings.HasPrefix(value, "/") || parsed.IsAbs() || parsed.Host != "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("MCP %s must be an absolute path without query or fragment", name)
		}
		if previous := seen[value]; previous != "" {
			return fmt.Errorf("MCP %s conflicts with %s at %q", name, previous, value)
		}
		seen[value] = name
	}
	return nil
}

func defaultURI(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
