package options

import (
	"fmt"
)

type Run struct {
	ConfigURL    string   `short:"c" long:"conf" description:"datly config"`
	WarmupURIs   []string `short:"w" long:"warmup" description:"warmup uris"`
	JobURL       string   `short:"z" long:"joburl" description:"job url"`
	MaxJobs      int      `short:"W" long:"mjobs" description:"max jobs" default:"40" `
	FailedJobURL string   `short:"F" long:"fjobs" description:"failed jobs" `
	LoadPlugin   bool     `short:"L" long:"lplugin" description:"load plugin"`
	MCPPort      *int     `long:"mcpPort" description:"enable MCP HTTP server on the specified port"`
	MCPAuthURL   string   `long:"mcpAuthClient" description:"auth client url for MCP server"`
	MCPIssuerURL string   `long:"mcpIssuerURL" description:"issuer url for MCP server"`
	MCPAuthMode  string   `long:"mcpAuth" description:"authorizer S - server authorizer, F fallback authorizer" choice:"F" choice:"S"`
	PluginInfo   string
	Version      string
}

func (r *Run) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	r.ConfigURL = ensureAbsPath(r.ConfigURL)
	return nil
}
