package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/mcp"
)

func (s *Service) Mcp(ctx context.Context, options *options.Options) (err error) {
	//loc, err := s.loadPlugin(ctx, options)
	//if err != nil {
	//	return err
	//}
	//options.Mcp.PluginInfo = loc
	options.Mcp.Version = options.Version
	return s.mcp(ctx, options.Mcp)
}

func (s *Service) mcp(ctx context.Context, mcpOption *options.Mcp) error {
	var err error
	if s.config, err = standalone.NewConfigFromURL(ctx, mcpOption.ConfigURL); err != nil {
		return err
	}
	setter.SetStringIfEmpty(&s.config.JobURL, mcpOption.JobURL)
	setter.SetStringIfEmpty(&s.config.FailedJobURL, mcpOption.FailedJobURL)
	setter.SetIntIfZero(&s.config.MaxJobs, mcpOption.MaxJobs)
	if s.config.FailedJobURL == "" && s.config.JobURL != "" {
		parent, _ := url.Split(s.config.JobURL, file.Scheme)
		s.config.FailedJobURL = url.Join(parent, "failed", "jobs")
	}
	if mcpOption.LoadPlugin && s.config.Config.PluginsURL != "" {
		parent, _ := url.Split(mcpOption.PluginInfo, file.Scheme)
		_ = s.fs.Copy(ctx, parent, s.config.Config.PluginsURL)
	}

	s.config.Version = mcpOption.Version
	disabled := false
	s.config.Logging.EnableAudit = &disabled
	s.config.Logging.EnableTracing = &disabled
	s.config.Logging.IncludeSQL = &disabled
	s.config.MCPEndpoint = &gateway.Endpoint{}

	if mcpOption.Port != nil {
		s.config.MCPEndpoint.Port = *mcpOption.Port
	} else {
		s.config.MCPEndpoint.Stdio = true
	}

	service, _, err := standalone.NewService(ctx, standalone.WithConfig(s.config))
	if err != nil {
		return err
	}
	integration := service.MCP()

	server, err := mcp.NewServer(integration, mcpOption)
	if err != nil {
		return err
	}
	return server.ListenAndServe() // Start the MCP server
}
