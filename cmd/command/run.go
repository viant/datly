package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/internal/setter"
)

func (s *Service) Run(ctx context.Context, options *options.Options) (err error) {
	loc, err := s.loadPlugin(ctx, options)
	if err != nil {
		return err
	}
	options.Run.PluginInfo = loc
	options.Run.Version = options.Version
	srv, err := s.run(ctx, options.Run)
	if err != nil {
		return err
	}
	if srv.MCP != nil {
		go func() {
			srv.MCP.ListenAndServe()
		}()
	}
	return srv.ListenAndServe()
}

func (s *Service) run(ctx context.Context, run *options.Run) (*standalone.Server, error) {
	var err error
	if s.config, err = standalone.NewConfigFromURL(ctx, run.ConfigURL); err != nil {
		return nil, err
	}
	setter.SetStringIfEmpty(&s.config.JobURL, run.JobURL)
	setter.SetStringIfEmpty(&s.config.FailedJobURL, run.FailedJobURL)
	setter.SetIntIfZero(&s.config.MaxJobs, run.MaxJobs)
	applyAsyncJobDefaults(s.config)
	if run.LoadPlugin && s.config.Config.PluginsURL != "" {
		parent, _ := url.Split(run.PluginInfo, file.Scheme)
		_ = s.fs.Copy(ctx, parent, s.config.Config.PluginsURL)
	}
	s.config.Version = run.Version
	return standalone.New(ctx, standalone.WithConfig(s.config))
}
