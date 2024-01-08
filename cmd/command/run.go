package command

import (
	"context"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/service/auth/jwt"
)

func (s *Service) Run(ctx context.Context, options *options.Options) (err error) {
	loc, err := s.loadPlugin(ctx, options)
	if err != nil {
		return err
	}
	options.Run.PluginInfo = loc
	srv, err := s.run(ctx, options.Run)
	if err != nil {
		return err
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
	if s.config.FailedJobURL == "" && s.config.JobURL != "" {
		parent, _ := url.Split(s.config.JobURL, file.Scheme)
		s.config.FailedJobURL = url.Join(parent, "failed", "jobs")
	}

	if run.LoadPlugin && s.config.Config.PluginsURL != "" {
		parent, _ := url.Split(run.PluginInfo, file.Scheme)
		_ = s.fs.Copy(ctx, parent, s.config.Config.PluginsURL)
	}

	authenticator, err := jwt.Init(s.config.Config, nil)
	var srv *standalone.Server
	if authenticator == nil {
		srv, err = standalone.New(ctx, s.config)
	} else {
		srv, err = standalone.NewWithAuth(ctx, s.config, authenticator)
	}
	return srv, err
}
