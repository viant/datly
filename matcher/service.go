package matcher

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/shared"
	"net/url"
)

//Service returns matched rule
type Service interface {

	//Match matches rule with supplied path
	Match(ctx context.Context, request *contract.Request, response *contract.Response) (*config.Rule, error)
	//Length returns  rules count
	Count() int
}

type service struct {
	config *config.Config
	fs     afs.Service
}

//Count returns rules count
func (s *service) Count() int {
	return s.config.Rules.Len()
}

func (s *service) Match(ctx context.Context, request *contract.Request, response *contract.Response) (*config.Rule, error) {
	rule, uriParams, err := s.match(ctx, request.Path)
	if err != nil {
		response.AddError(shared.ErrorTypeRule, "match", err)
		return nil, nil
	}
	response.Rules = s.Count()
	if rule == nil {
		response.Status = shared.StatusNoMatch
		return nil, nil
	}
	response.Rule = rule
	request.PathParams = uriParams
	return rule, nil
}

func (s *service) match(ctx context.Context, path string) (*config.Rule, url.Values, error) {
	err := s.config.ReloadChanged(ctx, s.fs)
	if err != nil {
		return nil, nil, err
	}
	rule, uriParams := s.config.Rules.Match(path)
	return rule, uriParams, nil
}

//New creates a service
func New(ctx context.Context, config *config.Config) (Service, error) {
	fs := afs.New()
	if config.CacheRules && config.URL != "" {
		fs = cache.Singleton(config.URL)
	}
	err := config.Init(ctx, fs)
	srv := &service{
		config: config,
		fs:     fs,
	}
	return srv, err
}
