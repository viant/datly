package translator

import (
	"context"
	"database/sql"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view"
	"path"
)

type Repository struct {
	fs              afs.Service
	Config          *Config
	Resource        []*Resource
	State           inference.State
	Connectors      []*view.Connector
	NamedConnectors view.Connectors
	Caches          view.Caches
	MessageBuses    []*mbus.Resource
	Messages        Messages
	Files           asset.Files
}

func (r *Repository) RuleName(rule *options.Rule) string {
	_, name := url.Split(rule.Source, file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	return name
}

func (r *Repository) RuleBaseURL(rule *options.Rule) string {
	return url.Join(r.Config.Config.RouteURL, rule.Prefix)
}

func (r *Repository) LookupDb(name string) (*sql.DB, error) {
	for _, candidate := range r.Connectors {
		if candidate.Name == name {
			return candidate.DB()
		}
	}
	return r.Connectors[0].DB()
}

//Init Initialises translator repository
func (r *Repository) Init(ctx context.Context) error {
	if err := r.Config.Init(ctx); err != nil {
		return err
	}
	if err := r.ensureDependencies(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Repository) ensureDependencies(ctx context.Context) error {
	if err := r.ensureConnectors(ctx); err != nil {
		return err
	}
	if err := r.ensureConstants(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Repository) Upload(ctx context.Context) error {
	return r.Files.Upload(ctx, r.fs)
}

func NewRepository(config *Config) *Repository {
	return &Repository{
		Config: config,
		fs:     afs.New(),
	}
}
