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
	"github.com/viant/datly/internal/msg"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"path"
	"strings"
)

type (
	Repository struct {
		fs              afs.Service
		Config          *Config
		Resource        []*Resource
		State           inference.State
		Connectors      []*view.Connector
		NamedConnectors view.Connectors
		Caches          view.Caches
		MessageBuses    []*mbus.Resource
		Messages        msg.Messages
		Files           asset.Files
		Substitutes     view.Substitutes
	}
)

func (r *Repository) RuleName(rule *options.Rule) string {
	_, name := url.Split(rule.SourceURL(), file.Scheme)
	if ext := path.Ext(name); ext != "" {
		name = name[:len(name)-len(ext)]
	}
	return name
}

func (r *Repository) RuleBaseURL(rule *options.Rule) string {
	return url.Join(r.Config.Config.RouteURL, rule.ModulePrefix)
}

func (r *Repository) LookupDb(name string) (*sql.DB, error) {
	for _, candidate := range r.Connectors {
		if candidate.Name == name {
			return candidate.DB()
		}
	}
	return r.Connectors[0].DB()
}

// Init Initialises translator repository
func (r *Repository) Init(ctx context.Context) error {
	if err := r.Config.Init(ctx); err != nil {
		return err
	}
	if err := r.ensureDependencies(ctx); err != nil {
		return err
	}
	return nil
}

func (r *Repository) PersistConfig() error {
	cfg := r.Config
	cfg.NormalizeURL(cfg.repository.RepositoryURL)
	config, err := asset.EncodeJSON(r.Config)
	if err != nil {
		return err
	}
	if err = r.persistConnections(cfg); err != nil {
		return err
	}
	if err = r.persistConstants(); err != nil {
		return err
	}
	if err = r.persistSubstitutes(); err != nil {
		return err
	}
	if err = r.persistCache(); err != nil {
		return err
	}
	r.Files.Append(asset.NewFile(cfg.URL, string(config)))
	return nil
}

func (r *Repository) persistSubstitutes() error {
	if len(r.Substitutes) == 0 {
		return nil
	}
	cfg := r.Config
	resource := view.Resource{Substitutes: r.Substitutes}
	content, err := asset.EncodeYAML(resource)
	if err != nil {
		return err
	}
	r.Files.Append(asset.NewFile(url.Join(cfg.DependencyURL, "substitutes.yaml"), string(content)))
	return nil
}

func (r *Repository) persistConstants() error {
	cfg := r.Config.Config
	literals := r.State.FilterByKind(state.KindConst)
	if len(literals) == 0 {
		return nil
	}
	resource := view.Resource{Parameters: literals.ViewParameters()}
	content, err := asset.EncodeYAML(resource)
	if err != nil {
		return err
	}
	data := string(content)
	data = r.Substitutes.ReverseReplace(data)
	r.Files.Append(asset.NewFile(url.Join(cfg.DependencyURL, "constants.yaml"), data))
	return nil
}

func (r *Repository) persistConnections(cfg *Config) error {
	if len(r.Connectors) == 0 {
		return nil
	}
	resource := view.Resource{Connectors: r.Connectors}
	connectors, err := asset.EncodeYAML(resource)
	if err != nil {
		return err
	}
	r.Files.Append(asset.NewFile(url.Join(cfg.DependencyURL, "connections.yaml"), string(connectors)))
	return nil
}

func (r *Repository) ensureDependencies(ctx context.Context) error {
	if err := r.ensureSubstitutes(ctx); err != nil {
		return err
	}
	if err := r.ensureConnectors(ctx); err != nil {
		return err
	}
	if err := r.ensureConstants(ctx); err != nil {
		return err
	}
	if err := r.ensureCache(ctx); err != nil {
		return err
	}
	return nil
}

// UploadPartialRules uploads rule into dest, this is temporary to see signature for component parameters
func (r *Repository) UploadPartialRules(ctx context.Context) error {
	if len(r.Resource) == 0 {
		return nil
	}
	var yamlFiles asset.Files
	for i, candidate := range r.Files {
		if strings.HasSuffix(candidate.URL, ".yaml") {
			yamlFiles = append(yamlFiles, r.Files[i])
		}
	}
	return yamlFiles.Upload(ctx, r.fs)
}

func (r *Repository) Upload(ctx context.Context) error {
	if len(r.Resource) == 0 {
		return nil
	}
	return r.Files.Upload(ctx, r.fs)
}

func (r *Repository) persistCache() error {
	if len(r.Caches) == 0 {
		return nil
	}
	cfg := r.Config
	resource := view.Resource{CacheProviders: r.Caches}
	content, err := asset.EncodeYAML(resource)
	if err != nil {
		return err
	}
	r.Files.Append(asset.NewFile(url.Join(cfg.DependencyURL, "cache.yaml"), string(content)))
	return nil

}

func NewRepository(config *Config) *Repository {
	return &Repository{
		Config: config,
		fs:     afs.New(),
	}
}
