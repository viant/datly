package shapeparity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/cmd/options"
	dqlscan "github.com/viant/datly/repository/shape/dql/scan"
)

// ScanDQL runs the legacy translator into a temporary repository, then feeds the
// generated route YAML back through the shape scanner so parity tests can
// compare the legacy YAML contract against shape IR.
func ScanDQL(ctx context.Context, req *dqlscan.Request) (*dqlscan.Result, error) {
	if req == nil {
		return nil, fmt.Errorf("shape parity scan request was nil")
	}
	dqlURL := strings.TrimSpace(req.DQLURL)
	if dqlURL == "" {
		return nil, fmt.Errorf("shape parity scan request DQLURL was empty")
	}
	fs := afs.New()
	dqlBytes, err := fs.DownloadWithURL(ctx, dqlURL)
	if err != nil {
		return nil, fmt.Errorf("failed to read DQL %s: %w", dqlURL, err)
	}
	tmpRepo, err := os.MkdirTemp("", "datly-shapeparity-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp repository: %w", err)
	}
	defer os.RemoveAll(tmpRepo)

	projectRoot := inferProjectRoot(req, dqlURL)
	modulePrefix := strings.Trim(strings.TrimSpace(req.ModulePrefix), "/")
	apiPrefix := strings.TrimSpace(req.APIPrefix)
	if apiPrefix == "" {
		apiPrefix = "/v1/api"
	}
	repoOpts := options.Repository{
		RepositoryURL: tmpRepo,
		ProjectURL:    projectRoot,
		APIPrefix:     apiPrefix,
	}
	repoOpts.Connectors = append(repoOpts.Connectors, req.Connectors...)
	if cfgURL := strings.TrimSpace(req.ConfigURL); cfgURL != "" {
		repoOpts.Configs.Append(cfgURL)
	}
	opts := &options.Options{
		Translate: &options.Translate{
			Rule: options.Rule{
				Project:      projectRoot,
				ModulePrefix: modulePrefix,
				Source:       []string{dqlURL},
				ModuleLocation: func() string {
					if req.Repository != "" {
						return filepath.Join(req.Repository, "pkg")
					}
					return filepath.Join(projectRoot, "pkg")
				}(),
				Engine: options.EngineLegacy,
			},
			Repository: repoOpts,
		},
	}
	if err = opts.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialise legacy translate options: %w", err)
	}
	if err = command.New().Translate(ctx, opts); err != nil {
		return nil, fmt.Errorf("failed to translate DQL %s with legacy pipeline: %w", dqlURL, err)
	}

	ruleName := strings.TrimSuffix(filepath.Base(url.Path(dqlURL)), filepath.Ext(url.Path(dqlURL)))
	routeYAMLURL := filepath.Join(tmpRepo, "Datly", "routes")
	if modulePrefix != "" {
		routeYAMLURL = filepath.Join(routeYAMLURL, filepath.FromSlash(modulePrefix))
	}
	routeYAMLURL = filepath.Join(routeYAMLURL, ruleName+".yaml")
	yamlBytes, err := fs.DownloadWithURL(ctx, routeYAMLURL)
	if err != nil {
		return nil, fmt.Errorf("failed to read generated route YAML %s: %w", routeYAMLURL, err)
	}
	return dqlscan.New().Result(ruleName, yamlBytes, string(dqlBytes), req)
}

func inferProjectRoot(req *dqlscan.Request, dqlURL string) string {
	if repositoryRoot := strings.TrimSpace(req.Repository); repositoryRoot != "" {
		return filepath.Dir(filepath.Clean(repositoryRoot))
	}
	if scheme := url.Scheme(dqlURL, file.Scheme); scheme != "" && scheme != file.Scheme {
		return filepath.Dir(url.Path(dqlURL))
	}
	return filepath.Dir(filepath.Clean(url.Path(dqlURL)))
}
