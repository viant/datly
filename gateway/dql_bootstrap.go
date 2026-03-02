package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	datlyservice "github.com/viant/datly/service"
	"github.com/viant/datly/view"
)

func (r *Service) applyDQLBootstrap(ctx context.Context, repo *repository.Service, cfg *DQLBootstrap) error {
	if cfg == nil || len(cfg.Sources) == 0 {
		return nil
	}
	sources, err := discoverDQLBootstrapSources(cfg.Sources, cfg.Exclude)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return fmt.Errorf("no DQL bootstrap sources matched")
	}
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	precedence := cfg.EffectivePrecedence()
	var errors []error
	for _, sourcePath := range sources {
		component, err := compileBootstrapComponent(ctx, compiler, loader, repo, sourcePath, cfg, r.Config.APIPrefix)
		if err != nil {
			if cfg.ShouldFailFast() {
				return err
			}
			errors = append(errors, err)
			continue
		}
		exists, lookupErr := hasRepositoryProvider(ctx, repo, &component.Path)
		if lookupErr != nil {
			if cfg.ShouldFailFast() {
				return lookupErr
			}
			errors = append(errors, lookupErr)
			continue
		}
		if exists {
			switch precedence {
			case DQLBootstrapPrecedenceRoutesWins:
				continue
			case DQLBootstrapPrecedenceErrorOnMixed:
				err = fmt.Errorf("DQL bootstrap conflict for %s:%s", component.Method, component.URI)
				if cfg.ShouldFailFast() {
					return err
				}
				errors = append(errors, err)
				continue
			}
		}
		repo.Register(component)
	}
	if len(errors) > 0 {
		return fmt.Errorf("DQL bootstrap completed with %d errors: %w", len(errors), errors[0])
	}
	return nil
}

func compileBootstrapComponent(ctx context.Context, compiler *shapeCompile.DQLCompiler, loader *shapeLoad.Loader, repo *repository.Service, sourcePath string, cfg *DQLBootstrap, apiPrefix string) (*repository.Component, error) {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read DQL bootstrap source %s: %w", sourcePath, err)
	}
	dql := strings.TrimSpace(string(data))
	if dql == "" {
		return nil, fmt.Errorf("empty DQL bootstrap source: %s", sourcePath)
	}
	sourceName := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	source := &shape.Source{
		Name: sourceName,
		Path: sourcePath,
		DQL:  dql,
	}
	planResult, err := compiler.Compile(ctx, source, compileOptionsFromBootstrap(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("failed to compile DQL bootstrap source %s: %w", sourcePath, err)
	}
	componentArtifact, err := loader.LoadComponent(ctx, planResult)
	if err != nil {
		return nil, fmt.Errorf("failed to load DQL bootstrap source %s: %w", sourcePath, err)
	}
	normalizeBootstrapInlineSQL(componentArtifact.Resource)
	mergeBootstrapSharedResources(componentArtifact.Resource, repo)
	loaded, ok := componentArtifact.Component.(*shapeLoad.Component)
	if !ok || loaded == nil {
		return nil, fmt.Errorf("unexpected shape component artifact for %s", sourcePath)
	}
	rootView := lookupRootView(componentArtifact.Resource, loaded.RootView)
	if rootView == nil {
		return nil, fmt.Errorf("missing root view %q for %s", loaded.RootView, sourcePath)
	}
	method, uri := resolvePathSettings(sourcePath, dql, apiPrefix)
	componentModel := &repository.Component{
		Path: contract.Path{
			Method: method,
			URI:    uri,
		},
		Contract: contract.Contract{
			Service: defaultServiceForMethod(method, rootView),
		},
		View:        rootView,
		TypeContext: loaded.TypeContext,
	}
	loadOptions := []repository.Option{}
	if repo != nil {
		loadOptions = append(loadOptions, repository.WithResources(repo.Resources()))
		loadOptions = append(loadOptions, repository.WithExtensions(repo.Extensions()))
	}
	components, err := repository.LoadComponentsFromMap(ctx, map[string]any{
		"Resource":   componentArtifact.Resource,
		"Components": []*repository.Component{componentModel},
	}, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize bootstrap component for %s: %w", sourcePath, err)
	}
	if err = components.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize bootstrap component for %s: %w", sourcePath, err)
	}
	if len(components.Components) == 0 || components.Components[0] == nil {
		return nil, fmt.Errorf("empty initialized bootstrap component for %s", sourcePath)
	}
	return components.Components[0], nil
}

func mergeBootstrapSharedResources(target *view.Resource, repo *repository.Service) {
	if target == nil || repo == nil || repo.Resources() == nil {
		return
	}
	if connectors, err := repo.Resources().Lookup(view.ResourceConnectors); err == nil && connectors != nil && connectors.Resource != nil {
		target.MergeFrom(connectors.Resource, nil)
	}
	if constants, err := repo.Resources().Lookup(view.ResourceConstants); err == nil && constants != nil && constants.Resource != nil {
		target.MergeFrom(constants.Resource, nil)
	}
}

func normalizeBootstrapInlineSQL(resource *view.Resource) {
	if resource == nil {
		return
	}
	for _, item := range resource.Views {
		if item == nil || item.Template == nil {
			continue
		}
		// DQL bootstrap compiles from in-memory source; keep SQL inline and avoid filesystem lookups.
		item.Template.SourceURL = ""
	}
}

func defaultServiceForMethod(method string, rootView *view.View) datlyservice.Type {
	if strings.EqualFold(method, "GET") {
		return datlyservice.TypeReader
	}
	if rootView != nil && rootView.Mode == view.ModeQuery {
		return datlyservice.TypeReader
	}
	return datlyservice.TypeExecutor
}

func hasRepositoryProvider(ctx context.Context, repo *repository.Service, path *contract.Path) (bool, error) {
	if repo == nil || repo.Registry() == nil || path == nil {
		return false, nil
	}
	_, err := repo.Registry().LookupProvider(ctx, path)
	if err != nil {
		message := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(message, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func compileOptionsFromBootstrap(cfg *DQLBootstrap) []shape.CompileOption {
	if cfg == nil {
		return nil
	}
	var result []shape.CompileOption
	switch strings.ToLower(strings.TrimSpace(cfg.CompileProfile)) {
	case string(shape.CompileProfileStrict):
		result = append(result, shape.WithCompileProfile(shape.CompileProfileStrict))
	case string(shape.CompileProfileCompat):
		result = append(result, shape.WithCompileProfile(shape.CompileProfileCompat))
	}
	switch strings.ToLower(strings.TrimSpace(cfg.MixedMode)) {
	case string(shape.CompileMixedModeExecWins):
		result = append(result, shape.WithMixedMode(shape.CompileMixedModeExecWins))
	case string(shape.CompileMixedModeReadWins):
		result = append(result, shape.WithMixedMode(shape.CompileMixedModeReadWins))
	case string(shape.CompileMixedModeErrorOnMixed):
		result = append(result, shape.WithMixedMode(shape.CompileMixedModeErrorOnMixed))
	}
	switch strings.ToLower(strings.TrimSpace(cfg.UnknownNonReadMode)) {
	case string(shape.CompileUnknownNonReadWarn):
		result = append(result, shape.WithUnknownNonReadMode(shape.CompileUnknownNonReadWarn))
	case string(shape.CompileUnknownNonReadError):
		result = append(result, shape.WithUnknownNonReadMode(shape.CompileUnknownNonReadError))
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ColumnDiscoveryMode)) {
	case string(shape.CompileColumnDiscoveryAuto):
		result = append(result, shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryAuto))
	case string(shape.CompileColumnDiscoveryOn):
		result = append(result, shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOn))
	case string(shape.CompileColumnDiscoveryOff):
		result = append(result, shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOff))
	}
	if marker := strings.TrimSpace(cfg.DQLPathMarker); marker != "" {
		result = append(result, shape.WithDQLPathMarker(marker))
	}
	if rel := strings.TrimSpace(cfg.RoutesRelativePath); rel != "" {
		result = append(result, shape.WithRoutesRelativePath(rel))
	}
	return result
}

func discoverDQLBootstrapSources(includes, excludes []string) ([]string, error) {
	seen := map[string]struct{}{}
	var result []string
	for _, include := range includes {
		include = strings.TrimSpace(include)
		if include == "" {
			continue
		}
		expanded, err := expandBootstrapPattern(include)
		if err != nil {
			return nil, err
		}
		for _, candidate := range expanded {
			if !isDQLSourceFile(candidate) {
				continue
			}
			if matchesAnyPattern(candidate, excludes) {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			result = append(result, candidate)
		}
	}
	sort.Strings(result)
	return result, nil
}

func expandBootstrapPattern(pattern string) ([]string, error) {
	pattern = filepath.Clean(pattern)
	if strings.Contains(pattern, "**") {
		return expandDoubleStarPattern(pattern)
	}
	if hasGlobMeta(pattern) {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		return flattenPaths(matches)
	}
	return flattenPaths([]string{pattern})
}

func flattenPaths(items []string) ([]string, error) {
	var result []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		info, err := os.Stat(item)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if !info.IsDir() {
			result = append(result, item)
			continue
		}
		err = filepath.WalkDir(item, func(candidate string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if isDQLSourceFile(candidate) {
				result = append(result, candidate)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func expandDoubleStarPattern(pattern string) ([]string, error) {
	slash := filepath.ToSlash(pattern)
	index := strings.Index(slash, "**")
	root := strings.TrimSuffix(slash[:index], "/")
	if root == "" {
		root = "."
	}
	rootPath := filepath.FromSlash(root)
	var result []string
	err := filepath.WalkDir(rootPath, func(candidate string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		normalized := filepath.ToSlash(candidate)
		if !globMatch(slash, normalized) {
			return nil
		}
		result = append(result, candidate)
		return nil
	})
	return result, err
}

func hasGlobMeta(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
}

func matchesAnyPattern(candidate string, patterns []string) bool {
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if globMatch(filepath.ToSlash(pattern), filepath.ToSlash(candidate)) {
			return true
		}
	}
	return false
}

func globMatch(pattern, candidate string) bool {
	pattern = filepath.ToSlash(pattern)
	candidate = filepath.ToSlash(candidate)
	if strings.Contains(pattern, "**") {
		return matchDoubleStar(strings.Split(pattern, "/"), strings.Split(candidate, "/"))
	}
	ok, _ := path.Match(pattern, candidate)
	return ok
}

func matchDoubleStar(pattern, candidate []string) bool {
	if len(pattern) == 0 {
		return len(candidate) == 0
	}
	head := pattern[0]
	if head == "**" {
		if matchDoubleStar(pattern[1:], candidate) {
			return true
		}
		if len(candidate) > 0 {
			return matchDoubleStar(pattern, candidate[1:])
		}
		return false
	}
	if len(candidate) == 0 {
		return false
	}
	ok, _ := path.Match(head, candidate[0])
	if !ok {
		return false
	}
	return matchDoubleStar(pattern[1:], candidate[1:])
}

func isDQLSourceFile(path string) bool {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(path)))
	return ext == ".dql" || ext == ".sql"
}

func lookupRootView(resource *view.Resource, root string) *view.View {
	if resource == nil {
		return nil
	}
	name := strings.TrimSpace(root)
	if name != "" {
		if candidate, _ := resource.View(name); candidate != nil {
			return candidate
		}
	}
	if len(resource.Views) > 0 {
		return resource.Views[0]
	}
	return nil
}

type bootstrapRuleSettings struct {
	Method string `json:"Method"`
	URI    string `json:"URI"`
}

func resolvePathSettings(sourcePath, dql, apiPrefix string) (string, string) {
	method := "GET"
	uri := ""
	settings := parseBootstrapRuleSettings(dql)
	if settings != nil {
		if candidate := strings.TrimSpace(strings.ToUpper(settings.Method)); candidate != "" {
			method = candidate
		}
		uri = strings.TrimSpace(settings.URI)
	}
	if uri == "" {
		stem := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
		uri = "/" + strings.Trim(stem, "/")
		if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
			uri = strings.TrimRight(prefix, "/") + uri
		}
	}
	return method, uri
}

func parseBootstrapRuleSettings(dql string) *bootstrapRuleSettings {
	start := strings.Index(dql, "/*")
	end := strings.Index(dql, "*/")
	if start == -1 || end == -1 || end <= start+2 {
		return nil
	}
	raw := strings.TrimSpace(dql[start+2 : end])
	if !strings.HasPrefix(raw, "{") || !strings.HasSuffix(raw, "}") {
		return nil
	}
	ret := &bootstrapRuleSettings{}
	if err := json.Unmarshal([]byte(raw), ret); err != nil {
		return nil
	}
	return ret
}
