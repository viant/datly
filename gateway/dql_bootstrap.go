package gateway

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/afs"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	datlyservice "github.com/viant/datly/service"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
)

var bootstrapFS = afs.New()

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

func compileBootstrapComponent(ctx context.Context, compiler *shapeCompile.DQLCompiler, loader *shapeLoad.Loader, repo *repository.Service, sourcePath string, cfg *DQLBootstrap, _ string) (*repository.Component, error) {
	data, err := bootstrapFS.DownloadWithURL(ctx, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read DQL bootstrap source %s: %w", sourcePath, err)
	}
	dql := strings.TrimSpace(string(data))
	if dql == "" {
		return nil, fmt.Errorf("empty DQL bootstrap source: %s", sourcePath)
	}
	sourceName := bootstrapSourceName(sourcePath)
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
	normalizeBootstrapTemplateParameters(componentArtifact.Resource)
	normalizeBootstrapViewAliases(componentArtifact.Resource)
	normalizeBootstrapCaches(componentArtifact.Resource)
	loaded, ok := componentArtifact.Component.(*shapeLoad.Component)
	if !ok || loaded == nil {
		return nil, fmt.Errorf("unexpected shape component artifact for %s", sourcePath)
	}
	bootstrapMetadata := snapshotBootstrapViewMetadata(componentArtifact.Resource)
	rootView := lookupRootView(componentArtifact.Resource, loaded.RootView)
	if rootView == nil && bootstrapRequiresRootView(loaded) {
		return nil, fmt.Errorf("missing root view %q for %s", loaded.RootView, sourcePath)
	}
	if rootView == nil {
		rootView = bootstrapHandlerView(componentArtifact.Resource, loaded, sourcePath)
	}
	method := strings.TrimSpace(strings.ToUpper(loaded.Method))
	uri := strings.TrimSpace(loaded.URI)
	if method == "" && len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		method = strings.TrimSpace(strings.ToUpper(loaded.ComponentRoutes[0].Method))
	}
	if uri == "" && len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		uri = strings.TrimSpace(loaded.ComponentRoutes[0].RoutePath)
	}
	if method == "" {
		method = "GET"
	}
	if uri == "" {
		return nil, fmt.Errorf("missing shape component route for %s", sourcePath)
	}
	reportConfig := bootstrapReport(loaded)
	if reportConfig == nil && rootView != nil && rootView.Groupable && strings.EqualFold(method, "GET") {
		reportConfig = (&repository.Report{Enabled: true}).Normalize()
	}
	componentMeta := contract.Meta{}
	if len(loaded.ComponentRoutes) > 0 && loaded.ComponentRoutes[0] != nil {
		componentMeta.Name = strings.TrimSpace(loaded.ComponentRoutes[0].Name)
	}
	componentMCP := contract.ModelContextProtocol{}
	if loaded.Directives != nil && loaded.Directives.MCP != nil {
		componentMeta.Name = strings.TrimSpace(loaded.Directives.MCP.Name)
		componentMeta.Description = strings.TrimSpace(loaded.Directives.MCP.Description)
		componentMeta.DescriptionURI = strings.TrimSpace(loaded.Directives.MCP.DescriptionPath)
		componentMCP.MCPTool = true
	}
	var outputType reflect.Type
	if shouldMaterializeBootstrapOutputType(loaded, rootView) {
		pkgPath := bootstrapTypePackage(loaded)
		lookupType := componentArtifact.Resource.LookupType()
		outputType, err = loaded.OutputReflectType(pkgPath, lookupType)
		if err != nil {
			return nil, fmt.Errorf("failed to materialize bootstrap output type for %s: %w", sourcePath, err)
		}
	}
	componentModel := &repository.Component{
		Meta:                 componentMeta,
		ModelContextProtocol: componentMCP,
		Path: contract.Path{
			Method: method,
			URI:    uri,
		},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{
					Parameters: loaded.InputParameters(),
				},
			},
			Output: contract.Output{
				CaseFormat:  bootstrapOutputCaseFormat(loaded),
				Cardinality: bootstrapOutputCardinality(loaded, rootView),
				Type: state.Type{
					Parameters: loaded.OutputParameters(),
				},
			},
			Service: defaultServiceForMethod(method, rootView),
		},
		View:        rootView,
		Report:      reportConfig,
		TypeContext: loaded.TypeContext,
	}
	if outputType != nil {
		if componentModel.Contract.Output.Type.Schema == nil {
			componentModel.Contract.Output.Type.Schema = state.NewSchema(nil)
		}
		componentModel.Contract.Output.Type.SetType(outputType)
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
	mergeBootstrapViewMetadata(components.Resource, bootstrapMetadata)
	if err = components.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize bootstrap component for %s: %w", sourcePath, err)
	}
	if len(components.Components) == 0 || components.Components[0] == nil {
		return nil, fmt.Errorf("empty initialized bootstrap component for %s", sourcePath)
	}
	mergeBootstrapView(components.Components[0].View, lookupRootView(bootstrapMetadata, loaded.RootView))
	return components.Components[0], nil
}

func bootstrapTypePackage(component *shapeLoad.Component) string {
	if component == nil || component.TypeContext == nil {
		return ""
	}
	if pkgPath := strings.TrimSpace(component.TypeContext.PackagePath); pkgPath != "" {
		return pkgPath
	}
	return strings.TrimSpace(component.TypeContext.DefaultPackage)
}

func shouldMaterializeBootstrapOutputType(component *shapeLoad.Component, rootView *view.View) bool {
	if component == nil || rootView == nil || rootView.Schema == nil || rootView.Schema.Cardinality != state.One {
		return false
	}
	for _, item := range component.Output {
		if item == nil || item.In == nil || item.In.Kind != state.KindOutput || item.In.Name != "view" {
			continue
		}
		if !strings.Contains(item.Tag, "anonymous") || item.Schema == nil {
			return false
		}
		return item.Schema.Cardinality == state.One
	}
	return false
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
	if strings.EqualFold(method, "GET") && rootView != nil {
		return datlyservice.TypeReader
	}
	if rootView != nil && rootView.Mode == view.ModeQuery {
		return datlyservice.TypeReader
	}
	return datlyservice.TypeExecutor
}

func bootstrapRequiresRootView(component *shapeLoad.Component) bool {
	if component == nil {
		return false
	}
	if strings.TrimSpace(component.RootView) != "" {
		return true
	}
	if len(component.Views) > 0 {
		return true
	}
	return component.Report != nil && component.Report.Enabled
}

func bootstrapHandlerView(resource *view.Resource, component *shapeLoad.Component, fallbackName string) *view.View {
	if component == nil {
		return nil
	}
	name := strings.TrimSpace(component.RootView)
	if name == "" {
		name = strings.TrimSpace(component.Name)
	}
	if name == "" {
		name = strings.TrimSpace(fallbackName)
	}
	if name == "" {
		name = "handler"
	}
	ret := &view.View{
		Name: name,
		Mode: view.ModeHandler,
	}
	if resource != nil && len(resource.Connectors) > 0 && resource.Connectors[0] != nil {
		ret.Connector = &view.Connector{
			Connection: view.Connection{
				DBConfig: view.DBConfig{
					Reference: shared.Reference{Ref: resource.Connectors[0].Name},
				},
			},
		}
	}
	if output := component.OutputParameters(); len(output) > 0 {
		if holder := output.LookupByLocation(state.KindOutput, "view"); holder != nil && holder.Schema != nil {
			ret.Schema = holder.Schema.Clone()
		}
	}
	return ret
}

func normalizeBootstrapTemplateParameters(resource *view.Resource) {
	if resource == nil || len(resource.Parameters) == 0 {
		return
	}
	params := make(state.Parameters, 0, len(resource.Parameters))
	for _, param := range resource.Parameters {
		if param == nil || param.In == nil {
			continue
		}
		switch param.In.Kind {
		case state.KindOutput, state.KindMeta, state.KindAsync:
			continue
		}
		params = append(params, param)
	}
	if len(params) == 0 {
		return
	}
	for _, item := range resource.Views {
		normalizeBootstrapViewTemplate(item, params)
	}
}

func normalizeBootstrapViewTemplate(aView *view.View, params state.Parameters) {
	if aView == nil {
		return
	}
	if aView.Template != nil {
		seen := map[string]bool{}
		for _, item := range aView.Template.Parameters {
			if item == nil || strings.TrimSpace(item.Name) == "" {
				continue
			}
			seen[strings.ToLower(strings.TrimSpace(item.Name))] = true
		}
		for _, param := range params {
			if param == nil || param.In == nil || strings.TrimSpace(param.Name) == "" {
				continue
			}
			if param.In.Kind == state.KindView && strings.EqualFold(strings.TrimSpace(param.In.Name), strings.TrimSpace(aView.Name)) {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(param.Name))
			if seen[key] {
				continue
			}
			aView.Template.Parameters = append(aView.Template.Parameters, param)
			seen[key] = true
		}
		if len(aView.Template.Parameters) > 0 {
			aView.Template.UseParameterStateType = true
			aView.Template.UseResourceParameterLookup = true
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		normalizeBootstrapViewTemplate(&rel.Of.View, params)
	}
}

func normalizeBootstrapViewAliases(resource *view.Resource) {
	if resource == nil || len(resource.Parameters) == 0 {
		return
	}
	existing := resource.Views.Index()
	for _, param := range resource.Parameters {
		if param == nil || param.In == nil || param.In.Kind != state.KindView || param.Schema == nil {
			continue
		}
		alias := strings.TrimSpace(param.Schema.Name)
		if alias == "" {
			continue
		}
		if _, err := existing.Lookup(alias); err == nil {
			continue
		}
		sourceName := strings.TrimSpace(param.In.Name)
		if sourceName == "" {
			sourceName = strings.TrimSpace(param.Name)
		}
		sourceView, err := existing.Lookup(sourceName)
		if err != nil || sourceView == nil {
			continue
		}
		cloned := *sourceView
		cloned.Name = alias
		cloned.Ref = ""
		resource.Views = append(resource.Views, &cloned)
		existing.Register(&cloned)
	}
}

func normalizeBootstrapCaches(resource *view.Resource) {
	if resource == nil {
		return
	}
	available := map[string]bool{}
	for _, provider := range resource.CacheProviders {
		if provider == nil || strings.TrimSpace(provider.Name) == "" {
			continue
		}
		available[strings.ToLower(strings.TrimSpace(provider.Name))] = true
	}
	for _, item := range resource.Views {
		normalizeBootstrapViewCache(item, available)
	}
}

func normalizeBootstrapViewCache(aView *view.View, available map[string]bool) {
	if aView == nil {
		return
	}
	if aView.Cache != nil {
		ref := strings.ToLower(strings.TrimSpace(aView.Cache.Ref))
		if ref != "" && !available[ref] {
			aView.Cache = nil
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		normalizeBootstrapViewCache(&rel.Of.View, available)
	}
}

func hasRepositoryProvider(ctx context.Context, repo *repository.Service, path *contract.Path) (bool, error) {
	if repo == nil || repo.Registry() == nil || path == nil {
		return false, nil
	}
	_, err := repo.Registry().LookupProvider(ctx, path)
	if err != nil {
		message := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(message, "not found") || strings.Contains(message, "couldn't match uri") {
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
	if hasNonFileScheme(pattern) && !hasGlobMeta(pattern) {
		return []string{pattern}, nil
	}
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
		if hasNonFileScheme(item) {
			exists, err := bootstrapFS.Exists(context.Background(), item)
			if err != nil {
				return nil, err
			}
			if exists {
				result = append(result, item)
			}
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

func hasNonFileScheme(URL string) bool {
	scheme := furl.Scheme(URL, "file")
	return scheme != "file"
}

func bootstrapSourceName(sourcePath string) string {
	_, URLPath := furl.Base(sourcePath, "file")
	baseName := path.Base(URLPath)
	ext := path.Ext(baseName)
	return strings.TrimSuffix(baseName, ext)
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

func bootstrapOutputCardinality(component *shapeLoad.Component, rootView *view.View) state.Cardinality {
	if component != nil {
		if output := component.OutputParameters(); len(output) > 0 {
			if parameter := output.LookupByLocation(state.KindOutput, "view"); parameter != nil && parameter.Schema != nil && parameter.Schema.Cardinality != "" {
				return parameter.Schema.Cardinality
			}
		}
	}
	if rootView != nil && rootView.Schema != nil && rootView.Schema.Cardinality != "" {
		return rootView.Schema.Cardinality
	}
	return ""
}

func bootstrapOutputCaseFormat(component *shapeLoad.Component) text.CaseFormat {
	if component != nil && component.Directives != nil {
		if value := strings.TrimSpace(component.Directives.CaseFormat); value != "" {
			return text.CaseFormat(value)
		}
	}
	return text.CaseFormatLowerCamel
}

func mergeBootstrapViewMetadata(target, source *view.Resource) {
	if target == nil || source == nil {
		return
	}
	sourceViews := source.Views.Index()
	for _, candidate := range target.Views {
		if candidate == nil {
			continue
		}
		original, _ := sourceViews.Lookup(candidate.Name)
		if original == nil {
			continue
		}
		mergeBootstrapView(candidate, original)
	}
}

func mergeBootstrapView(target, source *view.View) {
	if target == nil || source == nil {
		return
	}
	if source.AllowNulls != nil {
		value := *source.AllowNulls
		target.AllowNulls = &value
	}
	if source.Groupable {
		target.Groupable = true
	}
	if source.Selector != nil {
		target.Selector = source.Selector
	}
	if len(source.ColumnsConfig) > 0 {
		target.ColumnsConfig = map[string]*view.ColumnConfig{}
		for key, cfg := range source.ColumnsConfig {
			if cfg == nil {
				continue
			}
			cloned := *cfg
			if cfg.DataType != nil {
				value := *cfg.DataType
				cloned.DataType = &value
			}
			if cfg.Tag != nil {
				value := *cfg.Tag
				cloned.Tag = &value
			}
			if cfg.Groupable != nil {
				value := *cfg.Groupable
				cloned.Groupable = &value
			}
			target.ColumnsConfig[key] = &cloned
		}
	}
	mergeBootstrapColumns(target, source)
}

func snapshotBootstrapViewMetadata(resource *view.Resource) *view.Resource {
	if resource == nil {
		return nil
	}
	result := &view.Resource{}
	for _, item := range resource.Views {
		if item == nil {
			continue
		}
		cloned := &view.View{
			Name:      item.Name,
			Groupable: item.Groupable,
		}
		cloned.Reference.Ref = item.Ref
		if item.AllowNulls != nil {
			value := *item.AllowNulls
			cloned.AllowNulls = &value
		}
		if item.Selector != nil {
			cloned.Selector = item.Selector
		}
		if len(item.ColumnsConfig) > 0 {
			cloned.ColumnsConfig = map[string]*view.ColumnConfig{}
			for key, cfg := range item.ColumnsConfig {
				if cfg == nil {
					continue
				}
				copied := *cfg
				if cfg.DataType != nil {
					value := *cfg.DataType
					copied.DataType = &value
				}
				if cfg.Tag != nil {
					value := *cfg.Tag
					copied.Tag = &value
				}
				if cfg.Groupable != nil {
					value := *cfg.Groupable
					copied.Groupable = &value
				}
				cloned.ColumnsConfig[key] = &copied
			}
		}
		if len(item.Columns) > 0 {
			cloned.Columns = make([]*view.Column, 0, len(item.Columns))
			for _, column := range item.Columns {
				if column == nil {
					continue
				}
				copied := *column
				cloned.Columns = append(cloned.Columns, &copied)
			}
		}
		result.Views = append(result.Views, cloned)
	}
	return result
}

func mergeBootstrapColumns(target, source *view.View) {
	if target == nil || source == nil || len(target.Columns) == 0 || len(source.Columns) == 0 {
		return
	}
	sourceColumns := map[string]*view.Column{}
	for _, column := range source.Columns {
		if column == nil {
			continue
		}
		for _, key := range []string{column.Name, column.DatabaseColumn, column.FieldName()} {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			sourceColumns[strings.ToLower(key)] = column
		}
	}
	for _, column := range target.Columns {
		if column == nil {
			continue
		}
		for _, key := range []string{column.Name, column.DatabaseColumn, column.FieldName()} {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if sourceColumn, ok := sourceColumns[strings.ToLower(key)]; ok {
				if sourceColumn.Groupable {
					column.Groupable = true
				}
				if sourceColumn.Aggregate {
					column.Aggregate = true
				}
				if strings.TrimSpace(column.Tag) == "" {
					column.Tag = sourceColumn.Tag
				}
				break
			}
		}
	}
}
