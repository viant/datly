package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/shape"
	shapeColumn "github.com/viant/datly/repository/shape/column"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/xgen"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
)

func (s *Service) Transcribe(ctx context.Context, opts *options.Options) error {
	transcribe := opts.Transcribe
	if transcribe == nil {
		return fmt.Errorf("transcribe options not set")
	}
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	var sources []string
	for _, sourceURL := range transcribe.Source {
		_, name := url.Split(sourceURL, file.Scheme)
		dql, err := s.readSource(ctx, sourceURL)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", sourceURL, err)
		}
		shapeSource := &shape.Source{
			Name:      strings.TrimSuffix(name, path.Ext(name)),
			Path:      url.Path(sourceURL),
			DQL:       strings.TrimSpace(dql),
			Connector: transcribe.DefaultConnectorName(),
		}
		planResult, err := compiler.Compile(ctx, shapeSource, transcribeCompileOptions(transcribe)...)
		if err != nil {
			return fmt.Errorf("failed to compile %s: %w", sourceURL, err)
		}
		componentArtifact, err := loader.LoadComponent(ctx, planResult, shape.WithLoadTypeContextPackages(true))
		if err != nil {
			return fmt.Errorf("failed to load %s: %w", sourceURL, err)
		}
		component, ok := shapeLoad.ComponentFrom(componentArtifact)
		if !ok {
			return fmt.Errorf("unexpected component artifact for %s", sourceURL)
		}
		if componentArtifact.Resource != nil && len(transcribe.Connectors) > 0 {
			applyConnectorsToResource(componentArtifact.Resource, transcribe.Connectors)
			discoverColumns(ctx, componentArtifact.Resource)
			shapeLoad.RefineSummarySchemas(componentArtifact.Resource)
		}
		prepareResourceForTranscribeCodegen(componentArtifact.Resource, component)
		codegenResult, err := s.generateTranscribeTypes(sourceURL, dql, transcribe, componentArtifact.Resource, component)
		if err != nil {
			return err
		}
		if codegenResult != nil {
			alignGeneratedPackageAliases(componentArtifact.Resource, component, codegenResult.PackageDir, codegenResult.PackagePath, codegenResult.PackageName)
		}
		if !transcribe.SkipYAML {
			if err = s.persistTranscribeRoute(ctx, transcribe, sourceURL, dql, componentArtifact.Resource, component, codegenResult); err != nil {
				return err
			}
		}
		sources = append(sources, filepath.Clean(url.Path(sourceURL)))
	}
	return s.persistTranscribeDependencies(ctx, transcribe, sources)
}

func (s *Service) persistTranscribeDependencies(ctx context.Context, transcribe *options.Transcribe, sources []string) error {
	depURL := url.Join(transcribe.Repository, "Datly", "dependencies")
	depURL = url.Normalize(depURL, file.Scheme)
	if len(transcribe.Connectors) > 0 {
		var connectors []connEntry
		for _, c := range transcribe.Connectors {
			parts := strings.SplitN(c, "|", 4)
			if len(parts) >= 3 {
				connectors = append(connectors, connEntry{Name: parts[0], Driver: parts[1], DSN: parts[2]})
			}
		}
		if len(connectors) > 0 {
			connURL := url.Join(depURL, "connectors.yaml")
			existing := loadExistingConnectors(ctx, s.fs, connURL)
			merged := mergeConnectors(existing, connectors)
			connMap := map[string]any{"Connectors": merged}
			data, err := yaml.Marshal(connMap)
			if err != nil {
				return err
			}
			if err = s.fs.Upload(ctx, connURL, file.DefaultFileOsMode, strings.NewReader(string(data))); err != nil {
				return fmt.Errorf("failed to persist connections: %w", err)
			}
		}
	}

	cfgURL := url.Join(transcribe.Repository, "Datly", "config.json")
	cfg := s.seedTranscribeConfig(ctx, transcribe)
	if cfg.SyncFrequencyMs == 0 {
		cfg.SyncFrequencyMs = 2000
	}
	cfg.Meta.Init()
	if cfg.Meta.StatusURI == "" {
		cfg.Meta.StatusURI = "/v1/api/status"
	}
	payload := map[string]any{
		"APIPrefix":       cfg.APIPrefix,
		"DependencyURL":   depURL,
		"Endpoint":        map[string]any{"Port": 8080},
		"SyncFrequencyMs": cfg.SyncFrequencyMs,
		"Meta":            cfg.Meta,
	}
	if transcribe.APIPrefix != "" {
		payload["APIPrefix"] = transcribe.APIPrefix
	}
	if len(cfg.APIKeys) > 0 {
		payload["APIKeys"] = cfg.APIKeys
	}
	if cfg.JWTValidator != nil {
		payload["JWTValidator"] = cfg.JWTValidator
	}
	if cfg.JwtSigner != nil {
		payload["JwtSigner"] = cfg.JwtSigner
	}
	if transcribe.SkipYAML {
		payload["DQLBootstrap"] = map[string]any{"Sources": mergeStrings(existingBootstrapSources(ctx, s.fs, cfgURL), sources)}
	} else {
		payload["RouteURL"] = url.Normalize(url.Join(transcribe.Repository, "Datly", "routes"), file.Scheme)
	}
	cfgData, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err = s.fs.Upload(ctx, cfgURL, file.DefaultFileOsMode, strings.NewReader(string(cfgData))); err != nil {
		return fmt.Errorf("failed to persist config: %w", err)
	}
	return nil
}

func (s *Service) seedTranscribeConfig(ctx context.Context, transcribe *options.Transcribe) *gateway.Config {
	seed := &gateway.Config{}
	projectCfg := filepath.Join(transcribe.Project, "config.json")
	if data, err := s.fs.DownloadWithURL(ctx, projectCfg); err == nil {
		_ = json.Unmarshal(data, seed)
	}
	applyAuth(seed, &transcribe.Auth)
	return seed
}

func applyAuth(cfg *gateway.Config, auth *options.Auth) {
	if cfg == nil || auth == nil {
		return
	}
	if strings.TrimSpace(auth.RSA) != "" {
		cfg.JWTValidator = &verifier.Config{RSA: getScyResources(auth.RSA)}
		cfg.JwtSigner = &signer.Config{RSA: getScyResource(strings.Split(auth.RSA, ";")[0])}
	}
	if strings.TrimSpace(auth.HMAC) != "" {
		cfg.JWTValidator = &verifier.Config{HMAC: getScyResource(auth.HMAC)}
		cfg.JwtSigner = &signer.Config{HMAC: getScyResource(auth.HMAC)}
	}
}

func getScyResource(location string) *scy.Resource {
	pair := strings.Split(location, "|")
	res := &scy.Resource{URL: pair[0]}
	if len(pair) > 1 {
		res.Key = pair[1]
	}
	res.URL = url.Normalize(res.URL, file.Scheme)
	return res
}

func getScyResources(location string) []*scy.Resource {
	var result []*scy.Resource
	for _, item := range strings.Split(location, "-") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		result = append(result, getScyResource(item))
	}
	return result
}

func existingBootstrapSources(ctx context.Context, fs afs.Service, cfgURL string) []string {
	data, err := fs.DownloadWithURL(ctx, cfgURL)
	if err != nil {
		return nil
	}
	cfg := &gateway.Config{}
	if err = json.Unmarshal(data, cfg); err != nil || cfg.DQLBootstrap == nil {
		return nil
	}
	return append([]string{}, cfg.DQLBootstrap.Sources...)
}

func mergeStrings(existing, incoming []string) []string {
	seen := map[string]bool{}
	var result []string
	for _, item := range append(existing, incoming...) {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		result = append(result, item)
	}
	return result
}

type connEntry struct {
	Name   string `yaml:"Name"`
	Driver string `yaml:"Driver"`
	DSN    string `yaml:"DSN"`
}

func loadExistingConnectors(ctx context.Context, fs afs.Service, connURL string) []connEntry {
	data, err := fs.DownloadWithURL(ctx, connURL)
	if err != nil {
		return nil
	}
	var doc struct {
		Connectors []connEntry `yaml:"Connectors"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil
	}
	return doc.Connectors
}

func mergeConnectors(existing, incoming []connEntry) []connEntry {
	byName := map[string]connEntry{}
	var order []string
	for _, c := range existing {
		if _, ok := byName[c.Name]; !ok {
			order = append(order, c.Name)
		}
		byName[c.Name] = c
	}
	for _, c := range incoming {
		if _, ok := byName[c.Name]; !ok {
			order = append(order, c.Name)
		}
		byName[c.Name] = c
	}
	result := make([]connEntry, 0, len(order))
	for _, name := range order {
		result = append(result, byName[name])
	}
	return result
}

func (s *Service) readSource(ctx context.Context, sourceURL string) (string, error) {
	payload, err := s.fs.DownloadWithURL(ctx, sourceURL)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func (s *Service) persistTranscribeRoute(ctx context.Context, transcribe *options.Transcribe, sourceURL, dql string, resource *view.Resource, component *shapeLoad.Component, codegenResult *xgen.ComponentCodegenResult) error {
	sourcePath := filepath.Clean(url.Path(sourceURL))
	stem := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	routeRoot := url.Join(transcribe.Repository, "Datly", "routes")
	routeYAML := url.Join(routeRoot, stem+".yaml")
	if err := s.applyGeneratedMutableArtifacts(ctx, routeRoot, resource, component, codegenResult); err != nil {
		return err
	}
	if resource != nil && codegenResult != nil && strings.TrimSpace(codegenResult.VeltyFilePath) != "" {
		if source, err := os.ReadFile(filepath.Clean(codegenResult.VeltyFilePath)); err == nil {
			rootView := ""
			if component != nil {
				rootView = strings.TrimSpace(component.RootView)
			}
			root := lookupNamedView(resource, rootView)
			if root == nil && len(resource.Views) > 0 {
				root = resource.Views[0]
			}
			if root != nil {
				if root.Template == nil {
					root.Template = view.NewTemplate(stripLeadingRouteDirective(string(source)))
				} else {
					root.Template.Source = stripLeadingRouteDirective(string(source))
				}
				if rel, err := filepath.Rel(filepath.Clean(codegenResult.PackageDir), filepath.Clean(codegenResult.VeltyFilePath)); err == nil {
					root.Template.SourceURL = filepath.ToSlash(rel)
				}
			}
		}
	}
	if resource != nil {
		normalizeResourceSchemaPackages(resource)
		for _, item := range resource.Views {
			if item == nil || item.Template == nil || strings.TrimSpace(item.Template.Source) == "" {
				continue
			}
			if strings.HasSuffix(strings.TrimSpace(item.Template.SourceURL), "/patch.sql") || strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(component.RootView)) {
				item.Template.Source = stripLeadingRouteDirective(item.Template.Source)
			}
			sqlRel := strings.TrimSpace(item.Template.SourceURL)
			if sqlRel == "" {
				sqlRel = path.Join(stem, item.Name+".sql")
			}
			sqlDest := path.Join(url.Path(routeRoot), filepath.ToSlash(sqlRel))
			if err := s.fs.Upload(ctx, sqlDest, file.DefaultFileOsMode, strings.NewReader(item.Template.Source)); err != nil {
				return fmt.Errorf("failed to persist sql %s: %w", sqlDest, err)
			}
			item.Template.SourceURL = sqlRel
		}
	}
	rootView := ""
	if component != nil {
		rootView = strings.TrimSpace(component.RootView)
	}
	if rootView == "" && resource != nil && len(resource.Views) > 0 && resource.Views[0] != nil {
		rootView = resource.Views[0].Name
	}
	method, uri := transcribeRulePath(dql, stem, transcribe.APIPrefix, component)
	routeView := &view.View{
		Reference: shared.Reference{Ref: rootView},
		Name:      rootView,
	}
	if root := lookupNamedView(resource, rootView); root != nil {
		viewCopy := *root
		routeView = &viewCopy
		routeView.Reference = shared.Reference{Ref: rootView}
	}
	route := &repository.Component{
		Path: contract.Path{
			Method: method,
			URI:    uri,
		},
		Contract: contract.Contract{
			Service: serviceTypeForMethod(method),
			Output: contract.Output{
				CaseFormat: text.CaseFormatLowerCamel,
			},
		},
		View: routeView,
	}
	if root := lookupNamedView(resource, rootView); root != nil && root.Connector != nil {
		ref := strings.TrimSpace(root.Connector.Ref)
		if ref == "" {
			ref = strings.TrimSpace(root.Connector.Name)
		}
		if ref != "" {
			route.View.Connector = view.NewRefConnector(ref)
			route.View.Connector.Name = ref
		}
	}
	if component != nil {
		route.TypeContext = component.TypeContext
		if output := component.OutputParameters(); len(output) > 0 {
			route.Contract.Output = contract.Output{
				Cardinality: state.Many,
				CaseFormat:  text.CaseFormatLowerCamel,
				Type: state.Type{
					Parameters: output,
				},
			}
		}
		if component.Directives != nil && component.Directives.MCP != nil {
			route.Name = strings.TrimSpace(component.Directives.MCP.Name)
			route.Description = strings.TrimSpace(component.Directives.MCP.Description)
			route.DescriptionURI = strings.TrimSpace(component.Directives.MCP.DescriptionPath)
		}
	}
	if component != nil && (len(component.Input) > 0 || len(component.Meta) > 0) {
		params := transcribeInputParameters(component, resource)
		if len(params) > 0 {
			normalizeParameterSchemas(params)
			route.Contract.Input.Type.Parameters = normalizeParameterTypeNameTags(params)
		}
	}
	if component != nil {
		normalizeComponentStateSchemas(component)
	}
	payload := &shapeRuleFile{
		Routes:   []*repository.Component{route},
		Resource: sanitizeResourceForRouteYAML(resource),
		With:     transcribeSharedResourceRefs(resource),
	}
	if payload.Resource != nil {
		normalizeResourceSchemaPackages(payload.Resource)
		promoteAnonymousParameterTypeDefinitions(payload.Resource)
		backfillResourceColumnDataTypes(payload.Resource)
		canonicalizeResourceTypeDefinitions(payload.Resource)
	}
	if len(payload.Routes) > 0 {
		if payload.Resource != nil && payload.Routes[0] != nil && payload.Routes[0].View != nil {
			alignViewParameterSchemasToResourceTypes(payload.Routes[0].View, payload.Resource)
		}
		normalizeParameterSchemas(payload.Routes[0].Contract.Input.Type.Parameters)
		normalizeParameterSchemas(payload.Routes[0].Contract.Output.Type.Parameters)
	}
	if component != nil && component.TypeContext != nil {
		payload.TypeContext = component.TypeContext
	}
	if payload.Resource != nil && codegenResult != nil && strings.TrimSpace(codegenResult.VeltyFilePath) != "" {
		if source, err := os.ReadFile(filepath.Clean(codegenResult.VeltyFilePath)); err == nil {
			rootView := ""
			if component != nil {
				rootView = strings.TrimSpace(component.RootView)
			}
			root := lookupNamedView(payload.Resource, rootView)
			if root == nil && len(payload.Resource.Views) > 0 {
				root = payload.Resource.Views[0]
			}
			if root != nil {
				if root.Template == nil {
					root.Template = view.NewTemplate(stripLeadingRouteDirective(string(source)))
				} else {
					root.Template.Source = stripLeadingRouteDirective(string(source))
				}
				if rel, err := filepath.Rel(filepath.Clean(codegenResult.PackageDir), filepath.Clean(codegenResult.VeltyFilePath)); err == nil {
					root.Template.SourceURL = filepath.ToSlash(rel)
				}
			}
		}
	}
	data, err := yaml.Marshal(payload)
	if err != nil {
		return err
	}
	data, err = ensureSharedResourceRefsYAML(data, payload.With)
	if err != nil {
		return err
	}
	data, err = normalizeConnectorRefsYAML(data)
	if err != nil {
		return err
	}
	data, err = normalizeRouteViewRefsYAML(data)
	if err != nil {
		return err
	}
	data, err = normalizeRouteComponentEmbeddingYAML(data)
	if err != nil {
		return err
	}
	if err = s.fs.Upload(ctx, routeYAML, file.DefaultFileOsMode, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("failed to persist route yaml %s: %w", routeYAML, err)
	}
	return nil
}

func transcribeSharedResourceRefs(resource *view.Resource) []string {
	if resource == nil {
		return nil
	}
	var result []string
	if len(collectResourceConnectorRefs(resource)) > 0 {
		result = append(result, view.ResourceConnectors)
	}
	if len(resource.CacheProviders) > 0 {
		result = append(result, "cache")
	}
	return result
}

func (s *Service) generateTranscribeTypes(sourceAbsPath, dql string, transcribe *options.Transcribe, resource *view.Resource, component *shapeLoad.Component) (*xgen.ComponentCodegenResult, error) {
	if component == nil || component.TypeContext == nil || resource == nil {
		return nil, nil
	}
	ctx := component.TypeContext
	projectDir := findProjectDir(sourceAbsPath)
	if projectDir == "" {
		projectDir = transcribe.Project
	}
	codegen := &xgen.ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    !transcribe.SkipYAML,
		WithContract: true,
	}
	if pkgPath, pkgDir, pkgName := resolvedTranscribeTypeOutput(projectDir, ctx.PackagePath); pkgPath != "" {
		codegen.PackagePath = pkgPath
		codegen.PackageDir = pkgDir
		codegen.PackageName = pkgName
	}
	if method, uri := resolvedTranscribeRoute(sourceAbsPath, dql, transcribe.APIPrefix); uri != "" {
		component.Method = method
		component.URI = uri
	}
	return codegen.Generate()
}

func (s *Service) applyGeneratedMutableArtifacts(ctx context.Context, routeRoot string, resource *view.Resource, component *shapeLoad.Component, codegenResult *xgen.ComponentCodegenResult) error {
	if resource == nil || component == nil || codegenResult == nil || codegenResult.PackageDir == "" {
		return nil
	}
	uploaded := map[string]string{}
	packageDir := filepath.Clean(codegenResult.PackageDir)
	for _, generated := range codegenResult.GeneratedFiles {
		if strings.TrimSpace(generated) == "" || !strings.HasSuffix(strings.TrimSpace(generated), ".sql") {
			continue
		}
		absPath := filepath.Clean(generated)
		rel, err := filepath.Rel(packageDir, absPath)
		if err != nil {
			continue
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "../") {
			continue
		}
		data, err := os.ReadFile(absPath)
		if err != nil {
			return fmt.Errorf("failed to read generated sql %s: %w", absPath, err)
		}
		content := string(data)
		if strings.TrimSpace(codegenResult.VeltyFilePath) != "" && filepath.Clean(codegenResult.VeltyFilePath) == absPath {
			content = stripLeadingRouteDirective(content)
		}
		dest := path.Join(url.Path(routeRoot), rel)
		if err = s.fs.Upload(ctx, dest, file.DefaultFileOsMode, strings.NewReader(content)); err != nil {
			return fmt.Errorf("failed to persist generated sql %s: %w", dest, err)
		}
		uploaded[rel] = content
	}
	if len(uploaded) == 0 {
		return nil
	}
	rootView := ""
	if component != nil {
		rootView = strings.TrimSpace(component.RootView)
	}
	root := lookupNamedView(resource, rootView)
	if root == nil && resource != nil && len(resource.Views) > 0 {
		root = resource.Views[0]
	}
	if root != nil && strings.TrimSpace(codegenResult.VeltyFilePath) != "" {
		if rel, err := filepath.Rel(packageDir, filepath.Clean(codegenResult.VeltyFilePath)); err == nil {
			rel = filepath.ToSlash(rel)
			if source, ok := uploaded[rel]; ok {
				if root.Template == nil {
					root.Template = view.NewTemplate(source)
				} else {
					root.Template.Source = source
				}
				root.Template.SourceURL = rel
				preserveTemplateParameters(root, component.InputParameters())
			}
		}
	}
	for _, item := range resource.Views {
		if item == nil || item.Template == nil {
			continue
		}
		rel := strings.TrimSpace(item.Template.SourceURL)
		if item.Template.DeclaredParametersOnly {
			item.Template.Parameters = append(state.Parameters{}, resource.Parameters.UsedBy(item.Template.Source)...)
		} else {
			preserveTemplateParameters(item, resource.Parameters.UsedBy(item.Template.Source))
			preserveTemplateParameters(item, dependentTemplateParameters(item.Template.Parameters, resource.Parameters))
		}
		if len(item.Template.Parameters) > 0 {
			item.Template.UseParameterStateType = true
		}
		if rel == "" {
			continue
		}
		if source, ok := uploaded[rel]; ok {
			item.Template.Source = source
		}
	}
	return nil
}

func preserveTemplateParameters(aView *view.View, params state.Parameters) {
	if aView == nil || aView.Template == nil || len(params) == 0 {
		return
	}
	if aView.Template.DeclaredParametersOnly {
		return
	}
	seen := map[string]bool{}
	for _, item := range aView.Template.Parameters {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		seen[strings.ToLower(strings.TrimSpace(item.Name))] = true
	}
	for _, param := range params {
		if param == nil || strings.TrimSpace(param.Name) == "" {
			continue
		}
		switch param.In.Kind {
		case state.KindOutput, state.KindMeta, state.KindAsync:
			continue
		}
		key := strings.ToLower(strings.TrimSpace(param.Name))
		if seen[key] {
			continue
		}
		aView.Template.Parameters = append(aView.Template.Parameters, param)
		seen[key] = true
	}
}

func transcribeInputParameters(component *shapeLoad.Component, resource *view.Resource) state.Parameters {
	if component == nil {
		return nil
	}
	params := make(state.Parameters, 0, len(component.Input)+len(component.Meta)+4)
	seen := map[string]bool{}
	declared := map[string]bool{}
	appendParam := func(param *state.Parameter) {
		if param == nil {
			return
		}
		key := strings.ToLower(strings.TrimSpace(param.Name))
		if key == "" || seen[key] {
			return
		}
		cloned := *param
		if param.Schema != nil {
			cloned.Schema = param.Schema.Clone()
		}
		if param.Output != nil {
			output := *param.Output
			if param.Output.Schema != nil {
				output.Schema = param.Output.Schema.Clone()
			}
			cloned.Output = &output
		}
		params = append(params, &cloned)
		seen[key] = true
	}
	for _, item := range component.Input {
		if item != nil {
			if name := strings.ToLower(strings.TrimSpace(item.Name)); name != "" {
				declared[name] = true
			}
			appendParam(&item.Parameter)
		}
	}
	rootView := lookupNamedView(resource, strings.TrimSpace(component.RootView))
	if rootView != nil && rootView.Template != nil {
		for _, item := range rootView.Template.Parameters {
			if item == nil {
				continue
			}
			if !declared[strings.ToLower(strings.TrimSpace(item.Name))] {
				continue
			}
			appendParam(item)
		}
	}
	for _, item := range component.Meta {
		if item != nil {
			if name := strings.ToLower(strings.TrimSpace(item.Name)); name != "" {
				declared[name] = true
			}
			appendParam(&item.Parameter)
		}
	}
	return params
}

func prepareResourceForTranscribeCodegen(resource *view.Resource, component *shapeLoad.Component) {
	if resource == nil || component == nil {
		return
	}
	rootView := ""
	if component != nil {
		rootView = strings.TrimSpace(component.RootView)
	}
	root := lookupNamedView(resource, rootView)
	if root == nil && len(resource.Views) > 0 {
		root = resource.Views[0]
	}
	if root != nil && root.Template != nil {
		preserveTemplateParameters(root, component.InputParameters())
		preserveTemplateParameters(root, resource.Parameters)
		if len(root.Template.Parameters) > 0 {
			root.Template.UseParameterStateType = true
		}
	}
	for _, item := range resource.Views {
		if item == nil || item.Template == nil {
			continue
		}
		if item.Template.DeclaredParametersOnly {
			item.Template.Parameters = append(state.Parameters{}, resource.Parameters.UsedBy(item.Template.Source)...)
		} else {
			preserveTemplateParameters(item, resource.Parameters.UsedBy(item.Template.Source))
			preserveTemplateParameters(item, dependentTemplateParameters(item.Template.Parameters, resource.Parameters))
		}
		if len(item.Template.Parameters) > 0 {
			item.Template.UseParameterStateType = true
		}
	}
}

func dependentTemplateParameters(params state.Parameters, resourceParams state.Parameters) state.Parameters {
	if len(params) == 0 || len(resourceParams) == 0 {
		return nil
	}
	seen := map[string]bool{}
	result := make(state.Parameters, 0)
	for _, param := range params {
		if param == nil || param.In == nil {
			continue
		}
		switch param.In.Kind {
		case state.KindParam:
			name := strings.TrimSpace(param.In.Name)
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			if seen[key] {
				continue
			}
			if dep := resourceParams.Lookup(name); dep != nil {
				result = append(result, dep)
				seen[key] = true
			}
		}
	}
	return result
}

func stripLeadingRouteDirective(content string) string {
	trimmed := strings.TrimSpace(content)
	if !strings.HasPrefix(trimmed, "/*") {
		return content
	}
	end := strings.Index(trimmed, "*/")
	if end == -1 {
		return content
	}
	header := trimmed[2:end]
	if !strings.Contains(header, `"URI"`) || !strings.Contains(header, `"Method"`) {
		return content
	}
	return strings.TrimSpace(trimmed[end+2:]) + "\n"
}

func lookupNamedView(resource *view.Resource, name string) *view.View {
	if resource == nil || strings.TrimSpace(name) == "" {
		return nil
	}
	for _, item := range resource.Views {
		if item != nil && strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(name)) {
			return item
		}
	}
	return nil
}

var routeDirectivePattern = regexp.MustCompile(`\$route\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?`)

func resolvedTranscribeRoute(sourcePath, dql, apiPrefix string) (string, string) {
	matches := routeDirectivePattern.FindStringSubmatch(dql)
	if len(matches) > 0 {
		method := strings.ToUpper(strings.TrimSpace(matches[2]))
		if method == "" {
			method = "GET"
		}
		return method, strings.TrimSpace(matches[1])
	}
	stem := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	uri := "/" + strings.Trim(stem, "/")
	if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
		uri = strings.TrimRight(prefix, "/") + uri
	}
	return "GET", uri
}
func resolvedTranscribeTypeOutput(projectDir, packagePath string) (string, string, string) {
	projectDir = strings.TrimSpace(projectDir)
	packagePath = strings.TrimSpace(packagePath)
	if projectDir == "" || packagePath == "" {
		return "", "", ""
	}
	modulePath, err := transcribeModulePath(filepath.Join(projectDir, "go.mod"))
	if err != nil || modulePath == "" {
		return "", "", ""
	}
	prefix := strings.TrimRight(modulePath, "/") + "/"
	if !strings.HasPrefix(packagePath, prefix) {
		return "", "", ""
	}
	rel := strings.TrimPrefix(packagePath, prefix)
	rel = sanitizeTypeNamespace(rel)
	if rel == "" {
		return "", "", ""
	}
	pkgDir := filepath.Join(projectDir, filepath.FromSlash(rel))
	pkgName := filepath.Base(rel)
	return strings.TrimRight(modulePath, "/") + "/" + rel, pkgDir, pkgName
}

func transcribeModulePath(goModPath string) (string, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", fmt.Errorf("module path not found in %s", goModPath)
}
func transcribeCompileOptions(transcribe *options.Transcribe) []shape.CompileOption {
	var opts []shape.CompileOption
	if transcribe.Strict {
		opts = append(opts, shape.WithCompileStrict(true))
	}
	opts = append(opts, shape.WithLinkedTypes(false))
	namespace := strings.TrimSpace(transcribe.Namespace)
	module := strings.TrimSpace(transcribe.Module)
	typeOutput := strings.TrimSpace(transcribe.TypeOutput)
	if typeOutput == "" || typeOutput == "." {
		typeOutput = module
	}
	if namespace != "" {
		sanitizedNamespace := sanitizeTypeNamespace(namespace)
		pkgDir := filepath.Join(typeOutput, sanitizedNamespace)
		pkgName := filepath.Base(sanitizedNamespace)
		opts = append(opts, shape.WithTypeContextPackageDir(pkgDir))
		opts = append(opts, shape.WithTypeContextPackageName(pkgName))
	}
	return opts
}

func sanitizeTypeNamespace(namespace string) string {
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(namespace), "\\", "/"), "/")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		switch part {
		case "":
			continue
		case "vendor":
			part = "vendorsrc"
		default:
			part = sanitizeTypeNamespaceSegment(part)
		}
		parts[i] = part
	}
	return path.Join(parts...)
}

func sanitizeTypeNamespaceSegment(segment string) string {
	var b strings.Builder
	for _, r := range segment {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "generated"
	}
	ret := b.String()
	if ret[0] >= '0' && ret[0] <= '9' {
		return "p" + ret
	}
	return ret
}

func transcribeRulePath(_ string, ruleName, apiPrefix string, component *shapeLoad.Component) (string, string) {
	method := "GET"
	uri := "/" + strings.Trim(strings.TrimSpace(ruleName), "/")
	if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
		uri = strings.TrimRight(prefix, "/") + uri
	}
	if component != nil {
		if u := strings.TrimSpace(component.URI); u != "" {
			uri = u
		}
		if m := strings.TrimSpace(strings.ToUpper(component.Method)); m != "" {
			method = m
		}
	}
	return method, uri
}

func discoverColumns(ctx context.Context, resource *view.Resource) {
	if resource == nil {
		return
	}
	detector := shapeColumn.New()
	for _, aView := range resource.Views {
		if aView == nil {
			continue
		}
		columns, err := detector.Resolve(ctx, resource, aView)
		if err == nil && len(columns) > 0 {
			aView.Columns = columns
		}
	}
}

func applyConnectorsToResource(resource *view.Resource, connectors []string) {
	if resource == nil || len(connectors) == 0 {
		return
	}
	defaultName := ""
	for _, c := range connectors {
		parts := strings.SplitN(c, "|", 4)
		if len(parts) < 1 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		if name == "" {
			continue
		}
		if defaultName == "" {
			defaultName = name
		}
		if len(parts) >= 3 {
			resource.AddConnectors(view.NewConnector(name, strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])))
		}
	}
	if defaultName == "" {
		return
	}
	for _, v := range resource.Views {
		if v != nil && v.Connector == nil {
			v.Connector = view.NewRefConnector(defaultName)
		}
	}
}

func sanitizeResourceForRouteYAML(resource *view.Resource) *view.Resource {
	if resource == nil {
		return nil
	}
	cloned := *resource
	cloned.Parameters = normalizeParameterTypeNameTags(cloneParameters(resource.Parameters))
	if refs := collectResourceConnectorRefs(resource); len(refs) > 0 {
		cloned.Connectors = make([]*view.Connector, 0, len(refs))
		for _, ref := range refs {
			refConnector := view.NewRefConnector(ref)
			refConnector.Name = ref
			cloned.Connectors = append(cloned.Connectors, refConnector)
		}
	} else {
		cloned.Connectors = nil
	}
	if len(resource.Views) > 0 {
		cloned.Views = make(view.Views, 0, len(resource.Views))
		for _, item := range resource.Views {
			if item == nil {
				continue
			}
			viewCopy := *item
			if item.Connector != nil {
				ref := strings.TrimSpace(item.Connector.Ref)
				if ref == "" {
					ref = strings.TrimSpace(item.Connector.Name)
				}
				if ref != "" {
					refConnector := view.NewRefConnector(ref)
					refConnector.Name = ref
					viewCopy.Connector = refConnector
				} else {
					viewCopy.Connector = nil
				}
			}
			cloned.Views = append(cloned.Views, &viewCopy)
		}
	} else {
		cloned.Views = nil
	}
	return &cloned
}

func collectResourceConnectorRefs(resource *view.Resource) []string {
	if resource == nil {
		return nil
	}
	seen := map[string]bool{}
	var result []string
	appendRef := func(connector *view.Connector) {
		if connector == nil {
			return
		}
		ref := strings.TrimSpace(connector.Ref)
		if ref == "" {
			ref = strings.TrimSpace(connector.Name)
		}
		if ref == "" || seen[ref] {
			return
		}
		seen[ref] = true
		result = append(result, ref)
	}
	var visitView func(aView *view.View)
	visitView = func(aView *view.View) {
		if aView == nil {
			return
		}
		appendRef(aView.Connector)
		for _, rel := range aView.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			visitView(&rel.Of.View)
		}
	}
	for _, connector := range resource.Connectors {
		appendRef(connector)
	}
	for _, aView := range resource.Views {
		visitView(aView)
	}
	return result
}

func cloneParameters(params state.Parameters) state.Parameters {
	if len(params) == 0 {
		return nil
	}
	result := make(state.Parameters, 0, len(params))
	for _, item := range params {
		if item == nil {
			continue
		}
		cloned := *item
		if item.Schema != nil {
			cloned.Schema = item.Schema.Clone()
		}
		if item.Output != nil {
			output := *item.Output
			if item.Output.Schema != nil {
				output.Schema = item.Output.Schema.Clone()
			}
			cloned.Output = &output
		}
		result = append(result, &cloned)
	}
	return result
}

func normalizeParameterTypeNameTags(params state.Parameters) state.Parameters {
	if len(params) == 0 {
		return params
	}
	for _, item := range params {
		if item == nil || item.Schema == nil {
			continue
		}
		typeName := strings.TrimSpace(item.Schema.Name)
		if typeName == "" {
			continue
		}
		item.Tag = ensureTypeNameTag(item.Tag, typeName)
	}
	return params
}

func normalizeComponentStateSchemas(component *shapeLoad.Component) {
	if component == nil {
		return
	}
	for _, item := range component.Input {
		if item != nil {
			normalizeSchemaPackage(item.Schema)
		}
	}
	for _, item := range component.Output {
		if item != nil {
			normalizeSchemaPackage(item.Schema)
		}
	}
	for _, item := range component.Meta {
		if item != nil {
			normalizeSchemaPackage(item.Schema)
		}
	}
}

func normalizeResourceSchemaPackages(resource *view.Resource) {
	if resource == nil {
		return
	}
	normalizeParameterSchemas(resource.Parameters)
	for _, aView := range resource.Views {
		normalizeViewSchemaPackages(aView)
	}
	for _, item := range resource.Types {
		if item == nil {
			continue
		}
		item.Package = normalizedSchemaPackage(item.Package, item.ModulePath)
		for _, field := range item.Fields {
			if field == nil {
				continue
			}
			normalizeSchemaPackage(field.Schema)
		}
	}
}

func backfillResourceColumnDataTypes(resource *view.Resource) {
	if resource == nil {
		return
	}
	typeDefs := map[string]*view.TypeDefinition{}
	for _, item := range resource.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		typeDefs[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	var visitView func(aView *view.View)
	visitView = func(aView *view.View) {
		if aView == nil {
			return
		}
		backfillViewColumnDataTypes(aView, typeDefs)
		for _, rel := range aView.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			visitView(&rel.Of.View)
		}
	}
	for _, aView := range resource.Views {
		visitView(aView)
	}
}

func backfillViewColumnDataTypes(aView *view.View, defs map[string]*view.TypeDefinition) {
	if aView == nil || len(aView.Columns) == 0 || aView.Schema == nil {
		return
	}
	typeName := strings.TrimSpace(aView.Schema.Name)
	if typeName == "" {
		return
	}
	def := defs[strings.ToLower(typeName)]
	if def == nil {
		return
	}
	fieldTypes := map[string]string{}
	for _, field := range def.Fields {
		if field == nil || field.Schema == nil {
			continue
		}
		dataType := strings.TrimSpace(firstNonEmpty(field.Schema.DataType, field.Schema.Name))
		if dataType == "" {
			continue
		}
		for _, key := range []string{
			strings.ToUpper(strings.TrimSpace(field.Name)),
			strings.ToUpper(strings.TrimSpace(field.Column)),
			strings.ToUpper(strings.TrimSpace(field.FromName)),
		} {
			if key != "" {
				fieldTypes[key] = dataType
			}
		}
	}
	for _, column := range aView.Columns {
		if column == nil || strings.TrimSpace(column.DataType) != "" {
			continue
		}
		for _, key := range []string{
			strings.ToUpper(strings.TrimSpace(column.Name)),
			strings.ToUpper(strings.TrimSpace(column.DatabaseColumn)),
			strings.ToUpper(strings.TrimSpace(column.FieldName())),
		} {
			if dataType := strings.TrimSpace(fieldTypes[key]); dataType != "" {
				column.DataType = dataType
				break
			}
		}
	}
}

func canonicalizeResourceTypeDefinitions(resource *view.Resource) {
	if resource == nil {
		return
	}
	for _, def := range resource.Types {
		canonicalizeTypeDefinition(def)
	}
}

func promoteAnonymousParameterTypeDefinitions(resource *view.Resource) {
	if resource == nil {
		return
	}
	existing := map[string]bool{}
	for _, def := range resource.Types {
		if def == nil || strings.TrimSpace(def.Name) == "" {
			continue
		}
		existing[strings.ToLower(strings.TrimSpace(def.Name))] = true
	}
	promoted := map[string]string{}
	for _, param := range resource.Parameters {
		if param == nil || param.Schema == nil {
			continue
		}
		typeName := promotedParameterTypeName(param)
		if typeName == "" {
			continue
		}
		key := strings.ToLower(typeName)
		if !existing[key] {
			def := typeDefinitionFromAnonymousParameter(typeName, param)
			if def == nil {
				continue
			}
			resource.Types = append(resource.Types, def)
			existing[key] = true
		}
		promoted[strings.ToLower(strings.TrimSpace(param.Name))] = typeName
		rewritePromotedParameterSchema(param, typeName)
	}
	if len(promoted) == 0 {
		return
	}
	visitResourceParameters(resource, func(param *state.Parameter) {
		if param == nil || param.Schema == nil {
			return
		}
		typeName := promoted[strings.ToLower(strings.TrimSpace(param.Name))]
		if typeName == "" {
			return
		}
		rewritePromotedParameterSchema(param, typeName)
	})
}

func promotedParameterTypeName(param *state.Parameter) string {
	if param == nil || param.Schema == nil {
		return ""
	}
	if strings.TrimSpace(param.Name) == "" {
		return ""
	}
	if strings.TrimSpace(param.Schema.Name) != "" && !strings.Contains(strings.TrimSpace(param.Schema.Name), "struct {") {
		return ""
	}
	dataType := strings.TrimSpace(param.Schema.DataType)
	rType := param.Schema.Type()
	if !strings.Contains(dataType, "struct {") {
		if rType == nil {
			return ""
		}
		base := rType
		for base.Kind() == reflect.Ptr || base.Kind() == reflect.Slice || base.Kind() == reflect.Array {
			base = base.Elem()
		}
		if base.Kind() != reflect.Struct || base.Name() != "" {
			return ""
		}
	}
	return state.SanitizeTypeName(strings.TrimSpace(param.Name))
}

func typeDefinitionFromAnonymousParameter(typeName string, param *state.Parameter) *view.TypeDefinition {
	if param == nil || param.Schema == nil || typeName == "" {
		return nil
	}
	fields := typeDefinitionFieldsFromReflectType(param.Schema.Type())
	if len(fields) == 0 {
		return nil
	}
	for _, field := range fields {
		if field != nil {
			field.Tag = ""
		}
	}
	def := &view.TypeDefinition{Name: typeName, Fields: dedupeTypeDefinitionFields(fields)}
	canonicalizeTypeDefinition(def)
	return def
}

func rewritePromotedParameterSchema(param *state.Parameter, typeName string) {
	if param == nil || param.Schema == nil || typeName == "" {
		return
	}
	param.Schema.Name = typeName
	param.Schema.DataType = typeName
	param.Schema.Package = ""
	param.Schema.PackagePath = ""
	param.Schema.ModulePath = ""
}

func visitResourceParameters(resource *view.Resource, visitor func(param *state.Parameter)) {
	if resource == nil || visitor == nil {
		return
	}
	for _, param := range resource.Parameters {
		visitor(param)
	}
	var visitView func(aView *view.View)
	visitView = func(aView *view.View) {
		if aView == nil {
			return
		}
		if aView.Template != nil {
			for _, param := range aView.Template.Parameters {
				visitor(param)
			}
		}
		for _, rel := range aView.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			visitView(&rel.Of.View)
		}
	}
	for _, aView := range resource.Views {
		visitView(aView)
	}
}

func alignViewParameterSchemasToResourceTypes(aView *view.View, resource *view.Resource) {
	if aView == nil || resource == nil {
		return
	}
	typeNames := map[string]string{}
	for _, def := range resource.Types {
		if def == nil || strings.TrimSpace(def.Name) == "" {
			continue
		}
		typeNames[strings.ToLower(strings.TrimSpace(def.Name))] = strings.TrimSpace(def.Name)
	}
	var visitView func(current *view.View)
	visitView = func(current *view.View) {
		if current == nil {
			return
		}
		if current.Template != nil {
			for _, param := range current.Template.Parameters {
				if param == nil || param.Schema == nil {
					continue
				}
				typeName := typeNames[strings.ToLower(strings.TrimSpace(param.Name))]
				if typeName == "" {
					continue
				}
				rewritePromotedParameterSchema(param, typeName)
			}
		}
		for _, rel := range current.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			visitView(&rel.Of.View)
		}
	}
	visitView(aView)
}

func canonicalizeTypeDefinition(def *view.TypeDefinition) {
	if def == nil || len(def.Fields) == 0 {
		return
	}
	fields := dedupeTypeDefinitionFields(def.Fields)
	if len(fields) == 0 {
		return
	}
	def.DataType = inlineStructDataType(fields)
	def.Fields = nil
	def.Schema = nil
}

func dedupeTypeDefinitionFields(fields []*view.Field) []*view.Field {
	type keyedField struct {
		key   string
		field *view.Field
	}
	var ordered []keyedField
	index := map[string]int{}
	for _, field := range fields {
		if field == nil {
			continue
		}
		key := canonicalTypeFieldKey(field)
		if key == "" {
			continue
		}
		if pos, ok := index[key]; ok {
			merged := mergeTypeFields(ordered[pos].field, field)
			if preferTypeField(field, ordered[pos].field) {
				ordered[pos].field = merged
			} else {
				ordered[pos].field = merged
			}
			continue
		}
		index[key] = len(ordered)
		ordered = append(ordered, keyedField{key: key, field: cloneTypeField(field)})
	}
	result := make([]*view.Field, 0, len(ordered))
	for _, item := range ordered {
		if item.field != nil {
			item.field.Tag = sanitizeTypeFieldTag(item.field.Tag, item.field)
			result = append(result, item.field)
		}
	}
	return result
}

func mergeTypeFields(primary, secondary *view.Field) *view.Field {
	result := cloneTypeField(primary)
	if result == nil {
		return cloneTypeField(secondary)
	}
	if secondary == nil {
		return result
	}
	if strings.TrimSpace(result.Column) == "" {
		result.Column = strings.TrimSpace(secondary.Column)
	}
	if strings.TrimSpace(result.FromName) == "" {
		result.FromName = strings.TrimSpace(secondary.FromName)
	}
	if strings.TrimSpace(result.Tag) == "" {
		result.Tag = strings.TrimSpace(secondary.Tag)
	}
	if result.Schema == nil && secondary.Schema != nil {
		result.Schema = secondary.Schema.Clone()
	}
	return result
}

func cloneTypeField(field *view.Field) *view.Field {
	if field == nil {
		return nil
	}
	cloned := *field
	if field.Schema != nil {
		cloned.Schema = field.Schema.Clone()
	}
	return &cloned
}

func canonicalTypeFieldKey(field *view.Field) string {
	for _, candidate := range []string{
		strings.TrimSpace(field.Column),
		strings.TrimSpace(field.FromName),
		strings.TrimSpace(field.Name),
	} {
		if candidate != "" {
			return strings.ToUpper(candidate)
		}
	}
	return ""
}

func preferTypeField(candidate, existing *view.Field) bool {
	if existing == nil {
		return true
	}
	candidateScore := typeFieldPreferenceScore(candidate)
	existingScore := typeFieldPreferenceScore(existing)
	if candidateScore != existingScore {
		return candidateScore > existingScore
	}
	return strings.TrimSpace(candidate.Name) < strings.TrimSpace(existing.Name)
}

func typeFieldPreferenceScore(field *view.Field) int {
	if field == nil {
		return -1
	}
	score := 0
	name := strings.TrimSpace(field.Name)
	if name != "" && name != strings.ToUpper(name) {
		score += 10
	}
	if strings.TrimSpace(field.Column) == "" {
		score += 3
	}
	if strings.TrimSpace(field.FromName) == name {
		score += 2
	}
	if strings.EqualFold(name, "Has") {
		score += 5
	}
	return score
}

func inlineStructDataType(fields []*view.Field) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == nil || field.Schema == nil {
			continue
		}
		typeName := strings.TrimSpace(firstNonEmpty(field.Schema.DataType, field.Schema.Name))
		if typeName == "" {
			continue
		}
		tag := strings.TrimSpace(stripVeltyTag(field.Tag))
		if tag != "" {
			parts = append(parts, fmt.Sprintf(`%s %s %q`, strings.TrimSpace(field.Name), typeName, tag))
			continue
		}
		parts = append(parts, fmt.Sprintf(`%s %s`, strings.TrimSpace(field.Name), typeName))
	}
	return "struct { " + strings.Join(parts, "; ") + " }"
}

func typeDefinitionFieldsFromReflectType(rType reflect.Type) []*view.Field {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	result := make([]*view.Field, 0, rType.NumField())
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() {
			continue
		}
		result = append(result, &view.Field{
			Name:     field.Name,
			Schema:   schemaFromReflectType(field.Type),
			Tag:      string(field.Tag),
			FromName: field.Name,
		})
	}
	return result
}

func schemaFromReflectType(rType reflect.Type) *state.Schema {
	if rType == nil {
		return nil
	}
	schema := state.NewSchema(rType)
	if schema == nil {
		return nil
	}
	if schema.Name == "" && schema.DataType == "" {
		schema.DataType = rType.String()
		if schema.Cardinality == "" {
			schema.Cardinality = state.One
		}
	}
	if schema.Cardinality == state.Many && schema.DataType == "" {
		schema.DataType = rType.String()
	}
	return schema
}

func stripVeltyTag(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	updated, _ := xreflect.RemoveTag(tag, "velty")
	return strings.TrimSpace(updated)
}

func sanitizeTypeFieldTag(tag string, field *view.Field) string {
	tag = stripVeltyTag(tag)
	if field == nil || field.Schema == nil {
		return tag
	}
	dataType := strings.TrimSpace(firstNonEmpty(field.Schema.DataType, field.Schema.Name))
	if strings.HasPrefix(dataType, "*struct {") || strings.HasPrefix(dataType, "struct {") {
		updated, _ := xreflect.RemoveTag(tag, "typeName")
		tag = strings.TrimSpace(updated)
	}
	return tag
}

func normalizeViewSchemaPackages(aView *view.View) {
	if aView == nil {
		return
	}
	normalizeSchemaPackage(aView.Schema)
	if aView.Template != nil {
		normalizeParameterSchemas(aView.Template.Parameters)
		if aView.Template.Summary != nil {
			normalizeSchemaPackage(aView.Template.Summary.Schema)
		}
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		normalizeViewSchemaPackages(&rel.Of.View)
	}
}

func normalizeParameterSchemas(params state.Parameters) {
	for _, param := range params {
		if param == nil {
			continue
		}
		normalizeSchemaPackage(param.Schema)
		if param.Output != nil {
			normalizeSchemaPackage(param.Output.Schema)
		}
	}
}

func normalizeSchemaPackage(schema *state.Schema) {
	if schema == nil {
		return
	}
	schema.Package = normalizedSchemaPackage(schema.Package, firstNonEmpty(schema.PackagePath, schema.ModulePath))
	if strings.TrimSpace(schema.PackagePath) == "" && strings.Contains(strings.TrimSpace(schema.Package), "/") {
		schema.PackagePath = strings.TrimSpace(schema.Package)
	}
}

func normalizedSchemaPackage(pkg, pkgPath string) string {
	pkg = strings.TrimSpace(pkg)
	pkgPath = strings.TrimSpace(pkgPath)
	if pkgPath == "" && strings.Contains(pkg, "/") {
		pkgPath = pkg
	}
	if strings.Contains(pkg, "/") {
		return path.Base(pkg)
	}
	if pkg == "" && pkgPath != "" {
		return path.Base(pkgPath)
	}
	return pkg
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func alignGeneratedPackageAliases(resource *view.Resource, component *shapeLoad.Component, packageDir, packagePath, packageName string) {
	packageDir = strings.TrimSpace(packageDir)
	packagePath = strings.TrimSpace(packagePath)
	packageName = strings.TrimSpace(packageName)
	if packagePath == "" || packageName == "" {
		return
	}
	if component != nil && component.TypeContext != nil {
		if packageDir != "" {
			packageDir = filepath.ToSlash(filepath.Clean(packageDir))
		}
		if strings.TrimSpace(component.TypeContext.PackagePath) == packagePath {
			component.TypeContext.PackageName = packageName
			if packageDir != "" {
				component.TypeContext.PackageDir = packageDir
			}
		}
		if strings.TrimSpace(component.TypeContext.PackagePath) == "" {
			component.TypeContext.PackagePath = packagePath
		}
		if strings.TrimSpace(component.TypeContext.PackageName) == "" {
			component.TypeContext.PackageName = packageName
		}
		if packageDir != "" && strings.TrimSpace(component.TypeContext.PackageDir) == "" {
			component.TypeContext.PackageDir = packageDir
		}
		for _, group := range [][]*plan.State{component.Input, component.Output, component.Meta, component.Async, component.Other} {
			for _, item := range group {
				if item == nil {
					continue
				}
				alignSchemaPackageAlias(item.Schema, packagePath, packageName)
				alignSchemaPackageAlias(item.OutputSchema(), packagePath, packageName)
			}
		}
	}
	if resource == nil {
		return
	}
	for _, item := range resource.Parameters {
		if item == nil {
			continue
		}
		alignSchemaPackageAlias(item.Schema, packagePath, packageName)
		alignSchemaPackageAlias(item.OutputSchema(), packagePath, packageName)
	}
	for _, aView := range resource.Views {
		if aView == nil {
			continue
		}
		alignSchemaPackageAlias(aView.Schema, packagePath, packageName)
		if aView.Template != nil {
			alignSchemaPackageAlias(aView.Template.Schema, packagePath, packageName)
			alignParameterPackages(aView.Template.Parameters, packagePath, packageName)
			if aView.Template.Summary != nil {
				alignSchemaPackageAlias(aView.Template.Summary.Schema, packagePath, packageName)
			}
		}
		for _, rel := range aView.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			alignSchemaPackageAlias(rel.Of.Schema, packagePath, packageName)
			alignSchemaPackageAlias(rel.Of.View.Schema, packagePath, packageName)
			if rel.Of.View.Template != nil && rel.Of.View.Template.Summary != nil {
				alignSchemaPackageAlias(rel.Of.View.Template.Summary.Schema, packagePath, packageName)
			}
		}
	}
	for _, item := range resource.Types {
		if item == nil {
			continue
		}
		if firstNonEmpty(strings.TrimSpace(item.ModulePath), schemaPackagePath(item.Schema)) == packagePath {
			item.Package = packageName
		}
		alignSchemaPackageAlias(item.Schema, packagePath, packageName)
		for _, field := range item.Fields {
			if field == nil {
				continue
			}
			alignSchemaPackageAlias(field.Schema, packagePath, packageName)
		}
	}
}

func alignParameterPackages(params state.Parameters, packagePath, packageName string) {
	for _, item := range params {
		if item == nil {
			continue
		}
		alignSchemaPackageAlias(item.Schema, packagePath, packageName)
		alignSchemaPackageAlias(item.OutputSchema(), packagePath, packageName)
	}
}

func alignSchemaPackageAlias(schema *state.Schema, packagePath, packageName string) {
	if schema == nil {
		return
	}
	if schemaPackagePath(schema) == packagePath {
		schema.Package = packageName
		qualifyGeneratedSchemaDataType(schema, packageName)
	}
}

func schemaPackagePath(schema *state.Schema) string {
	if schema == nil {
		return ""
	}
	return firstNonEmpty(strings.TrimSpace(schema.PackagePath), strings.TrimSpace(schema.ModulePath))
}

func qualifyGeneratedSchemaDataType(schema *state.Schema, packageName string) {
	if schema == nil {
		return
	}
	packageName = strings.TrimSpace(packageName)
	if packageName == "" {
		return
	}
	dataType := strings.TrimSpace(schema.DataType)
	typeName := strings.TrimLeft(strings.TrimSpace(schema.Name), "*")
	if dataType == "" || typeName == "" || strings.Contains(dataType, ".") {
		return
	}
	replacements := map[string]string{
		typeName:         packageName + "." + typeName,
		"*" + typeName:   "*" + packageName + "." + typeName,
		"[]" + typeName:  "[]" + packageName + "." + typeName,
		"[]*" + typeName: "[]*" + packageName + "." + typeName,
	}
	if qualified, ok := replacements[dataType]; ok {
		schema.DataType = qualified
	}
}

func ensureTypeNameTag(tag string, typeName string) string {
	typeName = strings.TrimSpace(typeName)
	if typeName == "" {
		return strings.TrimSpace(tag)
	}
	tag = strings.TrimSpace(tag)
	if strings.Contains(tag, `typeName:"`) {
		return tag
	}
	if tag == "" {
		return fmt.Sprintf(`typeName:"%s"`, typeName)
	}
	return tag + ` typeName:"` + typeName + `"`
}

func normalizeConnectorRefsYAML(data []byte) ([]byte, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	rewriteConnectorNode(&node)
	return yaml.Marshal(&node)
}

func normalizeRouteViewRefsYAML(data []byte) ([]byte, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	rewriteRouteViewNode(&node)
	return yaml.Marshal(&node)
}

func normalizeRouteComponentEmbeddingYAML(data []byte) ([]byte, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	if len(node.Content) == 0 || node.Content[0] == nil {
		return data, nil
	}
	root := node.Content[0]
	routes := yamlMapLookup(root, "Routes")
	if routes == nil || routes.Kind != yaml.SequenceNode {
		return data, nil
	}
	for _, item := range routes.Content {
		flattenRouteComponentNode(item)
	}
	return yaml.Marshal(&node)
}

func ensureSharedResourceRefsYAML(data []byte, refs []string) ([]byte, error) {
	if len(refs) == 0 {
		return data, nil
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	if len(node.Content) == 0 || node.Content[0] == nil || node.Content[0].Kind != yaml.MappingNode {
		return data, nil
	}
	root := node.Content[0]
	if existing := yamlMapLookup(root, "With"); existing != nil && existing.Kind == yaml.SequenceNode && len(existing.Content) > 0 {
		return data, nil
	}
	seq := &yaml.Node{Kind: yaml.SequenceNode}
	for _, ref := range refs {
		if strings.TrimSpace(ref) == "" {
			continue
		}
		seq.Content = append(seq.Content, &yaml.Node{Kind: yaml.ScalarNode, Value: strings.TrimSpace(ref), Tag: "!!str"})
	}
	if len(seq.Content) == 0 {
		return data, nil
	}
	root.Content = append(root.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: "With", Tag: "!!str"},
		seq,
	)
	return yaml.Marshal(&node)
}

func rewriteConnectorNode(node *yaml.Node) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			rewriteConnectorNode(child)
		}
	case yaml.MappingNode:
		for i := 0; i < len(node.Content)-1; i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			switch strings.ToLower(strings.TrimSpace(key.Value)) {
			case "connector":
				if ref := connectorRefFromYAMLNode(value); ref != "" {
					node.Content[i+1] = connectorRefYAMLNode(ref)
					value = node.Content[i+1]
				}
			case "connectors":
				if value.Kind == yaml.SequenceNode {
					for j, item := range value.Content {
						if ref := connectorRefFromYAMLNode(item); ref != "" {
							value.Content[j] = connectorRefYAMLNode(ref)
						}
					}
				}
			}
			rewriteConnectorNode(value)
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			rewriteConnectorNode(child)
		}
	}
}

func rewriteRouteViewNode(node *yaml.Node) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			rewriteRouteViewNode(child)
		}
	case yaml.MappingNode:
		for i := 0; i < len(node.Content)-1; i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			if strings.EqualFold(strings.TrimSpace(key.Value), "view") && value.Kind == yaml.MappingNode {
				if ref := routeViewRefFromYAMLNode(value); ref != "" {
					node.Content[i+1] = &yaml.Node{
						Kind: yaml.MappingNode,
						Content: []*yaml.Node{
							{Kind: yaml.ScalarNode, Value: "Ref", Tag: "!!str"},
							{Kind: yaml.ScalarNode, Value: ref, Tag: "!!str"},
						},
					}
					value = node.Content[i+1]
				}
			}
			rewriteRouteViewNode(value)
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			rewriteRouteViewNode(child)
		}
	}
}

func flattenRouteComponentNode(node *yaml.Node) {
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}
	for _, key := range []string{"meta", "path", "contract"} {
		embedded := yamlMapLookup(node, key)
		if embedded == nil || embedded.Kind != yaml.MappingNode {
			continue
		}
		removeYAMLMapKey(node, key)
		node.Content = append(node.Content, embedded.Content...)
	}
}

func routeViewRefFromYAMLNode(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	if ref := yamlMapLookup(node, "Ref"); ref != nil && strings.TrimSpace(ref.Value) != "" {
		return strings.TrimSpace(ref.Value)
	}
	reference := yamlMapLookup(node, "reference")
	if reference == nil {
		return ""
	}
	ref := yamlMapLookup(reference, "ref")
	if ref == nil {
		return ""
	}
	return strings.TrimSpace(ref.Value)
}

func connectorRefFromYAMLNode(node *yaml.Node) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	if ref := yamlMapLookup(node, "ref"); ref != nil && strings.TrimSpace(ref.Value) != "" {
		return strings.TrimSpace(ref.Value)
	}
	connection := yamlMapLookup(node, "connection")
	if connection == nil {
		return ""
	}
	dbConfig := yamlMapLookup(connection, "dbconfig")
	if dbConfig == nil {
		return ""
	}
	reference := yamlMapLookup(dbConfig, "reference")
	if reference == nil {
		return ""
	}
	ref := yamlMapLookup(reference, "ref")
	if ref == nil {
		return ""
	}
	return strings.TrimSpace(ref.Value)
}

func connectorRefYAMLNode(ref string) *yaml.Node {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "ref", Tag: "!!str"},
			{Kind: yaml.ScalarNode, Value: ref, Tag: "!!str"},
		},
	}
}

func yamlMapLookup(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(node.Content)-1; i += 2 {
		if strings.EqualFold(strings.TrimSpace(node.Content[i].Value), key) {
			return node.Content[i+1]
		}
	}
	return nil
}

func removeYAMLMapKey(node *yaml.Node, key string) {
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}
	filtered := make([]*yaml.Node, 0, len(node.Content))
	for i := 0; i < len(node.Content)-1; i += 2 {
		if strings.EqualFold(strings.TrimSpace(node.Content[i].Value), key) {
			continue
		}
		filtered = append(filtered, node.Content[i], node.Content[i+1])
	}
	node.Content = filtered
}
