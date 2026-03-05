package command

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	pathpkg "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/shape"
	shapeColumn "github.com/viant/datly/repository/shape/column"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/xgen"
	"github.com/viant/datly/view"
	viewpkg "github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
)

// Transcribe runs the shape-only pipeline (compile → plan → load) for each
// DQL source. It does NOT depend on internal/translator.
func (s *Service) Transcribe(ctx context.Context, opts *options.Options) error {
	transcribe := opts.Transcribe
	if transcribe == nil {
		return fmt.Errorf("transcribe options not set")
	}
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	for _, sourceURL := range transcribe.Source {
		_, name := url.Split(sourceURL, file.Scheme)
		fmt.Printf("transcribing %v\n", name)
		dql, err := s.readSource(ctx, sourceURL)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", sourceURL, err)
		}
		dql = strings.TrimSpace(dql)
		if dql == "" {
			return fmt.Errorf("source %s was empty", sourceURL)
		}
		connectorName := transcribe.DefaultConnectorName()
		shapeSource := &shape.Source{
			Name:      strings.TrimSuffix(name, path.Ext(name)),
			Path:      url.Path(sourceURL),
			DQL:       dql,
			Connector: connectorName,
		}
		compileOpts := transcribeCompileOptions(transcribe)
		planResult, err := compiler.Compile(ctx, shapeSource, compileOpts...)
		if err != nil {
			return fmt.Errorf("failed to compile %s: %w", sourceURL, err)
		}
		componentArtifact, err := loader.LoadComponent(ctx, planResult)
		if err != nil {
			return fmt.Errorf("failed to load %s: %w", sourceURL, err)
		}
		component, ok := shapeLoad.ComponentFrom(componentArtifact)
		if !ok {
			return fmt.Errorf("unexpected component artifact for %s", sourceURL)
		}
		// Register connectors on resource first, then discover columns from DB
		if componentArtifact.Resource != nil && len(transcribe.Connectors) > 0 {
			applyConnectorsToResource(componentArtifact.Resource, transcribe.Connectors)
			discoverColumns(ctx, componentArtifact.Resource)
		}
		if err = s.persistTranscribeRoute(ctx, transcribe, sourceURL, dql, componentArtifact.Resource, component); err != nil {
			return err
		}
	}
	// Persist dependencies (connections.yaml, config.json)
	if len(transcribe.Connectors) > 0 {
		if err := s.persistTranscribeDependencies(ctx, transcribe); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) persistTranscribeDependencies(ctx context.Context, transcribe *options.Transcribe) error {
	depURL := url.Join(transcribe.Repository, "Datly", "dependencies")

	// connections.yaml — use flat format matching legacy translator output
	var connectors []connEntry
	for _, c := range transcribe.Connectors {
		parts := strings.SplitN(c, "|", 4)
		if len(parts) >= 3 {
			connectors = append(connectors, connEntry{Name: parts[0], Driver: parts[1], DSN: parts[2]})
		}
	}
	if len(connectors) > 0 {
		connURL := url.Join(depURL, "connections.yaml")
		// Merge with existing connections if file exists
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

	// config.json
	routeURL := url.Join(transcribe.Repository, "Datly", "routes")
	cfg := map[string]any{
		"APIPrefix":       transcribe.APIPrefix,
		"RouteURL":        routeURL,
		"DependencyURL":   depURL,
		"Endpoint":        map[string]any{"Port": 8080},
		"SyncFrequencyMs": 2000,
		"Meta":            map[string]any{"StatusURI": "/v1/api/status"},
	}
	cfgData, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	cfgURL := url.Join(transcribe.Repository, "Datly", "config.json")
	if err = s.fs.Upload(ctx, cfgURL, file.DefaultFileOsMode, strings.NewReader(string(cfgData))); err != nil {
		return fmt.Errorf("failed to persist config: %w", err)
	}
	return nil
}

func buildPathResource(resource *viewpkg.Resource, component *shapeLoad.Component) *pathpkg.Resource {
	if resource == nil {
		return nil
	}
	var params []*pathpkg.Parameter
	if component != nil {
		for _, s := range component.Input {
			if s != nil {
				params = append(params, &pathpkg.Parameter{
					Name:     s.Name,
					In:       s.In,
					Required: s.Required != nil && *s.Required,
					Schema:   s.Schema,
				})
			}
		}
	}
	if len(params) == 0 {
		return nil
	}
	return &pathpkg.Resource{Parameters: params}
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
		byName[c.Name] = c // incoming overrides existing for same name
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

func (s *Service) persistTranscribeRoute(ctx context.Context, transcribe *options.Transcribe, sourceURL, dql string, resource *view.Resource, component *shapeLoad.Component) error {
	sourcePath := filepath.Clean(url.Path(sourceURL))
	stem := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))

	// Determine generated file stem: --type-file flag, or root view name in lower_underscore, or DQL filename
	typeStem := transcribeTypeStem(transcribe, stem, component)

	routeRoot := url.Join(transcribe.Repository, "Datly", "routes")
	routeYAML := url.Join(routeRoot, stem+".yaml")

	if resource != nil {
		for _, item := range resource.Views {
			if item == nil || item.Template == nil || strings.TrimSpace(item.Template.Source) == "" {
				continue
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

	// Build route YAML as map to control key casing (runtime expects PascalCase YAML keys)
	routeEntry := map[string]any{
		"URI":    uri,
		"Method": method,
	}
	if rootView != "" {
		routeEntry["View"] = map[string]any{"Ref": rootView}
	}
	payload := map[string]any{
		"Routes":   []any{routeEntry},
		"Resource": sanitizeResourceForRouteYAML(resource),
	}
	data, err := yaml.Marshal(payload)
	if err != nil {
		return err
	}
	if err = s.fs.Upload(ctx, routeYAML, file.DefaultFileOsMode, strings.NewReader(string(data))); err != nil {
		return fmt.Errorf("failed to persist route yaml %s: %w", routeYAML, err)
	}
	_ = typeStem
	// Generate Go types directly from in-memory resource (no YAML roundtrip)
	if component != nil && component.TypeContext != nil && resource != nil {
		generateTranscribeTypes(url.Path(sourceURL), resource, component)
	}
	return nil
}

func generateTranscribeTypes(sourceAbsPath string, resource *view.Resource, component *shapeLoad.Component) {
	ctx := component.TypeContext
	if ctx == nil || strings.TrimSpace(ctx.PackageDir) == "" {
		return
	}
	projectDir := findProjectDir(sourceAbsPath)
	if projectDir == "" {
		fmt.Printf("WARNING: shape codegen: cannot locate go.mod from %s, skipping type generation\n", sourceAbsPath)
		return
	}
	codegen := &xgen.ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: false,
	}
	result, err := codegen.Generate()
	if err != nil {
		fmt.Printf("WARNING: shape codegen: type generation skipped for %s: %v\n", filepath.Base(sourceAbsPath), err)
		return
	}
	fmt.Printf("generated component %s → %s\n", strings.Join(result.Types, ", "), result.FilePath)
}

func transcribeCompileOptions(transcribe *options.Transcribe) []shape.CompileOption {
	var opts []shape.CompileOption
	if transcribe.Strict {
		opts = append(opts, shape.WithCompileStrict(true))
	}
	namespace := strings.TrimSpace(transcribe.Namespace)
	module := strings.TrimSpace(transcribe.Module)
	typeOutput := strings.TrimSpace(transcribe.TypeOutput)
	if typeOutput == "" || typeOutput == "." {
		typeOutput = module
	}
	if namespace != "" {
		pkgDir := filepath.Join(typeOutput, namespace)
		pkgName := filepath.Base(namespace)
		opts = append(opts, shape.WithTypeContextPackageDir(pkgDir))
		opts = append(opts, shape.WithTypeContextPackageName(pkgName))
	}
	return opts
}

// transcribeTypeStem determines the Go file name stem.
// Priority: --type-file flag > root view name (lower_underscore) > DQL filename
func transcribeTypeStem(transcribe *options.Transcribe, dqlStem string, component *shapeLoad.Component) string {
	if tf := strings.TrimSpace(transcribe.TypeFile); tf != "" {
		return strings.TrimSuffix(tf, ".go")
	}
	if component != nil {
		if rootView := strings.TrimSpace(component.RootView); rootView != "" {
			return toLowerUnderscore(rootView)
		}
	}
	return dqlStem
}

// toLowerUnderscore converts CamelCase or PascalCase to lower_underscore.
func toLowerUnderscore(s string) string {
	var buf strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := rune(s[i-1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) {
					buf.WriteByte('_')
				}
			}
			buf.WriteRune(unicode.ToLower(r))
		} else {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func transcribeRulePath(dql, ruleName, apiPrefix string, component *shapeLoad.Component) (string, string) {
	method := "GET"
	uri := "/" + strings.Trim(strings.TrimSpace(ruleName), "/")
	if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
		uri = strings.TrimRight(prefix, "/") + uri
	}
	if component != nil && component.Directives != nil && component.Directives.Route != nil {
		rd := component.Directives.Route
		if u := strings.TrimSpace(rd.URI); u != "" {
			uri = u
		}
		if len(rd.Methods) > 0 {
			if m := strings.TrimSpace(strings.ToUpper(rd.Methods[0])); m != "" {
				method = m
			}
		}
	}
	return method, uri
}

// discoverColumns resolves wildcard columns from DB for all views in the resource.
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
		if err != nil {
			fmt.Printf("  column discovery skipped for %s: %v\n", aView.Name, err)
			continue
		}
		if len(columns) > 0 {
			aView.Columns = columns
		}
	}
}

// applyConnectorsToResource registers connectors on the resource and sets refs on views.
// Connector format: name|driver|dsn (same encoding as datly translate -c flag).
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
			driver := strings.TrimSpace(parts[1])
			dsn := strings.TrimSpace(parts[2])
			resource.AddConnectors(view.NewConnector(name, driver, dsn))
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

// sanitizeResourceForRouteYAML returns a serialization-safe copy of resource
// with connector config stripped to references only. This keeps DSN/driver
// details out of route YAML; dependencies/connections.yaml remains the source
// of truth for connector definitions.
func sanitizeResourceForRouteYAML(resource *view.Resource) *view.Resource {
	if resource == nil {
		return nil
	}

	cloned := *resource

	if len(resource.Connectors) > 0 {
		cloned.Connectors = make([]*view.Connector, 0, len(resource.Connectors))
		for _, connector := range resource.Connectors {
			if connector == nil {
				continue
			}
			ref := strings.TrimSpace(connector.Ref)
			if ref == "" {
				ref = strings.TrimSpace(connector.Name)
			}
			if ref == "" {
				continue
			}
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
