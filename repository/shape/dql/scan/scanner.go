package scan

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/repository/shape/dql/decl"
	"github.com/viant/datly/repository/shape/dql/ir"
	"github.com/viant/datly/repository/shape/dql/parse"
	dqlplan "github.com/viant/datly/repository/shape/dql/plan"
	"github.com/viant/datly/repository/shape/dql/sanitize"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/repository/shape/typectx/source"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/x"
)

// Request defines input for DQL scan.
type Request struct {
	DQLURL                 string
	ConfigURL              string
	Repository             string
	ModulePrefix           string
	APIPrefix              string
	Connectors             []string
	AllowedProvenanceKinds []string
	AllowedSourceRoots     []string
	UseGoModuleResolve     *bool
	UseGOPATHFallback      *bool
	StrictProvenance       *bool
}

// Result holds scanner output.
type Result struct {
	RuleName string
	Shape    *dqlshape.Document
	IR       *ir.Document
}

// Scanner translates DQL to Datly route YAML in-memory.
type Scanner struct {
	fs afs.Service
}

func New() *Scanner {
	return &Scanner{fs: afs.New()}
}

func (s *Scanner) Scan(ctx context.Context, req *Request) (result *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("dql scan panic: %v", r)
			result = nil
		}
	}()
	if req == nil || req.DQLURL == "" {
		return nil, fmt.Errorf("dql scan: DQLURL was empty")
	}
	sourceURL := req.DQLURL
	project := inferProject(req.DQLURL)
	translate := &options.Translate{}
	translate.Rule.Project = project
	translate.Rule.Source = []string{sourceURL}
	translate.Rule.ModulePrefix = req.ModulePrefix
	translate.Repository.RepositoryURL = req.Repository
	translate.Repository.APIPrefix = req.APIPrefix
	if len(req.Connectors) > 0 {
		translate.Repository.Connectors = append(translate.Repository.Connectors, req.Connectors...)
	}
	if req.ConfigURL != "" {
		translate.Repository.Configs.Append(req.ConfigURL)
	}
	var initErr error
	if initErr = translate.Init(ctx); initErr != nil {
		return nil, initErr
	}
	if req.ConfigURL == "" {
		// Force in-memory translator config to avoid stale absolute paths from discovered config.json.
		translate.Repository.Configs = nil
	}
	if translate.Rule.ModulePrefix == "" {
		translate.Rule.ModulePrefix = "platform"
	}

	svc := translator.New(translator.NewConfig(&translate.Repository), s.fs)
	if initErr := svc.Init(ctx); initErr != nil {
		return nil, initErr
	}
	if initErr := svc.InitSignature(ctx, &translate.Rule); initErr != nil {
		return nil, initErr
	}
	dsql, loadErr := translate.Rule.LoadSource(ctx, s.fs, translate.Rule.SourceURL())
	if loadErr != nil {
		return nil, loadErr
	}
	translate.Rule.NormalizeComponent(&dsql)
	dsql = sanitize.SQL(dsql, sanitize.Options{Declared: sanitize.Declared(dsql)})
	top := &options.Options{Translate: translate}
	if initErr = svc.Translate(ctx, &translate.Rule, dsql, top); initErr != nil {
		return nil, initErr
	}
	ruleName := svc.Repository.RuleName(&translate.Rule)
	targetSuffix := "/" + ruleName + ".yaml"
	for _, item := range svc.Repository.Files {
		if !strings.HasSuffix(item.URL, targetSuffix) {
			continue
		}
		if strings.Contains(item.URL, "/.meta/") {
			continue
		}
		return s.result(ruleName, []byte(item.Content), dsql, req)
	}
	for _, item := range svc.Repository.Files {
		if strings.HasSuffix(item.URL, targetSuffix) {
			return s.result(ruleName, []byte(item.Content), dsql, req)
		}
	}
	return nil, fmt.Errorf("dql scan: generated YAML not found for %s", ruleName)
}

func (s *Scanner) result(ruleName string, routeYAML []byte, dql string, req *Request) (*Result, error) {
	if err := dqlplan.ValidateRelations(routeYAML); err != nil {
		return nil, fmt.Errorf("dql scan relation validation failed (%s): %w", ruleName, err)
	}
	fromYAML, err := ir.FromYAML(routeYAML)
	if err != nil {
		return nil, err
	}
	shapeDoc, err := dqlshape.FromIR(fromYAML)
	if err != nil {
		return nil, err
	}
	if parsed, parseErr := parse.New().Parse(dql); parseErr == nil && parsed != nil && parsed.TypeContext != nil {
		shapeDoc.TypeContext = parsed.TypeContext
	}
	if declarations, declErr := decl.Parse(dql); declErr == nil && len(declarations) > 0 {
		if resolutions, resolveErr := resolveTypeProvenance(declarations, shapeDoc.TypeContext, fromYAML, req); resolveErr != nil {
			return nil, resolveErr
		} else {
			shapeDoc.TypeResolutions = resolutions
		}
	}
	rebuiltIR, err := dqlshape.ToIR(shapeDoc)
	if err != nil {
		return nil, err
	}
	return &Result{RuleName: ruleName, Shape: shapeDoc, IR: rebuiltIR}, nil
}

func resolveTypeProvenance(declarations []*decl.Declaration, ctx *typectx.Context, doc *ir.Document, req *Request) ([]typectx.Resolution, error) {
	if len(declarations) == 0 {
		return nil, nil
	}
	registry, provenance := registryFromIR(doc)
	resolver := typectx.NewResolverWithProvenance(registry, ctx, provenance)
	policy := newProvenancePolicy(req)
	srcResolver, srcErr := newSourceResolver(policy, req)
	if srcErr != nil && policy.Strict {
		return nil, srcErr
	}
	var result []typectx.Resolution
	for _, declaration := range declarations {
		if declaration == nil || declaration.Kind != decl.KindCast {
			continue
		}
		expression := strings.TrimSpace(declaration.DataType)
		if expression == "" {
			continue
		}
		resolution, err := resolver.ResolveWithProvenance(expression)
		if err != nil {
			return nil, fmt.Errorf("dql scan cast resolution failed for %q: %w", expression, err)
		}
		if resolution == nil {
			continue
		}
		resolution.Target = declaration.Target
		enrichResolutionWithAST(resolution, srcResolver)
		if issue := validateResolutionPolicy(*resolution, policy); issue != "" {
			if policy.Strict {
				return nil, fmt.Errorf("dql scan provenance policy failed: %s", issue)
			}
			resolution.Provenance.Kind = "policy_warn:" + issue
		}
		result = append(result, *resolution)
	}
	return result, nil
}

func registryFromIR(doc *ir.Document) (*x.Registry, map[string]typectx.Provenance) {
	registry := x.NewRegistry()
	provenance := map[string]typectx.Provenance{}
	registerBuiltin := func(rType reflect.Type, kind string) {
		aType := x.NewType(rType)
		registry.Register(aType)
		provenance[aType.Key()] = typectx.Provenance{
			Package: packageOfKey(aType.Key()),
			Kind:    kind,
		}
	}
	registerBuiltin(reflect.TypeOf(time.Time{}), "builtin")
	registerBuiltin(reflect.TypeOf(""), "builtin")
	registerBuiltin(reflect.TypeOf(0), "builtin")
	registerBuiltin(reflect.TypeOf(int64(0)), "builtin")
	registerBuiltin(reflect.TypeOf(float64(0)), "builtin")
	registerBuiltin(reflect.TypeOf(true), "builtin")

	if doc == nil || doc.Root == nil {
		return registry, provenance
	}
	resource := asMap(doc.Root["Resource"])
	if resource == nil {
		return registry, provenance
	}
	for _, item := range asSlice(resource["Types"]) {
		typeMap := asMap(item)
		if typeMap == nil {
			continue
		}
		name := strings.TrimSpace(asString(typeMap["Name"]))
		if name == "" {
			continue
		}
		pkg := strings.TrimSpace(asString(typeMap["Package"]))
		aType := &x.Type{Name: name, PkgPath: pkg}
		registry.Register(aType)
		key := aType.Key()
		provenance[key] = typectx.Provenance{
			Package: pkg,
			File:    firstNonEmpty(asString(typeMap["SourceURL"]), asString(typeMap["ModulePath"])),
			Kind:    "resource_type",
		}
	}
	return registry, provenance
}

func asMap(raw any) map[string]any {
	if value, ok := raw.(map[string]any); ok {
		return value
	}
	if value, ok := raw.(map[any]any); ok {
		result := make(map[string]any, len(value))
		for k, v := range value {
			result[fmt.Sprint(k)] = v
		}
		return result
	}
	return nil
}

func asSlice(raw any) []any {
	if value, ok := raw.([]any); ok {
		return value
	}
	return nil
}

func asString(raw any) string {
	if raw == nil {
		return ""
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fmt.Sprint(raw)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func packageOfKey(key string) string {
	index := strings.LastIndex(key, ".")
	if index == -1 {
		return ""
	}
	return key[:index]
}

type provenancePolicy struct {
	AllowedKinds map[string]bool
	Roots        []string
	Strict       bool
}

func newProvenancePolicy(req *Request) provenancePolicy {
	allowedKinds := map[string]bool{
		"builtin":       true,
		"resource_type": true,
		"ast_type":      true,
	}
	if req != nil && len(req.AllowedProvenanceKinds) > 0 {
		allowedKinds = map[string]bool{}
		for _, item := range req.AllowedProvenanceKinds {
			item = strings.TrimSpace(strings.ToLower(item))
			if item != "" {
				allowedKinds[item] = true
			}
		}
	}
	repo := ""
	if req != nil {
		repo = req.Repository
	}
	roots := source.NormalizeRoots(repo, requestRoots(req))
	return provenancePolicy{
		AllowedKinds: allowedKinds,
		Roots:        roots,
		Strict:       requestStrict(req),
	}
}

func requestRoots(req *Request) []string {
	if req == nil {
		return nil
	}
	return req.AllowedSourceRoots
}

func requestStrict(req *Request) bool {
	if req == nil || req.StrictProvenance == nil {
		return true
	}
	return *req.StrictProvenance
}

func requestUseModule(req *Request) bool {
	if req == nil || req.UseGoModuleResolve == nil {
		return true
	}
	return *req.UseGoModuleResolve
}

func requestUseGOPATH(req *Request) bool {
	if req == nil || req.UseGOPATHFallback == nil {
		return true
	}
	return *req.UseGOPATHFallback
}

func newSourceResolver(policy provenancePolicy, req *Request) (*source.Resolver, error) {
	if req == nil || strings.TrimSpace(req.Repository) == "" {
		return nil, nil
	}
	return source.New(source.Config{
		ProjectDir:         req.Repository,
		AllowedSourceRoots: policy.Roots,
		UseGoModuleResolve: requestUseModule(req),
		UseGOPATHFallback:  requestUseGOPATH(req),
	})
}

func enrichResolutionWithAST(resolution *typectx.Resolution, srcResolver *source.Resolver) {
	if resolution == nil || srcResolver == nil {
		return
	}
	if strings.TrimSpace(resolution.Provenance.File) != "" {
		return
	}
	pkg := strings.TrimSpace(resolution.Provenance.Package)
	typeName := typeNameFromKey(resolution.ResolvedKey)
	if pkg == "" || typeName == "" {
		return
	}
	filePath, err := srcResolver.ResolveTypeFile(pkg, typeName)
	if err != nil {
		return
	}
	resolution.Provenance.File = filePath
	if resolution.Provenance.Kind == "" || resolution.Provenance.Kind == "registry" {
		resolution.Provenance.Kind = "ast_type"
	}
}

func typeNameFromKey(key string) string {
	index := strings.LastIndex(key, ".")
	if index == -1 || index+1 >= len(key) {
		return ""
	}
	return key[index+1:]
}

func validateResolutionPolicy(resolution typectx.Resolution, policy provenancePolicy) string {
	kind := strings.TrimSpace(strings.ToLower(resolution.Provenance.Kind))
	if kind == "" {
		kind = "registry"
	}
	if !policy.AllowedKinds[kind] {
		return fmt.Sprintf("expression=%q kind=%q not allowed", resolution.Expression, resolution.Provenance.Kind)
	}
	filePath := strings.TrimSpace(resolution.Provenance.File)
	if filePath == "" {
		return ""
	}
	if len(policy.Roots) == 0 {
		return ""
	}
	ok, err := source.IsWithinAnyRoot(filePath, policy.Roots)
	if err != nil {
		return fmt.Sprintf("expression=%q source=%q invalid: %v", resolution.Expression, filePath, err)
	}
	if !ok {
		return fmt.Sprintf("expression=%q source=%q outside trusted roots", resolution.Expression, filePath)
	}
	return ""
}

func inferProject(dqlURL string) string {
	base, _ := url.Split(dqlURL, file.Scheme)
	if idx := strings.Index(base, "/dql/"); idx != -1 {
		return filepath.Clean(base[:idx])
	}
	return filepath.Clean(base)
}
