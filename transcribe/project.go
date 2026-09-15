package transcribe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/bindly/resource"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format/text"
)

const projectManifestVersion = 1

// ProjectGeneration persists a coherent project from already compiled
// canonical components. Compilation remains owned by Compiler and
// PackageCompilation; this type owns only cross-component validation and
// project-level output.
type ProjectGeneration struct {
	Components []*Result
	// Resources is the staged named authority shared by all component sources.
	Resources *resource.Store
}

type GeneratedProject struct {
	Manifest   *ProjectManifest
	Components []*GeneratedPackage
}

type ProjectManifest struct {
	Version      int                    `json:"version"`
	Components   []ProjectComponent     `json:"components"`
	Dependencies []string               `json:"dependencies,omitempty"`
	Analysis     ProjectMigrationReport `json:"analysis"`
}

type ProjectComponent struct {
	Key          spec.Key               `json:"key"`
	Source       string                 `json:"source,omitempty"`
	Package      string                 `json:"package"`
	OutputType   string                 `json:"outputType,omitempty"`
	OutputOrigin typecatalog.TypeOrigin `json:"outputOrigin,omitempty"`
	IR           string                 `json:"ir"`
	Diagnostics  string                 `json:"diagnostics"`
	Artifacts    []string               `json:"artifacts"`
	Routes       []ProjectRoute         `json:"routes,omitempty"`
	Dependencies []string               `json:"dependencies,omitempty"`
}

type ProjectRoute struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type ProjectMigrationReport struct {
	Components []ProjectMigrationComponent `json:"components"`
}

type ProjectMigrationComponent struct {
	Key             spec.Key `json:"key"`
	InputOwnership  string   `json:"inputOwnership"`
	OutputOwnership string   `json:"outputOwnership"`
	GeneratedViews  int      `json:"generatedViews"`
	LinkedViews     int      `json:"linkedViews"`
	Handler         string   `json:"handler"`
}

type preparedProjectComponent struct {
	compiled      *Result
	key           spec.Key
	identity      string
	slug          string
	packagePath   string
	targetPackage string
	plan          *gen.Plan
	dependencies  map[string]bool
}

func (g *ProjectGeneration) Generate(ctx context.Context, rootDir string) (*GeneratedProject, error) {
	if g == nil || len(g.Components) == 0 {
		return nil, fmt.Errorf("project generation requires at least one compiled component")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store, err := newProjectMetadataStore(rootDir)
	if err != nil {
		return nil, err
	}
	release, err := store.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err = store.prepare(); err != nil {
		return nil, err
	}
	existing, err := store.readManifest()
	if err != nil {
		return nil, err
	}
	prepared, err := g.prepareWithManifest(rootDir, existing)
	if err != nil {
		return nil, err
	}
	editor, err := newProjectManifestEditor(existing, prepared)
	if err != nil {
		return nil, err
	}
	generated := make([]*GeneratedPackage, 0, len(prepared))
	metadata := make(map[string]any, len(prepared)*2)
	for _, component := range prepared {
		artifact, err := NewCompiler().generateCompiledAt(ctx, rootDir, component.packagePath, component.compiled)
		if err != nil {
			return nil, fmt.Errorf("generate component %q: %w", component.identity, err)
		}
		generated = append(generated, artifact)
		entry, err := component.manifestEntry(rootDir, artifact)
		if err != nil {
			return nil, err
		}
		editor.Replace(entry, migrationEntry(component.key, artifact.Result.Plan))
		metadata[entry.IR] = component.compiled.Component
		metadata[entry.Diagnostics] = component.compiled.Diagnostics
	}
	manifest := editor.Finalize()
	if err = store.persist(manifest, metadata); err != nil {
		return nil, err
	}
	return &GeneratedProject{Manifest: manifest, Components: generated}, nil
}

func (g *ProjectGeneration) prepare(rootDir string) ([]preparedProjectComponent, error) {
	return g.prepareWithManifest(rootDir, nil)
}

func (g *ProjectGeneration) prepareWithManifest(rootDir string, existing *ProjectManifest) ([]preparedProjectComponent, error) {
	seen := map[string]bool{}
	result := make([]preparedProjectComponent, 0, len(g.Components))
	for _, source := range g.Components {
		compiled, err := source.projectClone()
		if err != nil {
			return nil, err
		}
		key, err := compiled.projectKey()
		if err != nil {
			return nil, err
		}
		identity := key.String()
		if seen[identity] {
			return nil, fmt.Errorf("project component %q occurs more than once", identity)
		}
		seen[identity] = true
		slug := projectComponentSlug(key)
		packagePath := filepath.Join("generated", slug)
		input, packagePath, err := generationInput(rootDir, packagePath, compiled)
		if err != nil {
			return nil, fmt.Errorf("plan component %q: %w", identity, err)
		}
		plan, err := gen.New(input).Plan()
		if err != nil {
			return nil, fmt.Errorf("plan component %q: %w", identity, err)
		}
		if err = plan.ValidateDestination(filepath.Join(rootDir, packagePath)); err != nil {
			return nil, fmt.Errorf("validate component %q destination: %w", identity, err)
		}
		result = append(result, preparedProjectComponent{
			compiled: compiled, key: key, identity: identity, slug: slug,
			packagePath: packagePath, targetPackage: input.TargetPackage, plan: plan,
		})
	}
	var existingComponents []ProjectComponent
	if existing != nil {
		existingComponents = existing.Components
	}
	components := newProjectComponentResolver(result, existingComponents)
	if err := components.resolve(); err != nil {
		return nil, err
	}
	for index := range result {
		component := &result[index]
		input, _, err := generationInput(rootDir, component.packagePath, component.compiled)
		if err != nil {
			return nil, fmt.Errorf("plan component %q: %w", component.identity, err)
		}
		component.plan, err = gen.New(input).Plan()
		if err != nil {
			return nil, fmt.Errorf("plan component %q: %w", component.identity, err)
		}
		if err = component.plan.ValidateDestination(filepath.Join(rootDir, component.packagePath)); err != nil {
			return nil, fmt.Errorf("validate component %q destination: %w", component.identity, err)
		}
	}
	components.components = result
	ordered, err := components.order()
	if err != nil {
		return nil, err
	}
	var packages gen.Packages
	for _, entry := range result {
		packages = append(packages, gen.PackagePlan{Plan: entry.plan, Directory: filepath.Join(rootDir, entry.packagePath)})
	}
	if err := packages.Validate(); err != nil {
		return nil, err
	}
	return ordered, nil
}

func (r *Result) projectClone() (*Result, error) {
	if r == nil || r.Source == nil || r.Component == nil {
		return nil, fmt.Errorf("project component requires a compiled transcribe result")
	}
	result := *r
	componentSource := *r.Source
	if componentSource.Types == nil {
		componentSource.Types = typecatalog.NewCatalog()
	} else {
		var err error
		componentSource.Types, err = componentSource.Types.Clone()
		if err != nil {
			return nil, err
		}
	}
	result.Source = &componentSource
	result.Component = r.Component.Clone()
	result.Diagnostics = append([]*Diagnostic(nil), r.Diagnostics...)
	result.GeneratedTypes = append([]gen.GeneratedTypeReference(nil), r.GeneratedTypes...)
	if result.TypeAuthority == "" {
		result.TypeAuthority = typecatalog.TranscribeAuthority
	}
	var err error
	result.TypeResolver, err = typecatalog.NewResolver(componentSource.Types, result.TypeAuthority, result.TypeContext)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (r *Result) projectKey() (spec.Key, error) {
	if r == nil || r.Component == nil || r.Source == nil {
		return spec.Key{}, fmt.Errorf("project component requires a compiled transcribe result")
	}
	key := r.Component.Key
	if key.Kind == "" {
		key.Kind = spec.KindComponent
	}
	if strings.TrimSpace(key.Scope) == "" {
		key.Scope = strings.TrimSpace(r.Source.Scope)
	}
	if strings.TrimSpace(key.Name) == "" {
		key.Name = strings.TrimSpace(r.Component.Name)
	}
	key.Scope = strings.TrimSpace(key.Scope)
	key.Name = strings.TrimSpace(key.Name)
	if key.Name == "" {
		return spec.Key{}, fmt.Errorf("project component name is required")
	}
	return key, nil
}

func projectComponentSlug(key spec.Key) string {
	name := text.DetectCaseFormat(key.Name).Format(key.Name, text.CaseFormatLowerUnderscore)
	name = strings.Trim(name, "_")
	if name == "" {
		name = "component"
	}
	digest := sha256.Sum256([]byte(key.String()))
	return name + "_" + hex.EncodeToString(digest[:4])
}

func (c preparedProjectComponent) manifestEntry(rootDir string, generated *GeneratedPackage) (ProjectComponent, error) {
	output, err := c.output()
	if err != nil {
		return ProjectComponent{}, fmt.Errorf("index component %q output: %w", c.identity, err)
	}
	entry := ProjectComponent{
		Key: c.key, Source: strings.TrimSpace(c.compiled.Source.Path),
		Package:      filepath.ToSlash(c.packagePath),
		OutputType:   output.descriptor.Key(),
		OutputOrigin: output.origin,
		IR:           filepath.ToSlash(filepath.Join("ir", c.slug+".json")),
		Diagnostics:  filepath.ToSlash(filepath.Join("diagnostics", c.slug+".json")),
		Routes:       c.routes(),
		Dependencies: projectDependencies(generated.Result.Plan),
	}
	for _, file := range generated.Result.Files {
		relative, err := filepath.Rel(rootDir, file.Path)
		if err != nil {
			return ProjectComponent{}, fmt.Errorf("index component %q artifact %q: %w", c.identity, file.Path, err)
		}
		relative, err = managedProjectPath(relative)
		if err != nil {
			return ProjectComponent{}, fmt.Errorf("index component %q artifact %q: %w", c.identity, file.Path, err)
		}
		entry.Artifacts = append(entry.Artifacts, filepath.ToSlash(relative))
	}
	sort.Strings(entry.Artifacts)
	return entry, nil
}

func (c preparedProjectComponent) routes() []ProjectRoute {
	if c.compiled == nil || c.compiled.Component == nil {
		return nil
	}
	if content := c.compiled.Component.Static; content != nil {
		return []ProjectRoute{{Method: "GET", Path: content.Path}, {Method: "HEAD", Path: content.Path}}
	}
	result := make([]ProjectRoute, 0, len(c.compiled.Component.Routes))
	for _, route := range c.compiled.Component.Routes {
		if route == nil {
			continue
		}
		result = append(result, ProjectRoute{Method: strings.ToUpper(strings.TrimSpace(route.Method)), Path: strings.TrimSpace(route.Path)})
	}
	return result
}

func projectRouteIdentity(route ProjectRoute) string {
	method := strings.ToUpper(strings.TrimSpace(route.Method))
	path := strings.TrimSpace(route.Path)
	if method == "" || path == "" {
		return ""
	}
	return method + " " + path
}

func projectDependencies(plan *gen.Plan) []string {
	if plan == nil {
		return nil
	}
	seen := map[string]bool{}
	for _, item := range plan.Imports {
		if dependency := strings.TrimSpace(item.Package); dependency != "" {
			seen[dependency] = true
		}
	}
	for _, generated := range plan.GeneratedTypes {
		for _, item := range generated.Imports {
			if dependency := strings.TrimSpace(item.Package); dependency != "" {
				seen[dependency] = true
			}
		}
	}
	result := make([]string, 0, len(seen))
	for dependency := range seen {
		result = append(result, dependency)
	}
	sort.Strings(result)
	return result
}

func migrationEntry(key spec.Key, plan *gen.Plan) ProjectMigrationComponent {
	entry := ProjectMigrationComponent{Key: key}
	if plan == nil {
		return entry
	}
	entry.InputOwnership = string(plan.Input.Ownership)
	entry.OutputOwnership = string(plan.Output.Ownership)
	for _, view := range plan.Views {
		if view.Ownership == gen.ViewLinked {
			entry.LinkedViews++
		} else {
			entry.GeneratedViews++
		}
	}
	switch {
	case plan.VeltyHandler != nil:
		entry.Handler = "velty"
	case plan.GoHandler != nil || strings.TrimSpace(plan.Handler) != "":
		entry.Handler = "custom"
	case strings.TrimSpace(plan.RootViewName) != "":
		entry.Handler = "reader"
	}
	return entry
}
