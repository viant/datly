package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	authoring "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/transcribe/internal/program"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// compileFactoryInputShape uses the existing reader parser and column-discovery
// owners in an isolated authoring component. Only the resulting shape transfers
// to generation: the executable factory component has no body reader root.
// Independently declared input reads retain their ordinary runtime bindings.
func (c *Compiler) compileFactoryInputShape(ctx context.Context, source *Source, prepared *dql.PreparedSource, runtime *spec.Component, connector string, resolver *typecatalog.Resolver, declarations gen.Declarations, bindings gen.ViewBindings, linkedViews gen.ViewReferences, ownership *factoryDeclarations) (*spec.View, error) {
	shapeComponent := runtime.Clone()
	// These contracts are generated from the current declarations. Discovery
	// must not resolve a same-named imported or previously generated Input.
	// Keep the executable component's contract settings intact.
	shapeComponent.Settings.InputType = ""
	if strings.TrimSpace(prepared.SQL) == "" {
		if len(shapeComponent.Views) != 0 {
			rows, err := newPackageViewResolver(shapeComponent, resolver).typedInputDescriptors(prepared.TypeContext.PackagePath, bindings, func(name string) (bool, error) {
				return ownership.authoredCurrent(name)
			}, nil)
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				if prior := linkedViews[row.identity]; prior != nil && prior.DescriptorKey != row.descriptor.Key() {
					return nil, fmt.Errorf("factory Current view %q has conflicting type authorities", row.identity)
				}
				linkedViews[row.identity] = &gen.ViewReference{DescriptorKey: row.descriptor.Key()}
			}
			factory := &gen.ExternalHandler{Package: prepared.TypeContext.PackagePath, Name: strings.TrimPrefix(runtime.Routes[0].Handler, prepared.TypeContext.PackagePath+"."), GeneratedContracts: true}
			if err := refineComponentColumns(ctx, shapeComponent, source, declarations, bindings, resolver, gen.Input{ExternalHandler: factory, Views: linkedViews}); err != nil {
				return nil, err
			}
			runtime.Views = shapeComponent.Views
		}
		return nil, nil
	}
	settings := runtime.Settings
	if settings.Mutation != "" || settings.SequenceStrategy != "" || settings.IndependentChildTransactions || settings.Report != nil || settings.Cache != nil || settings.WarmupTarget != nil {
		return nil, fmt.Errorf("factory input shape cannot contain mutation, transaction, report, cache or warmup settings")
	}
	sqlSource := &spec.ViewSource{SQL: prepared.SQL, Embeds: dql.EmbeddedSQLRefs(prepared.SQL)}
	if err := dsql.ResolveSource(runtime.Name, sqlSource, source.Resources); err != nil {
		return nil, err
	}
	resolved := dql.PrepareSource(sqlSource.SQL)
	if err := resolved.Err(); err != nil {
		return nil, err
	}
	if err := validateFactoryProjection(resolved); err != nil {
		return nil, err
	}
	resolved.TypeContext = prepared.TypeContext
	shapeComponent.Settings.DefaultConnector = connector
	shapeComponent.RootView = &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: source.Scope, Name: runtime.Name}, Name: runtime.Name, Source: sqlSource}
	shapeSourceMap := newSourceMap(len(sqlSource.SQL), nil, resolved.TrimPrefix, sqlSource.SQL)
	generatedRows, err := collectGeneratedRowAuthority(shapeComponent, resolved, &declarationProducts{views: shapeComponent.Views}, resolver, source.Path, shapeSourceMap)
	if err != nil {
		return nil, err
	}
	root := generatedRows.root
	if generatedRows.plan == nil {
		root, err = (&readPlanCompiler{sourceMap: shapeSourceMap, path: source.Path, types: resolver}).compile(shapeComponent, resolved)
		if err != nil {
			return nil, err
		}
	}
	// Resolve qualified row names before ownership checks compare Go declarations.
	authority := &gen.Plan{Imports: prepared.TypeContext.Imports}
	ownedNames := map[string]string{}
	link := func(view *spec.View, canonical string) error {
		descriptor, err := resolver.Descriptor(canonical)
		if err != nil || descriptor == nil || descriptor.Key() != canonical || descriptor.Name == "" || descriptor.PkgPath == "" {
			return fmt.Errorf("factory input shape external type %q requires exact named struct authority: %v", canonical, err)
		}
		if _, err := xshape.New(descriptor, resolver.Descriptor).Fields(); err != nil {
			return fmt.Errorf("factory input shape external type %q requires a struct: %w", canonical, err)
		}
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		if prior := linkedViews[identity]; prior != nil && prior.DescriptorKey != canonical {
			return fmt.Errorf("factory input shape view %q has conflicting external type authorities", identity)
		}
		linkedViews[identity] = &gen.ViewReference{DescriptorKey: canonical}
		return nil
	}
	var localize func(*spec.View) error
	localize = func(view *spec.View) error {
		if view.TypeName != "" {
			canonical, err := authority.CanonicalType(prepared.TypeContext.PackagePath, view.TypeName)
			if err != nil {
				return err
			}
			pkg, name, err := program.Symbol(canonical)
			if err != nil || view == root && pkg != prepared.TypeContext.PackagePath {
				return fmt.Errorf("factory input shape type %q must belong to generated destination %q", view.TypeName, prepared.TypeContext.PackagePath)
			}
			if pkg != prepared.TypeContext.PackagePath {
				return link(view, canonical)
			}
			ownedNames[canonical] = name
			view.TypeName = name
		}
		for _, relation := range view.Relations {
			if relation != nil && relation.View != nil {
				if err := localize(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = localize(root); err != nil {
		return nil, err
	}
	if err = validateFactoryShape(root); err != nil {
		return nil, err
	}
	var validateLocalOwnership func(*spec.View, string) error
	validateLocalOwnership = func(view *spec.View, destination string) error {
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		if linkedViews[identity] != nil {
			return nil
		}
		if view.Dest != "" {
			destination = view.Dest
		}
		if err := ownership.validateShape(runtime.Name, &spec.View{TypeName: view.TypeName, Dest: destination}); err != nil {
			return err
		}
		for _, relation := range view.Relations {
			if relation != nil && relation.View != nil {
				if err := validateLocalOwnership(relation.View, destination); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = validateLocalOwnership(root, runtime.Settings.Generation.File("view", "views.go")); err != nil {
		return nil, err
	}
	bodyCount := 0
	for _, p := range runtime.Parameters {
		if p == nil || !strings.EqualFold(p.Source.Kind, "body") {
			continue
		}
		// The field destination remains the local SQL-derived type. An explicit
		// codec may use a different provider source, but must declare that exact
		// destination; native codec compilation validates its source and factory.
		destination := p.TypeExpr
		invalidOverride := strings.TrimSpace(p.OutputTypeExpr) != ""
		if p.Codec != nil {
			destination = p.OutputTypeExpr
			invalidOverride = strings.TrimSpace(destination) == ""
		}
		bodyType, typeErr := authority.CanonicalType(prepared.TypeContext.PackagePath, destination)
		rowType := prepared.TypeContext.PackagePath + "." + root.TypeName
		if typeErr != nil || (bodyType != "[]*"+rowType && bodyType != "*"+rowType) || invalidOverride || p.EmitOutput {
			return nil, fmt.Errorf("factory input shape body %s must directly reference generated *%s or []*%s", p.Name, root.TypeName, root.TypeName)
		}
		cardinality := spec.CardinalityMany
		if bodyType == "*"+rowType {
			cardinality = spec.CardinalityOne
		}
		if p.Cardinality != "" && !strings.EqualFold(p.Cardinality, string(cardinality)) {
			return nil, fmt.Errorf("factory input shape body %s cardinality conflicts with its declared type", p.Name)
		}
		root.Cardinality = cardinality
		bodyCount++
	}
	if bodyCount != 1 {
		return nil, fmt.Errorf("factory input shape requires exactly one generated body binding")
	}
	// Localize only names owned by this fresh SQL graph in the detached runtime.
	// Authored DQL and references to external or authored local types stay intact.
	for _, p := range runtime.Parameters {
		if p == nil {
			continue
		}
		for _, expression := range []*string{&p.TypeExpr, &p.OutputTypeExpr} {
			if strings.TrimSpace(*expression) == "" || *expression == "?" {
				continue
			}
			canonical, err := authority.CanonicalType(prepared.TypeContext.PackagePath, *expression)
			if err != nil {
				return nil, err
			}
			localized, err := (xshape.Resolver{Rewriter: func(name string) (string, error) {
				if local := ownedNames[name]; local != "" {
					return local, nil
				}
				return name, nil
			}}).Rewrite(canonical)
			if err != nil {
				return nil, err
			}
			// Preserve external spelling unless this expression references an owned row.
			if localized != canonical {
				*expression = localized
			}
		}
	}
	if source.ColumnRefiner == nil {
		return nil, fmt.Errorf("factory input shape requires native column discovery")
	}
	for _, view := range shapeComponent.Views {
		if view.TypeName == "" {
			continue
		}
		canonical, err := authority.CanonicalType(prepared.TypeContext.PackagePath, view.TypeName)
		if err != nil {
			return nil, err
		}
		if local := ownedNames[canonical]; local != "" {
			view.TypeName = local
		} else if pkg, _, err := program.Symbol(canonical); err != nil {
			return nil, err
		} else if pkg != prepared.TypeContext.PackagePath {
			if err := link(view, canonical); err != nil {
				return nil, err
			}
		}
	}
	shapeComponent.Parameters = runtime.Clone().Parameters
	shapeComponent.RootView = root
	rows, err := newPackageViewResolver(shapeComponent, resolver).typedInputDescriptors(prepared.TypeContext.PackagePath, bindings, ownership.authoredCurrent, ownedNames)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := link(row.view, row.descriptor.Key()); err != nil {
			return nil, err
		}
	}
	factory := &gen.ExternalHandler{Package: prepared.TypeContext.PackagePath, Name: strings.TrimPrefix(runtime.Routes[0].Handler, prepared.TypeContext.PackagePath+"."), GeneratedContracts: true, InputShape: root}
	if err = refineComponentColumns(ctx, shapeComponent, source, declarations, bindings, resolver, gen.Input{ExternalHandler: factory, Views: linkedViews}); err != nil {
		return nil, err
	}
	if err = authoring.BackfillRelationMetadata(shapeComponent, source.Resources); err != nil {
		return nil, err
	}
	if err = validateFactoryShape(shapeComponent.RootView); err != nil {
		return nil, err
	}
	if err = validateFactoryDiscoveredShape(shapeComponent.RootView); err != nil {
		return nil, err
	}
	runtime.Views = shapeComponent.Views
	// Resolve generated names and inherited destinations through the same planner
	// used for publication, including children without an explicit type/dest.
	factoryName := strings.TrimPrefix(runtime.Routes[0].Handler, prepared.TypeContext.PackagePath+".")
	plan, err := gen.New(gen.Input{Component: runtime, Declarations: declarations, Views: linkedViews, ViewBindings: bindings, TypeResolver: resolver, Resources: source.Resources, TargetPackage: prepared.TypeContext.PackagePath, ExternalHandler: &gen.ExternalHandler{Package: prepared.TypeContext.PackagePath, Name: factoryName, GeneratedContracts: true, InputShape: shapeComponent.RootView}}).Plan()
	if err != nil {
		return nil, err
	}
	bodyIdentities := map[string]bool{}
	var collect func(*spec.View)
	collect = func(view *spec.View) {
		identity, _ := view.Identity()
		if bodyIdentities[identity] {
			return
		}
		bodyIdentities[identity] = true
		for _, relation := range view.Relations {
			if relation != nil && relation.View != nil {
				collect(relation.View)
			}
		}
	}
	collect(shapeComponent.RootView)
	for _, view := range plan.Views {
		if bodyIdentities[view.Identity] && view.Ownership == gen.ViewGenerated {
			if err := ownership.validateShape(runtime.Name, &spec.View{TypeName: view.Type, Dest: view.Destination}); err != nil {
				return nil, err
			}
		}
	}

	return shapeComponent.RootView, nil
}

// validateFactoryProjection rejects executable syntax before connector lookup,
// including expanded resources. Native reader compilation owns graph syntax
// and metadata validation after this statement boundary check.
func validateFactoryProjection(prepared *dql.PreparedSource) error {
	if prepared.Directives == nil || len(prepared.Directives.Params) != 0 || len(prepared.Directives.Views) != 0 || !prepared.Directives.Settings.IsZero() || (prepared.Directives.Settings != nil && prepared.Directives.Settings.Mutation != "") || prepared.Directives.Route != nil || prepared.Directives.Static != nil || prepared.Directives.HandlerFactory != "" || prepared.Directives.HandlerName != "" || prepared.Directives.MCP != nil || prepared.Directives.MCPOnly || prepared.Directives.Internal || !prepared.Directives.Documentation.IsZero() || (prepared.TypeContext != nil && (prepared.TypeContext.PackagePath != "" || prepared.TypeContext.DefaultPackage != "" || len(prepared.TypeContext.Imports) != 0)) {
		return fmt.Errorf("factory input projection cannot contain expanded declarations")
	}
	if len(prepared.Statements) != 1 {
		return fmt.Errorf("factory input shape requires exactly one SELECT projection")
	}
	item := prepared.Statements[0]
	if item == nil || item.Kind != statement.KindRead || !item.TemplateBalanced || item.SQLStart != item.Start || item.SQLEnd != item.End {
		return fmt.Errorf("factory input shape cannot contain an executable template or service program")
	}
	return nil
}

func validateFactoryShape(view *spec.View) error {
	if view == nil || view.Source == nil {
		return fmt.Errorf("factory input shape requires a SQL-derived graph")
	}
	if view.EntityHooks != "" || view.QueueContract != "" || view.WriterActionPolicy != "" || view.Reconciliation != nil || view.MutationPredicateGroup != nil {
		return fmt.Errorf("factory input shape cannot contain writer execution metadata")
	}
	for _, column := range view.Columns {
		if column == nil || strings.Contains(column.Tag, `sql:"`) {
			return fmt.Errorf("factory input shape cannot contain runtime SQL tags")
		}
	}
	for _, relation := range view.Relations {
		if relation != nil && relation.View != nil {
			if err := validateFactoryShape(relation.View); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFactoryDiscoveredShape(view *spec.View) error {
	if len(view.Columns) == 0 {
		return fmt.Errorf("factory input shape has no discovered columns")
	}
	for _, column := range view.Columns {
		if column == nil || column.EffectiveType().IsZero() {
			return fmt.Errorf("factory input shape requires discovered column types")
		}
	}
	for _, relation := range view.Relations {
		if relation != nil && relation.View != nil {
			if err := validateFactoryDiscoveredShape(relation.View); err != nil {
				return err
			}
		}
	}
	return nil
}

// One compilation uses one build-selected declaration snapshot for both generated
// contracts and borrowed Current rows. It never survives into another compilation.
type factoryDeclarations struct {
	ctx       context.Context
	build     *gobuild.Context
	pkg       string
	types     map[string][]factoryDeclaration
	functions map[string]bool
}

type factoryDeclaration struct {
	file      string
	source    string
	generated bool
}

func (d *factoryDeclarations) load() error {
	if d.types != nil {
		return nil
	}
	selected, err := selectedFactoryPackage(d.ctx, d.build, d.pkg)
	if err != nil {
		return err
	}
	types := map[string][]factoryDeclaration{}
	functions := map[string]bool{}
	for _, file := range append(selected.GoFiles, selected.CgoFiles...) {
		data, err := os.ReadFile(filepath.Join(selected.Dir, file))
		if err != nil {
			return err
		}
		tree, err := parser.ParseFile(token.NewFileSet(), file, data, parser.ParseComments)
		if err != nil {
			return err
		}
		record := factoryDeclaration{file: file, source: string(data), generated: ast.IsGenerated(tree)}
		for _, declaration := range tree.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil {
				functions[fn.Name.Name] = true
			}
			group, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, declaration := range group.Specs {
				typ, ok := declaration.(*ast.TypeSpec)
				if ok {
					types[typ.Name.Name] = append(types[typ.Name.Name], record)
				}
			}
		}
	}
	d.types, d.functions = types, functions
	return nil
}

func (d *factoryDeclarations) validateShape(owner string, shape *spec.View, inherited ...string) error {
	destination := strings.TrimSpace(shape.Dest)
	if destination == "" && len(inherited) > 0 {
		destination = inherited[0]
	}
	for _, relation := range shape.Relations {
		if relation != nil && relation.View != nil {
			if err := d.validateShape(owner, relation.View, destination); err != nil {
				return err
			}
		}
	}
	if shape.TypeName == "" {
		return nil
	}
	if err := d.load(); err != nil {
		return err
	}
	for _, declaration := range d.types[shape.TypeName] {
		if filepath.Clean(declaration.file) != filepath.Clean(destination) || !strings.HasPrefix(declaration.source, "// Code generated by Datly for "+owner+"; DO NOT EDIT.\n") {
			return fmt.Errorf("factory input shape %s conflicts with authored or imported authority in %s", shape.TypeName, declaration.file)
		}
	}
	return nil
}

// Use the same build-selected declarations for borrowing and generation ownership.
type factoryPackageSelection struct {
	Dir               string
	GoFiles, CgoFiles []string
}

func selectedFactoryPackage(ctx context.Context, build *gobuild.Context, pkg string) (*factoryPackageSelection, error) {
	args := []string{"list", "-e", "-json"}
	if build.Tags != "" {
		args = append(args, "-tags", build.Tags)
	}
	// Apply the existing factory-selection readonly policy without overriding
	// vendor, explicit tags or an application-selected modfile.
	env := append(os.Environ(), build.Env...)
	flags := ""
	for _, value := range env {
		if v, ok := strings.CutPrefix(value, "GOFLAGS="); ok {
			flags = v
		}
	}
	mode := ""
	for _, flag := range strings.Fields(flags) {
		if value, ok := strings.CutPrefix(strings.Trim(flag, "\"'"), "-mod="); ok {
			mode = value
		}
	}
	if mode == "mod" {
		args = append(args, "-mod=readonly")
	}
	args = append(args, "--", pkg)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir, cmd.Env = build.Dir, env
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("factory input shape build selection: %w", err)
	}
	var selected factoryPackageSelection
	if err = json.Unmarshal(data, &selected); err != nil {
		return nil, err
	}
	return &selected, nil
}

func (d *factoryDeclarations) authoredCurrent(name string) (bool, error) {
	if !ast.IsExported(name) {
		return false, fmt.Errorf("factory Current type %s must be exported", name)
	}
	if err := d.load(); err != nil {
		return false, err
	}
	declarations := d.types[name]
	if len(declarations) > 1 {
		return false, fmt.Errorf("factory Current type %s has ambiguous selected ownership", name)
	}
	if len(declarations) == 0 {
		return false, nil
	}
	if declarations[0].generated {
		return false, fmt.Errorf("factory Current type %s is generated, not an authored borrowed row", name)
	}
	return true, nil
}

// Function kind is checked here; the staged Go build validates its signature.
func (d *factoryDeclarations) requireFunction(name string) error {
	if err := d.load(); err != nil {
		return err
	}
	if d.functions[name] {
		return nil
	}
	return fmt.Errorf("handler factory %s.%s must be a declared function in the selected build", d.pkg, name)
}
