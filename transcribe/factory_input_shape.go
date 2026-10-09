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
	"github.com/viant/datly/typecatalog"
)

// compileFactoryInputShape uses the existing reader parser and column-discovery
// owners in an isolated authoring component. Only the resulting shape transfers
// to generation: the executable factory component has no body reader root.
// Independently declared input reads retain their ordinary runtime bindings.
func (c *Compiler) compileFactoryInputShape(ctx context.Context, source *Source, prepared *dql.PreparedSource, runtime *spec.Component, connector string, resolver *typecatalog.Resolver, build *gobuild.Context, declarations gen.Declarations, bindings gen.ViewBindings) (*spec.View, error) {
	if strings.TrimSpace(prepared.SQL) == "" {
		if len(runtime.Views) != 0 {
			if err := refineComponentColumns(ctx, runtime, source, declarations, bindings, resolver); err != nil {
				return nil, err
			}
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
	shapeComponent := runtime.Clone()
	shapeComponent.Settings.DefaultConnector = connector
	shapeComponent.RootView = &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: source.Scope, Name: runtime.Name}, Name: runtime.Name, Source: sqlSource}
	root, err := (&readPlanCompiler{sourceMap: newSourceMap(len(sqlSource.SQL), nil, resolved.TrimPrefix, sqlSource.SQL), path: source.Path, types: resolver}).compile(shapeComponent, resolved)
	if err != nil {
		return nil, err
	}
	if err = validateFactoryShape(root); err != nil {
		return nil, err
	}
	if err = validateFactoryShapeOwnership(ctx, build, prepared.TypeContext.PackagePath, runtime.Name, root, runtime.Settings.Generation.File("view", "views.go")); err != nil {
		return nil, err
	}
	bodyCount := 0
	for _, p := range runtime.Parameters {
		if p == nil || !strings.EqualFold(p.Source.Kind, "body") {
			continue
		}
		// The generated field must bind directly to the local SQL-derived type.
		// Output type, codec and emission controls cannot override or omit it.
		if strings.TrimSpace(p.TypeExpr) != "[]*"+root.TypeName || strings.TrimSpace(p.OutputTypeExpr) != "" || p.Codec != nil || p.EmitOutput {
			return nil, fmt.Errorf("factory input shape body %s must directly reference generated []*%s", p.Name, root.TypeName)
		}
		bodyCount++
	}
	if bodyCount != 1 {
		return nil, fmt.Errorf("factory input shape requires exactly one generated body collection")
	}
	if source.ColumnRefiner == nil {
		return nil, fmt.Errorf("factory input shape requires native column discovery")
	}
	shapeComponent.RootView = root
	if err = refineComponentColumns(ctx, shapeComponent, source, declarations, bindings, resolver); err != nil {
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
	plan, err := gen.New(gen.Input{Component: runtime, Declarations: declarations, ViewBindings: bindings, TypeResolver: resolver, TargetPackage: prepared.TypeContext.PackagePath, ExternalHandler: &gen.ExternalHandler{Package: prepared.TypeContext.PackagePath, Name: factoryName, GeneratedContracts: true, InputShape: shapeComponent.RootView}}).Plan()
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
			if err := validateFactoryShapeOwnership(ctx, build, prepared.TypeContext.PackagePath, runtime.Name, &spec.View{TypeName: view.Type, Dest: view.Destination}); err != nil {
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

// Current native-generated source is legitimate regeneration input. An authored
// declaration or alias of this exact local type would compete with SQL authority.
func validateFactoryShapeOwnership(ctx context.Context, build *gobuild.Context, pkg, owner string, shape *spec.View, inherited ...string) error {
	destination := strings.TrimSpace(shape.Dest)
	if destination == "" && len(inherited) > 0 {
		destination = inherited[0]
	}
	for _, relation := range shape.Relations {
		if relation != nil && relation.View != nil {
			if err := validateFactoryShapeOwnership(ctx, build, pkg, owner, relation.View, destination); err != nil {
				return err
			}
		}
	}
	if shape.TypeName == "" {
		return nil
	}
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
		return fmt.Errorf("factory input shape build selection: %w", err)
	}
	var selected struct {
		Dir               string
		GoFiles, CgoFiles []string
	}
	if err = json.Unmarshal(data, &selected); err != nil {
		return err
	}
	for _, file := range append(selected.GoFiles, selected.CgoFiles...) {
		data, err := os.ReadFile(filepath.Join(selected.Dir, file))
		if err != nil {
			return err
		}
		tree, err := parser.ParseFile(token.NewFileSet(), file, data, 0)
		if err != nil {
			return err
		}
		for _, decl := range tree.Decls {
			group, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, specification := range group.Specs {
				typ, ok := specification.(*ast.TypeSpec)
				if !ok || typ.Name.Name != shape.TypeName {
					continue
				}
				if filepath.Clean(file) != filepath.Clean(destination) || !strings.HasPrefix(string(data), "// Code generated by Datly for "+owner+"; DO NOT EDIT.\n") {
					return fmt.Errorf("factory input shape %s conflicts with authored or imported authority in %s", shape.TypeName, file)
				}
			}
		}
	}
	return nil
}
