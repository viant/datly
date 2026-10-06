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
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

// compileFactoryInputShape uses the existing reader parser and column-discovery
// owners in an isolated authoring component. Only the resulting shape transfers
// to generation: the executable factory component has no root or SQL capability.
func (c *Compiler) compileFactoryInputShape(ctx context.Context, source *Source, prepared *dql.PreparedSource, runtime *spec.Component, connector string, resolver *typecatalog.Resolver, build *gobuild.Context) (*spec.View, error) {
	if strings.TrimSpace(prepared.SQL) == "" {
		return nil, nil
	}
	settings := runtime.Settings
	if settings.Mutation != "" || settings.SequenceStrategy != "" || settings.IndependentChildTransactions || settings.Report != nil || settings.Cache != nil || settings.WarmupTarget != nil {
		return nil, fmt.Errorf("factory input shape cannot contain mutation, transaction, report, cache or warmup settings")
	}
	for _, p := range runtime.Parameters {
		if p == nil {
			continue
		}
		if p.DeclarationSQL != "" || strings.EqualFold(p.Source.Kind, "view") || strings.EqualFold(p.Name, "Current") || strings.EqualFold(p.Name, "Previous") {
			return nil, fmt.Errorf("factory input shape cannot contain executable or Current/Previous bindings")
		}
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
	authoring := runtime.Clone()
	authoring.Settings.DefaultConnector = connector
	authoring.RootView = &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: source.Scope, Name: runtime.Name}, Name: runtime.Name, Source: sqlSource}
	root, err := (&readPlanCompiler{sourceMap: newSourceMap(len(sqlSource.SQL), nil, resolved.TrimPrefix, sqlSource.SQL), path: source.Path, types: resolver}).compile(authoring, resolved)
	if err != nil {
		return nil, err
	}
	if err = validateFactoryShape(root); err != nil {
		return nil, err
	}
	if err = validateFactoryShapeOwnership(ctx, build, prepared.TypeContext.PackagePath, runtime.Name, root); err != nil {
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
	authoring.RootView = root
	// The projection is parameter-free. Discovery receives neither request values
	// nor a runtime view binding; its connector is isolated in authoring settings.
	if err = source.ColumnRefiner.BeginCompilation().RefineRoot(ctx, authoring, nil, nil); err != nil {
		return nil, err
	}
	if err = validateFactoryShape(authoring.RootView); err != nil {
		return nil, err
	}
	for _, column := range authoring.RootView.Columns {
		if column == nil || column.Type.IsZero() || strings.TrimSpace(column.DatabaseType) == "" {
			return nil, fmt.Errorf("factory input shape requires discovered column type metadata")
		}
	}
	return authoring.RootView, nil
}

// validateFactoryProjection rejects executable syntax before connector lookup,
// including expanded resources. It deliberately permits only a direct physical
// auxiliary leaf with named scalar columns and existing shape metadata calls.
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
	parsed, err := sqlparser.ParseQuery(prepared.SQL, sqlparser.WithStructuralValidation())
	if err != nil {
		return err
	}
	table, aux, err := sqlparser.SourceTable(parsed.From.X)
	if err != nil {
		return err
	}
	if table == "" || !aux || len(parsed.Joins) != 0 || parsed.Union != nil || len(parsed.WithSelects) != 0 || parsed.WithRecursive || parsed.Qualify != nil || len(parsed.GroupBy) != 0 || parsed.Having != nil || parsed.QualifyClause != nil || len(parsed.OrderBy) != 0 || parsed.Window != nil || parsed.Limit != nil || parsed.Offset != nil || parsed.Kind != "" {
		return fmt.Errorf("factory input shape requires a direct auxiliary leaf projection without query operations")
	}
	alias := strings.TrimSpace(parsed.From.Alias)
	if alias == "" {
		return fmt.Errorf("factory input projection requires an explicit source alias")
	}
	columns := 0
	for _, item := range parsed.List {
		if item == nil {
			return fmt.Errorf("factory input projection has an empty item")
		}
		switch value := item.Expr.(type) {
		case *expr.Selector:
			ident, ok := value.X.(*expr.Ident)
			if value.Name != alias || value.Expression != "" || !ok || ident.Name == "*" || item.Alias == "" {
				return fmt.Errorf("factory input projection requires named direct source columns")
			}
			columns++
		case *expr.Call:
			name, ok := value.X.(*expr.Ident)
			if !ok {
				return fmt.Errorf("factory input shape metadata must be a simple native directive")
			}
			arity := 0
			switch strings.ToLower(name.Name) {
			case "type", "dest", "tag":
				arity = 2
			case "required", "optional":
				arity = 1
			default:
				return fmt.Errorf("factory input shape cannot use directive or function %q", name.Name)
			}
			if len(value.Args) != arity || item.Alias != "" {
				return fmt.Errorf("factory input shape metadata has an invalid argument shape")
			}
			if arity == 2 {
				if _, ok := value.Args[1].(*expr.Literal); !ok {
					return fmt.Errorf("factory input shape metadata requires a literal")
				}
			}
			switch target := value.Args[0].(type) {
			case *expr.Ident:
				if target.Name != alias {
					return fmt.Errorf("factory input shape directive targets another source")
				}
			case *expr.Selector:
				_, ok := target.X.(*expr.Ident)
				if target.Name != alias || !ok || target.Expression != "" {
					return fmt.Errorf("factory input shape directive targets another column")
				}
			default:
				return fmt.Errorf("factory input shape directive has an invalid target")
			}
		default:
			return fmt.Errorf("factory input shape cannot contain expressions or executable SQL")
		}
	}
	if columns == 0 {
		return fmt.Errorf("factory input shape requires direct source columns")
	}
	return nil
}

func validateFactoryShape(view *spec.View) error {
	if view == nil || !view.Auxiliary || view.Source == nil || strings.TrimSpace(view.Source.Table) == "" || !token.IsIdentifier(view.TypeName) || !token.IsExported(view.TypeName) || len(view.Columns) == 0 {
		return fmt.Errorf("factory input shape must resolve one auxiliary leaf and a generated type")
	}
	if view.Source.URI != "" || len(view.Source.Embeds) != 0 || view.Source.Bindings != nil || !view.Source.Controls.IsZero() {
		return fmt.Errorf("factory input shape cannot contain resource or executable source bindings")
	}
	remainder := view.Clone()
	remainder.Key = spec.Key{}
	remainder.Name = ""
	remainder.Namespace = ""
	remainder.Auxiliary = false
	remainder.TypeName = ""
	remainder.Dest = ""
	remainder.Columns = nil
	remainder.Source = nil
	if !reflect.DeepEqual(remainder, &spec.View{}) {
		return fmt.Errorf("factory input shape cannot contain relations, lifecycle, selector or execution metadata")
	}
	for _, column := range view.Columns {
		if column == nil || column.Source == "" || column.Expression != "" || column.Codec != nil || column.DeleteMarker || column.ConcurrencyToken || column.ExplicitType || strings.TrimSpace(column.Tag) != `sqlx:"-"` {
			return fmt.Errorf("factory input shape must retain direct scalar source-column authority")
		}
	}
	return nil
}

// Current native-generated source is legitimate regeneration input. An authored
// declaration or alias of this exact local type would compete with SQL authority.
func validateFactoryShapeOwnership(ctx context.Context, build *gobuild.Context, pkg, owner string, shape *spec.View) error {
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
				if filepath.Clean(file) != filepath.Clean(shape.Dest) || !strings.HasPrefix(string(data), "// Code generated by Datly for "+owner+"; DO NOT EDIT.\n") {
					return fmt.Errorf("factory input shape %s conflicts with authored or imported authority in %s", shape.TypeName, file)
				}
			}
		}
	}
	return nil
}
