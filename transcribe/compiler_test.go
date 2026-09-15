package transcribe

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestCompilerCompileAppliesSourceConnectorDefault(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope:     "example.com/demo",
		Name:      "Events",
		Connector: "analytics",
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.Source == nil || result.Component == nil {
		t.Fatalf("Compile() result = %+v", result)
	}
	if actual := result.Component.Settings.DefaultConnector; actual != "analytics" {
		t.Fatalf("DefaultConnector = %q, want analytics", actual)
	}
}

func TestCompilerCompilePreservesAuthoredConnector(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name:      "Events",
		Connector: "fallback",
		Text: `#setting($_ = $route('/events', 'GET'))
#setting($_ = $connector('primary'))
SELECT * FROM events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if actual := result.Component.Settings.DefaultConnector; actual != "primary" {
		t.Fatalf("DefaultConnector = %q, want primary", actual)
	}
}

func TestCompilerCompileSynthesizesConstantsAndResolvesReadTables(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', 'VENDOR'))
SELECT vendor.* FROM $Unsafe.Vendor vendor`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	component := result.Component
	if component == nil || component.RootView == nil || component.RootView.Source == nil || component.RootView.Source.Table != "VENDOR" {
		t.Fatalf("component root = %+v", component)
	}
	if len(component.Parameters) != 1 {
		t.Fatalf("params = %+v", component.Parameters)
	}
	param := component.Parameters[0]
	if param.Name != "Vendor" || param.Source != (spec.BindSource{Kind: "const", Name: "Vendor"}) ||
		param.TypeExpr != "string" || param.Value == nil || *param.Value != "VENDOR" || param.Tag != `internal:"true"` {
		t.Fatalf("constant param = %+v", param)
	}
}

func TestCompilerCompileRejectsAmbiguousConstantNames(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', 'VENDOR'))
#setting($_ = $const('vendor', 'OTHER'))
SELECT * FROM vendors`,
	})
	if err == nil || !strings.Contains(err.Error(), "constant names") {
		t.Fatalf("Compile() error = %v", err)
	}
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 || compileError.Diagnostics[0].Span.Start.Line != 3 {
		t.Fatalf("Compile() diagnostics = %#v", err)
	}
}

func TestCompilerCompileRejectsEmptyConstantTable(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', ''))
SELECT * FROM $Unsafe.Vendor`,
	})
	if err == nil || !strings.Contains(err.Error(), "empty identifier") {
		t.Fatalf("Compile() error = %v", err)
	}
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 || compileError.Diagnostics[0].Span.Start.Line != 2 {
		t.Fatalf("Compile() diagnostics = %#v", err)
	}
}

func TestCompilerCompileRejectsConflictingDeclaredConstantAtDeclarationSpan(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', 'VENDOR'))
#define($_ = $Vendor<string>(const/Vendor).Value('OTHER'))
SELECT * FROM $Unsafe.Vendor`,
	})
	if err == nil || !strings.Contains(err.Error(), "conflicting values") {
		t.Fatalf("Compile() error = %v", err)
	}
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 || compileError.Diagnostics[0].Span.Start.Line != 3 {
		t.Fatalf("Compile() diagnostics = %#v", err)
	}
}

func TestCompilerCompileCanonicalizesDeclaredConstantSourceName(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', 'VENDOR'))
#define($_ = $Vendor<string>(const/vendor).Value('VENDOR'))
SELECT * FROM $Unsafe.Vendor`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(result.Component.Parameters) != 1 || result.Component.Parameters[0].Source.Name != "Vendor" {
		t.Fatalf("params = %+v", result.Component.Parameters)
	}
}

func TestCompilerCompileAuthoredConstantOverridesPackageByCanonicalName(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Vendors",
		PackageComponent: &spec.Component{Settings: &spec.Settings{Const: map[string]string{
			"Vendor": "package_vendors", "PackageOnly": "kept",
		}}},
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('vendor', 'dql_vendors'))
SELECT * FROM $Unsafe.vendor`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	component := result.Component
	if len(component.Settings.Const) != 2 || component.Settings.Const["vendor"] != "dql_vendors" ||
		component.Settings.Const["PackageOnly"] != "kept" || component.RootView.Source.Table != "dql_vendors" {
		t.Fatalf("component = %+v", component)
	}
	if len(component.Parameters) != 2 || component.Parameters[1].Name != "vendor" || component.Parameters[1].Value == nil || *component.Parameters[1].Value != "dql_vendors" {
		t.Fatalf("params = %+v", component.Parameters)
	}
}

func TestCompilerCompileRejectsNilAndCanceledSources(t *testing.T) {
	if _, err := NewCompiler().Compile(context.Background(), nil); !errors.Is(err, ErrNilSource) {
		t.Fatalf("Compile(nil) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewCompiler().Compile(ctx, &Source{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Compile(canceled) error = %v", err)
	}
}

func TestCompilerCompileRejectsUnclosedReadTemplateFrame(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Orders", Path: "/tmp/orders.dql",
		Text: "#setting($_ = $route('/orders', 'GET'))\n#if($Enabled)\nSELECT * FROM orders",
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
		compileError.Diagnostics[0].Code != codeReadTemplateFrame ||
		compileError.Diagnostics[0].Path != "/tmp/orders.dql" || compileError.Diagnostics[0].Span.Start.Line != 2 {
		t.Fatalf("Compile() error = %v, diagnostics = %+v", err, compileError.Diagnostics)
	}
}

func TestCompilerCompilePreservesPrefixedCTEForDecomposedViews(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Orders", Path: "/tmp/orders.dql",
		Text: `#setting($_ = $route('/orders', 'GET'))
#set($criteria = $TenantID)
WITH orders_src AS (SELECT id, tenant_id FROM orders WHERE tenant_id = $criteria),
items_src AS (SELECT order_id, tenant_id FROM items WHERE tenant_id = $criteria)
SELECT orders.*, items.*
FROM orders_src orders
JOIN items_src items ON items.order_id = orders.id AND items.tenant_id = orders.tenant_id`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	root := result.Component.RootView
	if root == nil || len(root.Relations) != 1 {
		t.Fatalf("root = %+v", root)
	}
	for _, source := range []*spec.ViewSource{root.Source, root.Relations[0].View.Source} {
		if source == nil || !strings.HasPrefix(source.SQL, "#set($criteria = $TenantID)\nWITH orders_src AS") ||
			strings.Contains(source.SQL, " JOIN ") {
			t.Fatalf("source = %+v", source)
		}
	}
}

func TestCompilerCompileIncludesSourcePathInError(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "broken",
		Path: "/tmp/broken.dql",
		Text: "SELECT 1",
	})
	if err == nil || !strings.Contains(err.Error(), "/tmp/broken.dql") {
		t.Fatalf("Compile() error = %v", err)
	}
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() diagnostic error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Path != "/tmp/broken.dql" || diagnostic.Span.Start.Line != 1 || diagnostic.Span.Start.Char != 1 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompilePreservesResourceReferencesForLoadStage(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT * FROM (${embed:sql/events.sql}) events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	source := result.Component.RootSource()
	if source == nil || len(source.Embeds) != 1 || source.Embeds[0].Path != "sql/events.sql" ||
		!strings.Contains(source.SQL, source.Embeds[0].Raw) {
		t.Fatalf("RootSource() = %+v", source)
	}
}

func TestCompilerCompilePreservesResourceTokenWhileLoweringViewDirective(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT events.*, set_limit(events, 10)
FROM (${embed:sql/events.sql}) events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	source := result.Component.RootSource()
	if source == nil || len(source.Embeds) != 1 || !strings.Contains(source.SQL, source.Embeds[0].Raw) ||
		strings.Contains(strings.ToLower(source.SQL), "set_limit(") {
		t.Fatalf("RootSource() = %+v", source)
	}
}

func TestCompilerCompileLowersViewDecoratorsIntoCanonicalMetadata(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT events.*, allow_nulls(events), groupable(events),
allowed_order_by_columns(events, 'created:CREATED_AT'), cardinality(events, 'One')
FROM events events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	view := result.Component.RootView
	if view == nil || view.AllowNulls == nil || !*view.AllowNulls || view.Groupable == nil || !*view.Groupable ||
		view.Cardinality != spec.CardinalityOne || view.Selector == nil ||
		view.Selector.OrderAliases["created"] != "CREATED_AT" {
		t.Fatalf("RootView = %+v", view)
	}
	for _, name := range []string{"allow_nulls", "groupable", "allowed_order_by_columns", "cardinality"} {
		if strings.Contains(strings.ToLower(view.Source.SQL), name+"(") {
			t.Fatalf("compiled SQL retains %s: %s", name, view.Source.SQL)
		}
	}
}

func TestCompilerCompileLowersOuterNamedViewDirectives(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT wrapper.*, items.*, set_limit(wrapper, 50), use_cache(items, 'items')
FROM (SELECT o.* FROM orders o) wrapper
JOIN (SELECT i.* FROM items i) items
  ON items.order_id = wrapper.id`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	root := result.Component.RootView
	if root == nil || root.Source == nil || root.Source.Controls == nil || root.Source.Controls.Limit == nil || *root.Source.Controls.Limit != 50 || len(root.Relations) != 1 {
		t.Fatalf("RootView = %+v", root)
	}
	child := root.Relations[0].View
	if child == nil || child.Source == nil || child.Source.Bindings == nil || child.Source.Bindings.CacheName != "items" {
		t.Fatalf("child = %+v", child)
	}
	for _, SQL := range []string{root.Source.SQL, child.Source.SQL} {
		for _, name := range []string{"set_limit", "use_cache"} {
			if strings.Contains(strings.ToLower(SQL), name+"(") {
				t.Fatalf("compiled SQL retains %s: %s", name, SQL)
			}
		}
	}
}

func TestCompilerCompileNormalizesIndependentViewCardinality(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/generated/views",
		Name:  "ViewContracts",
		Text: `#setting($_ = $route('/views', 'GET'))
#define($_ = $Required<?>(view/Required).Required() /* SELECT id FROM required_rows r */)
#define($_ = $Optional<?>(view/Optional).Optional() /* SELECT id FROM optional_rows o */)
#define($_ = $ExplicitMany<?>(view/ExplicitMany).Required().Cardinality('Many') /* SELECT id FROM explicit_rows e */)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	cardinality := map[string]string{}
	for _, param := range spec.EffectiveParameters(result.Component.Parameters) {
		if param != nil {
			cardinality[param.Name] = param.Cardinality
		}
	}
	if cardinality["Required"] != string(spec.CardinalityOne) ||
		cardinality["Optional"] != string(spec.CardinalityMany) ||
		cardinality["ExplicitMany"] != string(spec.CardinalityMany) {
		t.Fatalf("cardinality = %+v", cardinality)
	}
}

func TestCompilerCompileRejectsControlsInsideDatabaseSQL(t *testing.T) {
	tests := []string{
		`WITH unused AS (SELECT a.*, use_cache(a, 'audit') FROM audit a) SELECT orders.* FROM orders orders`,
		`WITH source AS (SELECT a.*, use_cache(a, 'audit') FROM audit a) SELECT left_source.*, right_source.* FROM source left_source JOIN source right_source ON right_source.id = left_source.id`,
		`WITH source AS (SELECT a.*, use_cache(a, 'audit') FROM audit a) SELECT left_source.* FROM source left_source UNION ALL SELECT right_source.* FROM source right_source`,
		`SELECT wrapper.*, use_cache(wrapper, 'outer') FROM (SELECT o.*, use_cache(o, 'inner') FROM orders o) wrapper`,
	}
	for _, SQL := range tests {
		_, err := NewCompiler().Compile(context.Background(), &Source{
			Name: "Orders",
			Path: "/tmp/orders.dql",
			Text: "#setting($_ = $route('/orders', 'GET'))\n" + SQL,
		})
		var compileError *CompileError
		if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
			compileError.Diagnostics[0].Code != "DQL-VIEW-DIRECTIVE" || compileError.Diagnostics[0].Path != "/tmp/orders.dql" {
			t.Fatalf("Compile(%q) error = %#v", SQL, err)
		}
	}
}

func TestCompilerCompilePreservesNestedEmbedWhileLoweringDirective(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT orders.*, items.*, use_cache(items, 'items')
FROM orders orders
JOIN (SELECT i.* FROM (${embed:sql/items.sql}) i) items
  ON items.order_id = orders.id`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	child := result.Component.RootView.Relations[0].View
	if child.Source == nil || child.Source.Bindings == nil || child.Source.Bindings.CacheName != "items" ||
		len(child.Source.Embeds) != 1 || child.Source.Embeds[0].Raw != "${embed:sql/items.sql}" ||
		!strings.Contains(child.Source.SQL, child.Source.Embeds[0].Raw) || strings.Contains(strings.ToLower(child.Source.SQL), "use_cache(") {
		t.Fatalf("child source = %+v", child.Source)
	}
}

func TestCompilerCompileReportsNestedSourceParseFailure(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Orders",
		Path: "/tmp/orders.dql",
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT orders.*, items.* FROM orders orders
JOIN (SELECT i.*, use_cache(i, 'items') FROM items i WHERE ') items
  ON items.order_id = orders.id`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
		compileError.Diagnostics[0].Code != "DQL-SQL-PARSE" || compileError.Diagnostics[0].Path != "/tmp/orders.dql" {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestCompilerCompileKeepsUnionBranchScopesInternal(t *testing.T) {
	tests := []string{
		`WITH source AS (SELECT a.* FROM audit a)
SELECT left_source.*, use_cache(left_source, 'outer') FROM source left_source
UNION ALL
WITH source AS (SELECT b.* FROM backup b)
SELECT right_source.* FROM source right_source`,
		`WITH source AS (SELECT a.* FROM audit a)
SELECT left_source.*, use_cache(left_source, 'outer') FROM source left_source
UNION ALL
SELECT archived.* FROM archived archived
JOIN source internal_source ON internal_source.id = archived.id`,
	}
	for _, SQL := range tests {
		result, err := NewCompiler().Compile(context.Background(), &Source{
			Name: "Audit",
			Text: "#setting($_ = $route('/audit', 'GET'))\n" + SQL,
		})
		if err != nil {
			t.Fatalf("Compile(%q) error = %v", SQL, err)
		}
		root := result.Component.RootView
		if root.Source == nil || root.Source.Bindings == nil || root.Source.Bindings.CacheName != "outer" || len(root.Relations) != 0 {
			t.Fatalf("root = %+v", root)
		}
	}
}

func TestCompilerCompileBuildsTypeContextFromSourceAndImports(t *testing.T) {
	root := t.TempDir()
	sourcePath := root + "/events.dql"
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/events",
		Name:  "Events",
		Path:  sourcePath,
		Text: `#package('example.com/models')
#import('customer', 'example.com/models/customer')
#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	actual := result.TypeContext
	if actual == nil || actual.PackageDir != root || actual.PackagePath != "example.com/app/events" || actual.PackageName != "events" {
		t.Fatalf("TypeContext = %+v", actual)
	}
	if actual.DefaultPackage != "example.com/models" || len(actual.Imports) != 1 || actual.Imports[0].Alias != "customer" {
		t.Fatalf("TypeContext imports = %+v", actual)
	}
}

func TestCompilerCompileBuildsResolverFromShapeCatalog(t *testing.T) {
	type packageEvent struct{ Package bool }
	type dqlEvent struct{ DQL bool }
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(packageEvent{}), x.WithName("Event"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := catalog.Register(typecatalog.TypeOriginDQL, x.NewType(
		reflect.TypeOf(dqlEvent{}), x.WithName("Event"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name:  "Events",
		Types: catalog,
		Text: `#package('example.com/models')
#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.TypeResolver == nil {
		t.Fatal("TypeResolver is nil")
	}
	actual, err := result.TypeResolver.Resolve("Event")
	if err != nil || actual != "example.com/models.Event" {
		t.Fatalf("Resolve(Event) = %q, %v", actual, err)
	}
	actualType, err := result.TypeResolver.Type("Event")
	if err != nil || actualType != reflect.TypeOf(dqlEvent{}) {
		t.Fatalf("Type(Event) = %v, %v; want DQL authority", actualType, err)
	}
}

func TestCompilerCompileUsesPreparedSQLAndMapsItsFirstOffset(t *testing.T) {
	text := `#package('example.com/models')
#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`
	result, err := NewCompiler().Compile(context.Background(), &Source{Name: "Events", Text: text})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if result.PreparedSQL != "SELECT * FROM events" {
		t.Fatalf("PreparedSQL = %q", result.PreparedSQL)
	}
	position := result.SourceMap.Position(0)
	if position.Line != 3 || position.Char != 1 || position.Offset != strings.Index(text, "SELECT") {
		t.Fatalf("prepared SQL position = %+v", position)
	}
	if len(result.Statements) != 1 || result.Statements[0].Kind != statement.KindRead {
		t.Fatalf("Statements = %#v", result.Statements)
	}
	statementPosition := result.SourceMap.Position(result.Statements[0].Start)
	if statementPosition != position {
		t.Fatalf("statement position = %+v, want %+v", statementPosition, position)
	}
}

func TestCompilerCompileBuildsCanonicalCompoundRelations(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/demo", Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT o.*, i.*
FROM orders o
JOIN order_items i ON i.order_id = o.id AND i.tenant_id = o.tenant_id`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	root := result.Component.RootView
	if root.Namespace != "o" || root.Source.Table != "orders" || len(root.Relations) != 1 {
		t.Fatalf("root plan = %+v", root)
	}
	relation := root.Relations[0]
	if relation.Kind != spec.RelationKindSubview || relation.View == nil || relation.View.Namespace != "i" || len(relation.On) != 2 {
		t.Fatalf("relation = %+v", relation)
	}
}

func TestCompilerCompileCarriesIndependentViewOptionsIntoCanonicalView(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/demo", Name: "Users",
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Authorization<*AuthorizationRow>(view/authorization).WithURI('queries/authorization.sql').Connector('security').Cardinality('One').TypeName('AuthorizationRow').Dest('authorization.go').WithCache('authorization-cache').WithLimit(1).WithColumnType('Authorized','bool').WithColumnTag('Authorized','internal:"true"').WithColumnGroupable('Authorized',false) /* SELECT authorized FROM authorization a */)
SELECT id FROM users u`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(result.Component.Views) != 1 {
		t.Fatalf("Views = %+v", result.Component.Views)
	}
	view := result.Component.Views[0]
	if view.Name != "Authorization" || view.Namespace != "a" || view.TypeName != "AuthorizationRow" || view.Dest != "authorization.go" || view.Cardinality != spec.CardinalityOne {
		t.Fatalf("View = %+v", view)
	}
	if view.Source == nil || view.Source.URI != "queries/authorization.sql" || len(view.Source.Embeds) != 0 ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "security" || view.Source.Bindings.CacheName != "authorization-cache" ||
		view.Source.Controls == nil || view.Source.Controls.Limit == nil || *view.Source.Controls.Limit != 1 {
		t.Fatalf("View source = %+v", view.Source)
	}
	if len(view.Columns) != 1 || view.Columns[0].Type.Name != "bool" || view.Columns[0].Tag != `internal:"true"` ||
		view.Columns[0].Groupable == nil || *view.Columns[0].Groupable {
		t.Fatalf("View columns = %+v", view.Columns)
	}
}

func TestCompilerCompileMapsRelationFailureToAuthoredSource(t *testing.T) {
	text := `#setting($_ = $route('/orders', 'GET'))
SELECT o.*, i.* FROM orders o -- root view
JOIN items i ON i.order_id > o.id`
	_, err := NewCompiler().Compile(context.Background(), &Source{Path: "/tmp/orders.sql", Name: "Orders", Text: text})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	wantStart := strings.Index(text, "ON i.order_id > o.id")
	if diagnostic.Code != "DQL-REL-UNSUPPORTED" || diagnostic.Path != "/tmp/orders.sql" ||
		diagnostic.Span.Start.Offset != wantStart || diagnostic.Span.End.Offset != len(text) ||
		diagnostic.Span.Start.Line != 3 || diagnostic.Span.Start.Char != 14 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileRejectsAmbiguousReadStatementSelection(t *testing.T) {
	text := `#setting($_ = $route('/orders', 'GET'))
SELECT * FROM orders;
SELECT * FROM items`
	_, err := NewCompiler().Compile(context.Background(), &Source{Path: "/tmp/multiple.sql", Name: "Orders", Text: text})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != codeReadStatementSelection || diagnostic.Path != "/tmp/multiple.sql" || diagnostic.Span.Start.Line != 2 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileReportsInvalidImportAtAuthoredSpan(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Path: "/tmp/events.dql",
		Text: `#import('model')
#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != dql.DiagnosticInvalidImport || diagnostic.Path != "/tmp/events.dql" ||
		diagnostic.Span.Start.Line != 1 || diagnostic.Span.Start.Char != 1 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileReportsSettingErrorAtAuthoredSpan(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Path: "/tmp/events.dql",
		Text: `#setting($_ = $route('/events', 'GET'))
#setting($_ = $dest())
SELECT * FROM events`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != dql.DiagnosticParse || diagnostic.Span.Start.Line != 2 || diagnostic.Span.Start.Char != 1 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileReportsUnicodeCRLFDirectiveSpan(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: "-- café\r\n\t#import('model')\r\nSELECT 1",
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	start := compileError.Diagnostics[0].Span.Start
	if start.Line != 2 || start.Char != 2 {
		t.Fatalf("diagnostic start = %+v", start)
	}
}

func TestCompilerCompileProducesGenerationDeclarations(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $EventTypes<?>(param/Events) /*
SELECT Price, Timestamp FROM /EventsPerformance
*/)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	declaration, ok := result.Declarations[result.Component.Parameters[1].Identity()]
	if !ok || !reflect.DeepEqual(declaration.Projection, []gen.DeclarationProjection{{Name: "Price", Source: "Price"}, {Name: "Timestamp", Source: "Timestamp"}}) || declaration.NestedChildField != "EventsPerformance" {
		t.Fatalf("Declarations = %+v", result.Declarations)
	}
}

func TestCompilerCompileProducesIndependentViews(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/demo",
		Name:  "Users",
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Authorization<*Authorization>(view/authorization) /* SELECT authorized FROM auth */)
#define($_ = $Audit<[]Audit>(data_view/audit) /*
SELECT a.*, d.* FROM audit a JOIN audit_detail d ON d.audit_id = a.id
*/)
SELECT * FROM users u`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	component := result.Component
	if len(component.Views) != 2 || len(component.RootView.Relations) != 0 {
		t.Fatalf("component views = %+v, root relations = %+v", component.Views, component.RootView.Relations)
	}
	if component.Parameters[0].Source.Kind != "view" || component.Parameters[0].Source.Name != "Authorization" ||
		component.Parameters[1].Source.Kind != "view" || component.Parameters[1].Source.Name != "Audit" {
		t.Fatalf("view params = %+v", component.Parameters)
	}
	authorization := component.Views[0]
	if authorization.Key != (spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "Authorization"}) ||
		authorization.Name != "Authorization" || authorization.Namespace != "auth" || authorization.Source.Table != "auth" {
		t.Fatalf("authorization view = %+v", authorization)
	}
	audit := component.Views[1]
	if audit.Name != "Audit" || audit.Namespace != "a" || audit.Source.Table != "audit" || len(audit.Relations) != 1 ||
		audit.Relations[0].View == nil || audit.Relations[0].View.Name != "d" {
		t.Fatalf("audit view = %+v", audit)
	}
}

func TestCompilerCompileNormalizesOutputSummariesToCanonicalRelations(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/demo",
		Name:  "Users",
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Totals<Totals>(output/summary) /* SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent */)
#define($_ = $Bounds<Bounds>(output/summary) /* SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM ($View.Users.NonWindowSQL) parent */)
SELECT id FROM users`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	root := result.Component.RootView
	if root == nil || len(root.Relations) != 2 {
		t.Fatalf("root relations = %+v", root)
	}
	for i, expected := range []struct {
		holder string
		sql    string
	}{
		{holder: "Totals", sql: "SELECT COUNT(*) AS count FROM ($View.Users.NonWindowSQL) parent"},
		{holder: "Bounds", sql: "SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM ($View.Users.NonWindowSQL) parent"},
	} {
		relation := root.Relations[i]
		if relation == nil || relation.Kind != spec.RelationKindDerived || relation.Holder != expected.holder ||
			relation.Cardinality != spec.CardinalityOne || relation.View == nil || relation.View.Name != expected.holder ||
			relation.View.Source == nil || relation.View.Source.SQL != expected.sql {
			if relation != nil && relation.View != nil {
				t.Logf("actual summary source: %+v", relation.View.Source)
			}
			t.Fatalf("relation[%d] = %+v, expected SQL %q", i, relation, expected.sql)
		}
	}
}

func TestCompilerCompileRejectsIndependentViewNameCollision(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Users",
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Users<[]User>(view/users) /* SELECT * FROM archived_users */)
SELECT * FROM users`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
		!strings.Contains(compileError.Diagnostics[0].Message, `view "Users" is declared more than once`) {
		t.Fatalf("Compile() error = %#v", err)
	}
}

func TestCompilerCompileRejectsMalformedDeclarationSQL(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Path: "/tmp/events.dql",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Broken<?>(param/Events) /* {not-json} SELECT 1 */)
SELECT 1`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != diagnosticDeclarationSQL || diagnostic.Path != "/tmp/events.dql" || !strings.Contains(diagnostic.Message, "parameter Broken") ||
		diagnostic.Span.Start.Line != 2 || diagnostic.Span.Start.Char != 1 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileRejectsUnsupportedGeneratedHelperProjection(t *testing.T) {
	for _, projection := range []string{
		"SELECT COUNT(Id) AS Count FROM `/`",
		"SELECT ARRAY_AGG(Id) AS IDs, Name FROM `/`",
		"SELECT * FROM `/`",
	} {
		_, err := NewCompiler().Compile(context.Background(), &Source{
			Name: "Events",
			Text: "#setting($_ = $route('/events', 'POST'))\n" +
				"#define($_ = $Events<[]Event>(body/).Required())\n" +
				"#define($_ = $Derived<?>(param/Events) /* " + projection + " */)\n" +
				"SELECT 1",
		})
		var compileError *CompileError
		if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 ||
			compileError.Diagnostics[0].Code != diagnosticDeclarationSQL ||
			!strings.Contains(compileError.Diagnostics[0].Message, "generated StructQL helper requires an explicit type") {
			t.Fatalf("Compile(%q) error = %#v", projection, err)
		}
	}
}

func TestCompilerCompileUsesDistinctSpansForSameNameInputAndOutput(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<Event>(body/) /* SELECT 1 */)
#define($_ = $Events<Event>(body/).Output() /* {not-json} SELECT 1 */)
SELECT 1`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) != 1 {
		t.Fatalf("Compile() error = %#v", err)
	}
	diagnostic := compileError.Diagnostics[0]
	if diagnostic.Code != diagnosticDeclarationSQL || diagnostic.Span.Start.Line != 3 || diagnostic.Span.Start.Char != 1 {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestCompilerCompileRejectsUnclosedDeclarationSQLComment(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<Event>(body/) /* SELECT 1)
SELECT 1`,
	})
	var compileError *CompileError
	if !errors.As(err, &compileError) || len(compileError.Diagnostics) == 0 {
		t.Fatalf("Compile() error = %#v", err)
	}
	if compileError.Diagnostics[0].Severity != SeverityError || compileError.Diagnostics[0].Span.Start.Line != 2 {
		t.Fatalf("diagnostic = %+v", compileError.Diagnostics[0])
	}
}
