package transcribe

import (
	"context"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/x"
)

func transcribeSource(ctx context.Context, destination string, source *Source) (*GeneratedPackage, error) {
	return NewCompiler().Transcribe(ctx, Request{Source: source, Destination: destination})
}

func TestCompilerGenerateCarriesAcceptedCustomHandlerAsset(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	file, err := parser.ParseFile(token.NewFileSet(), "handler.go", `package authored

import "context"

func HandleOrders(ctx context.Context, input *OrdersInput) (*OrdersOutput, error) {
	return &OrdersOutput{}, nil
}
`, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse handler: %v", err)
	}
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'POST'))
#define($_ = $ID<int>(body/ID))
SELECT 1`,
		GoHandler: &gen.GoHandlerAsset{Entry: "HandleOrders", File: file},
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Result.Plan.Handler != "HandleOrders" || generated.Result.Plan.GoHandler == nil ||
		len(generated.Result.Files) == 0 {
		t.Fatalf("generated handler result = %+v", generated.Result)
	}
	component, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.RouterDest))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(component), "handler=HandleOrders") {
		t.Fatalf("component source:\n%s", component)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated custom handler module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateCarriesAcceptedVeltyHandlerAsset(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'POST'))
#define($_ = $Name<string>(query/name))
#define($_ = $Result<string>(output/body))
SELECT 1`,
		VeltyHandler: &gen.VeltyHandlerAsset{Template: `#set($Output.Result = $Input.Name)`},
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Result.Plan.Handler != "NewOrdersHandler" || generated.Result.Plan.VeltyHandler == nil {
		t.Fatalf("generated Velty handler result = %+v", generated.Result.Plan)
	}
	component, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.RouterDest))
	if err != nil || !strings.Contains(string(component), "handler=NewOrdersHandler") {
		t.Fatalf("component source = %q, %v", component, err)
	}
	template, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.VeltyHandler.ResourceDestination))
	if err != nil || string(template) != `#set($Output.Result = $Input.Name)` {
		t.Fatalf("Velty template = %q, %v", template, err)
	}
}

func TestCompilerGeneratePreservesEveryAuthoredRoute(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET', 'POST'))
#setting($_ = $api_key('X-Key', 'secret'))
#setting($_ = $cache(true, '5m'))
#settings($_ = $marshal('application/json','codec.JSON'))
#settings($_ = $unmarshal('application/json','codec.Input'))
#settings($_ = $unmarshal('application/xml','codec.XML'))
#settings($_ = $format('tabular_json'))
#settings($_ = $date_format('2006-01-02'))
#settings($_ = $case_format('lc'))
#define($_ = $ID<int>(query/id))
SELECT id FROM orders`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Result.Plan.Routes) != 2 || generated.Result.Plan.Routes[0].Method != "GET" || generated.Result.Plan.Routes[1].Method != "POST" {
		t.Fatalf("generated routes = %+v", generated.Result.Plan.Routes)
	}
	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), root, []string{"example.com/generated/generated"}, nil)
	if err != nil || len(sources) != 2 {
		t.Fatalf("discovered generated routes = %+v, %v", sources, err)
	}
	for index, method := range []string{"GET", "POST"} {
		component, resolveErr := sources[index].Resolve(nil, nil)
		if resolveErr != nil {
			t.Fatalf("Resolve() error = %v", resolveErr)
		}
		if component.Name != "Orders" || len(component.Routes) != 1 || component.Routes[0].Method != method ||
			component.Routes[0].Path != "/orders" || component.Routes[0].APIKeyHeader != "X-Key" || component.Routes[0].APIKeyValue != "secret" ||
			component.Settings == nil || component.Settings.CaseFormat != "lc" || component.Settings.Format != "tabular" ||
			component.Settings.DateFormat != "2006-01-02" || component.Settings.JSONMarshalType != "codec.JSON" ||
			component.Settings.JSONUnmarshalType != "codec.Input" || component.Settings.XMLUnmarshalType != "codec.XML" ||
			component.Settings.Cache == nil || !component.Settings.Cache.Enabled || component.Settings.Cache.TTL != "5m" ||
			len(component.Routes[0].MCP) != 0 {
			t.Fatalf("resolved route %d = routes:%+v settings:%+v", index, component.Routes, component.Settings)
		}
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated multi-route module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateRejectsAmbiguousMCPAcrossRoutes(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	_, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders",
		Text: `#setting($_ = $route('/orders', 'GET', 'POST'))
#settings($_ = $mcp('orders.search', 'Search orders'))
SELECT id FROM orders`,
	})
	if err == nil || !strings.Contains(err.Error(), "mcp directive is ambiguous across 2 routes") {
		t.Fatalf("Generate() error = %v", err)
	}
}

func TestCompilerGenerateTranscribesExplicitVeltyServiceProgram(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/events", Name: "Events",
		Text: `#setting($_ = $route('/events', 'POST'))
#define($_ = $Name<string>(body/Name))
#define($_ = $Result<string>(output/body))
$dml.Execute("INSERT INTO events(name) VALUES (?)", $Name);
#set($Output.Result = $Name)`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if generated.Result.Plan.Handler != "NewEventsHandler" || generated.Result.Plan.VeltyHandler == nil {
		t.Fatalf("generated Velty plan = %+v", generated.Result.Plan)
	}
	template, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.VeltyHandler.ResourceDestination))
	if err != nil || !strings.Contains(string(template), "$dml.Execute") || !strings.Contains(string(template), "$Output.Result") {
		t.Fatalf("generated template = %q, %v", template, err)
	}
	testSource := `package events

import "testing"

func TestTranscribedVeltyFactory(t *testing.T) {
	if _, err := NewEventsHandler(); err != nil {
		t.Fatal(err)
	}
}
`
	if err = os.WriteFile(filepath.Join(root, "generated", "transcribed_velty_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("transcribed Velty module does not compile and initialize: %v\n%s", runErr, output)
	}
}

func TestCompilerGeneratePreservesConnectorDefault(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope:     "example.com/demo",
		Name:      "Events",
		Connector: "analytics",
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT * FROM events`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if actual := generated.Result.Plan.Connector; actual != "analytics" {
		t.Fatalf("generated connector = %q, want analytics", actual)
	}
	if generated.Types == nil {
		t.Fatal("generated type catalog is nil")
	}
	typ, ok, err := generated.Types.Resolve(typecatalog.PackageAuthority, generated.Package.PkgPath+".EventsInput")
	if err != nil || !ok || typ == nil || typ.SynteticType == nil {
		t.Fatalf("generated input type = %#v, %v, %v", typ, ok, err)
	}
}

func TestCompilerGenerateEmitsCanonicalConstantInput(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/vendors", Name: "Vendors",
		Text: `#setting($_ = $route('/vendors', 'GET'))
#setting($_ = $const('Vendor', 'vendors'))
SELECT * FROM $Unsafe.Vendor`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	fields := generated.Result.Plan.Input.Fields
	if len(fields) != 1 || fields[0].Name != "Vendor" || fields[0].Type != "string" ||
		fields[0].Tag != `parameter:"Vendor,kind=const,in=Vendor,dataType=string,value=vendors" internal:"true"` {
		t.Fatalf("input fields = %+v", fields)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated constant module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateUsesDiscoveredCanonicalViewColumns(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE events (
		event_id INTEGER NOT NULL,
		name TEXT,
		score REAL,
		created_at TIMESTAMP
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/events", Name: "Events", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/events', 'GET'))
SELECT event_id, name, score, created_at FROM events`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Result.Plan.Views) != 1 || generated.Result.Plan.Views[0].Name != "EventsView" {
		t.Fatalf("Views = %+v", generated.Result.Plan.Views)
	}
	fields := generated.Result.Plan.Views[0].Fields
	if len(fields) != 4 || fields[0].Name != "EventId" || fields[0].Type != "*int" ||
		fields[1].Name != "Name" || fields[1].Type != "*string" ||
		fields[2].Name != "Score" || fields[2].Type != "*float64" ||
		fields[3].Name != "CreatedAt" || fields[3].Type != "*time.Time" {
		t.Fatalf("ViewFields = %+v", fields)
	}
	content, err := os.ReadFile(filepath.Join(root, "generated", generated.Result.Plan.ViewDest))
	if err != nil {
		t.Fatalf("read generated view: %v", err)
	}
	if !strings.Contains(string(content), "EventId *int") || !strings.Contains(string(content), "Name *string") ||
		!strings.Contains(string(content), `time "time"`) || !strings.Contains(string(content), "CreatedAt *time.Time") {
		t.Fatalf("generated view source:\n%s", content)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateUsesDiscoveredIndependentViewColumns(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		"CREATE TABLE users (id INTEGER NOT NULL)",
		"CREATE TABLE authorization (authorized BOOLEAN NOT NULL)",
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/users", Name: "Users", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Authorization<*Authorization>(view/Authorization) /* SELECT authorized FROM authorization */)
SELECT id FROM users`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	var independent *gen.ViewPlan
	for index := range generated.Result.Plan.Views {
		if generated.Result.Plan.Views[index].Name == "Authorization" {
			independent = &generated.Result.Plan.Views[index]
			break
		}
	}
	if independent == nil || independent.Ownership != gen.ViewGenerated || len(independent.Fields) != 1 ||
		independent.Fields[0].Name != "Authorized" || len(generated.Result.Plan.Input.Fields) != 1 ||
		generated.Result.Plan.Input.Fields[0].Type != "*Authorization" {
		t.Fatalf("independent generation = view:%+v input:%+v", independent, generated.Result.Plan.Input.Fields)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated independent-view module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateAppliesIndependentViewOptions(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		"CREATE TABLE users (id INTEGER NOT NULL)",
		"CREATE TABLE authorization (authorized BOOLEAN NOT NULL, subject TEXT NOT NULL)",
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/users", Name: "Users", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/users', 'GET'))
#define($_ = $Authorization<*AuthorizationRow>(view/Authorization).WithURI('queries/authorization.sql').Connector('main').Cardinality('One').TypeName('AuthorizationRow').Dest('authorization.go').WithCache('authorization-cache').WithLimit(1).ColumnType('authorized','bool').ColumnTag('authorized','internal:"true"').ColumnGroupable('authorized',false) /* SELECT authorized, subject FROM authorization */)
SELECT id FROM users`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	var independent *gen.ViewPlan
	for index := range generated.Result.Plan.Views {
		if generated.Result.Plan.Views[index].Name == "AuthorizationRow" {
			independent = &generated.Result.Plan.Views[index]
			break
		}
	}
	if independent == nil || independent.Destination != "authorization.go" || independent.Ownership != gen.ViewGenerated || len(independent.Fields) != 2 ||
		independent.Fields[0].Name != "Authorized" || independent.Fields[0].Type != "*bool" || !strings.Contains(independent.Fields[0].Tag, `internal:"true"`) ||
		strings.Contains(independent.Fields[0].Tag, `groupable:`) || generated.Result.Plan.Input.Fields[0].Type != "*AuthorizationRow" ||
		!strings.Contains(generated.Result.Plan.Input.Fields[0].Tag, `view:"Authorization,type=AuthorizationRow,dest=authorization.go,uri=queries/authorization.sql`) ||
		!strings.Contains(generated.Result.Plan.Input.Fields[0].Tag, `sql:"uri=datly_`) {
		t.Fatalf("independent generation = view:%+v input:%+v", independent, generated.Result.Plan.Input.Fields)
	}
	matchedSQL := false
	for _, file := range generated.Result.Plan.Resources.Files {
		if file.Content == "SELECT authorized, subject FROM authorization" {
			matchedSQL = true
		}
	}
	if !matchedSQL {
		t.Fatal("independent authored SQL missing from generated resource")
	}
	if _, err := os.Stat(filepath.Join(root, "generated", "authorization.go")); err != nil {
		t.Fatalf("generated independent view destination: %v", err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated independent-view module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateUsesNormalizedIndependentViewCardinality(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/views",
		Name:  "ViewContracts",
		Text: `#setting($_ = $route('/views', 'GET'))
#define($_ = $Required<?>(view/Required).Required() /* SELECT id FROM required_rows r */)
#define($_ = $Optional<?>(view/Optional).Optional() /* SELECT id FROM optional_rows o */)
#define($_ = $ExplicitMany<?>(view/ExplicitMany).Required().Cardinality('Many') /* SELECT id FROM explicit_rows e */)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	fields := map[string]gen.Field{}
	for _, field := range generated.Result.Plan.Input.Fields {
		fields[field.Name] = field
	}
	if fields["Required"].Type != "*RequiredView" || !strings.Contains(fields["Required"].Tag, "cardinality=one") ||
		fields["Optional"].Type != "[]*OptionalView" || !strings.Contains(fields["Optional"].Tag, "cardinality=many") ||
		fields["ExplicitMany"].Type != "[]*ExplicitManyView" || !strings.Contains(fields["ExplicitMany"].Tag, "cardinality=many") {
		t.Fatalf("input fields = %+v", generated.Result.Plan.Input.Fields)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated view-cardinality module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateUsesExactSameNameIndependentViewIdentity(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated/audit", Name: "AuditLog",
		PackageComponent: &spec.Component{Views: []*spec.View{{
			Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/generated/audit", Name: "Audit"},
			Name: "Audit", Namespace: "archive", TypeName: "ArchiveAudit",
			Columns: []*spec.Column{{Name: "ID", Source: "id", Type: spec.TypeRef{Name: "int"}}},
			Source:  &spec.ViewSource{SQL: "SELECT id FROM archived_audit archive"},
		}}},
		Text: `#setting($_ = $route('/audit', 'GET'))
#define($_ = $Audit<?>(view/Audit).TypeName('CurrentAudit') /* SELECT id FROM current_audit current */)
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Result.Plan.Input.Fields) != 1 || generated.Result.Plan.Input.Fields[0].Type != "[]*CurrentAudit" {
		t.Fatalf("input = %+v", generated.Result.Plan.Input.Fields)
	}
	viewNames := map[string]bool{}
	for _, view := range generated.Result.Plan.Views {
		viewNames[view.Name] = true
	}
	if len(generated.Result.Plan.Views) != 3 || !viewNames["CurrentAudit"] || !viewNames["ArchiveAudit"] {
		t.Fatalf("views = %+v", generated.Result.Plan.Views)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated same-name independent-view module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerCompileDiscoversParameterizedSQLWithGeneratedInput(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE events (id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	result, err := NewCompiler().Compile(ctx, &Source{
		Scope: "example.com/generated/events", Name: "Events", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/events', 'GET'))
#define($_ = $Table<string>(const/Table).Value('events'))
#define($_ = $IncludeName<bool>(query/includeName).Value('true'))
#define($_ = $ID<int>(query/id).Value('7'))
SELECT id
#if($IncludeName)
, name
#end
FROM $Unsafe.Table
WHERE id >= $ID`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	columns := result.Component.RootView.Columns
	if len(columns) != 2 || columns[0].Name != "id" || columns[1].Name != "name" {
		t.Fatalf("columns = %+v", columns)
	}
}

func TestCompilerGenerateBuildsNestedRelationViewTypes(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE orders (id INTEGER, tenant_id INTEGER, name TEXT)`,
		`CREATE TABLE order_items (order_id INTEGER, tenant_id INTEGER, product_id INTEGER, quantity INTEGER)`,
		`CREATE TABLE products (id INTEGER, name TEXT)`,
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT o.*, cardinality(p, 'One')
FROM orders o
JOIN order_items i ON i.order_id = o.id AND i.tenant_id = o.tenant_id
JOIN products p ON p.id = i.product_id`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	views := generated.Result.Plan.Views
	if len(views) != 3 || views[0].Name != "OrdersView" || views[1].Name != "IView" || views[2].Name != "PView" {
		t.Fatalf("Views = %+v", views)
	}
	rootRelation := views[0].Fields[len(views[0].Fields)-1]
	if rootRelation.Name != "I" || rootRelation.Type != "[]*IView" {
		t.Fatalf("root relation = %+v", rootRelation)
	}
	links, err := dtag.ParseRelation(reflect.StructTag(rootRelation.Tag).Get("on"))
	if err != nil || len(links) != 2 {
		t.Fatalf("root relation links = %+v, %v", links, err)
	}
	nested := views[1].Fields[len(views[1].Fields)-1]
	if nested.Name != "P" || nested.Type != "*PView" {
		t.Fatalf("nested relation = %+v", nested)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("nested generated module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateLowersProjectionExclusionsIntoViewTags(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE vendors (id INTEGER, name TEXT, internal_note TEXT)`,
		`CREATE TABLE products (id INTEGER, vendor_id INTEGER, name TEXT, internal_note TEXT)`,
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	compiler := NewCompiler()
	compiled, err := compiler.Compile(ctx, &Source{
		Scope: "example.com/generated/vendors", Name: "Vendors", Connector: "main",
		Types:         typecatalog.NewCatalog(),
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/vendors', 'GET'))
SELECT vendor.* EXCEPT internal_note,
  product.* EXCEPT (vendor_id, internal_note)
FROM vendors vendor
JOIN products product ON product.vendor_id = vendor.id`,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	component := compiled.Component
	if component.RootView.Source.Table != "vendors" || component.RootView.Source.SQL != "" ||
		len(component.RootView.Relations) != 1 || component.RootView.Relations[0].View.Source.Table != "products" {
		t.Fatalf("decomposed component = %+v", component.RootView)
	}
	generated, err := compiler.generateCompiled(ctx, root, compiled)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Result.Plan.Views) != 2 {
		t.Fatalf("views = %+v", generated.Result.Plan.Views)
	}
	assertInternal := func(view gen.ViewPlan, names ...string) {
		t.Helper()
		fields := map[string]gen.Field{}
		for _, field := range view.Fields {
			fields[field.Name] = field
		}
		for _, name := range names {
			field := fields[name]
			if !strings.Contains(field.Tag, `internal:"true"`) {
				t.Fatalf("view %s field %s = %+v", view.Name, name, field)
			}
		}
	}
	assertInternal(generated.Result.Plan.Views[0], "InternalNote")
	assertInternal(generated.Result.Plan.Views[1], "VendorId", "InternalNote")
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated projection-exclusion module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateUsesPerViewTypeAndDestinationDirectives(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx,
		`CREATE TABLE orders (id INTEGER, name TEXT)`,
		`CREATE TABLE order_items (order_id INTEGER, quantity INTEGER)`,
	); err != nil {
		t.Fatalf("create tables: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	generated, err := transcribeSource(ctx, root, &Source{
		Scope: "example.com/generated/orders", Name: "Orders", Connector: "main",
		ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		Text: `#setting($_ = $route('/orders', 'GET'))
SELECT o.*, type(o, 'OrderRow'), dest(o, 'orders.go'),
  type(i, 'ItemRow'), dest(i, 'items.go')
FROM orders o
JOIN order_items i ON i.order_id = o.id`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	views := generated.Result.Plan.Views
	if len(views) != 2 || views[0].Name != "OrderRow" || views[0].Destination != "orders.go" ||
		views[1].Name != "ItemRow" || views[1].Destination != "items.go" ||
		generated.Result.Plan.Output.Fields[1].Type != "[]*OrderRow" {
		t.Fatalf("generated view plans = %+v, output = %+v", views, generated.Result.Plan.Output.Fields)
	}
	for _, file := range []string{"orders.go", "items.go"} {
		if _, err = os.Stat(filepath.Join(root, "generated", file)); err != nil {
			t.Fatalf("generated %s: %v", file, err)
		}
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated module does not compile: %v\n%s", runErr, output)
	}
}

func TestCompilerGenerateIncludesInferredRoutePathParam(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/demo",
		Name:  "TeamMembers",
		Text: `#setting($_ = $route('/teams/{teamID}/members', 'GET'))
SELECT * FROM members`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	fields := generated.Result.Plan.Input.Fields
	if len(fields) != 1 || fields[0].Name != "TeamID" || fields[0].Type != "string" ||
		fields[0].Tag != `parameter:"teamID,kind=path,in=teamID,dataType=string,cardinality=One"` {
		t.Fatalf("InputFields = %+v", fields)
	}
}

func TestCompilerGenerateRejectsInvalidRoutePathParamAtAuthoredSource(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	_, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/demo",
		Name:  "Orders",
		Text: `#setting($_ = $route('/orders/{order-id}', 'GET'))
SELECT 1`,
	})
	if err == nil || !strings.Contains(err.Error(), "path placeholder \"order-id\"") {
		t.Fatalf("Generate() error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "generated", "input.go")); !os.IsNotExist(statErr) {
		t.Fatalf("invalid route must fail before generation, stat error = %v", statErr)
	}
}

func TestCompilerGenerateUsesShapeCatalog(t *testing.T) {
	type customer struct{ ID int }
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/generated\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(customer{}), x.WithName("Customer"), x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	generated, err := transcribeSource(context.Background(), root, &Source{
		Scope: "example.com/generated",
		Name:  "Customers",
		Types: catalog,
		Text: `#import('models','example.com/models')
#setting($_ = $route('/customers', 'POST'))
#define($_ = $Customer<?>(body/Customer).Tag('typeName:"models.Customer"'))
SELECT 1`,
	})
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Result.Plan.Input.Fields) != 1 || generated.Result.Plan.Input.Fields[0].Type != "models.Customer" {
		t.Fatalf("InputFields = %+v", generated.Result.Plan.Input.Fields)
	}
	if generated.Types != catalog {
		t.Fatal("Generate() replaced the supplied type catalog")
	}
}
