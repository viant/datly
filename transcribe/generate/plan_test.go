package generate

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

type generatedBootstrapRow struct{}

func testXDatlyModulePath() string {
	location, err := (testharness.GeneratedModule{}).DependencyDir("github.com/viant/xdatly")
	if err != nil {
		panic(err)
	}
	return location
}

func generatedGoModWithoutModule() string {
	return testharness.GeneratedGoModWithoutModule()
}

func generatedContract(typeName, destination string, fields ...Field) ContractPlan {
	return ContractPlan{Type: typeName, Destination: destination, Fields: fields, Ownership: ContractGenerated}
}

func TestResolvePlan_UsesExplicitSettings(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors', 'GET'))
#settings($_ = $meta('docs/orders.md'))
#setting($_ = $connector('analytics'))
#setting($_ = $useTemplate('patch'))
#setting($_ = $dest('vendor.go'))
#setting($_ = $input_dest('vendor_input.go'))
#setting($_ = $output_dest('vendor_output.go'))
#setting($_ = $router_dest('vendor_router.go'))
#setting($_ = $input_type('VendorInput'))
#setting($_ = $output_type('VendorOutput'))
#define($_ = $VendorID<int>(path/vendorID).Required())
#define($_ = $Name<string>(query/name).Optional().Tag('json:"name,omitempty"'))
#define($_ = $Data<[]*VendorView>(output/view))
#define($_ = $Status<string>(output/status))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/vendors", "VendorCatalog", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	assertly.AssertValues(t, "VendorCatalog", plan.ComponentName)
	assertly.AssertValues(t, "analytics", plan.Connector)
	assertly.AssertValues(t, "vendor.go", plan.ViewDest)
	assertly.AssertValues(t, "vendor_input.go", plan.Input.Destination)
	assertly.AssertValues(t, "vendor_output.go", plan.Output.Destination)
	assertly.AssertValues(t, "vendor_router.go", plan.RouterDest)
	assertly.AssertValues(t, "VendorInput", plan.Input.Type)
	assertly.AssertValues(t, "VendorOutput", plan.Output.Type)
	if len(plan.Input.Fields) != 2 {
		t.Fatalf("expected two input fields, got %d", len(plan.Input.Fields))
	}
	assertly.AssertValues(t, "VendorID", plan.Input.Fields[0].Name)
	assertly.AssertValues(t, "int", plan.Input.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"VendorID,kind=path,in=vendorID,dataType=int,required=true"`, plan.Input.Fields[0].Tag)
	assertly.AssertValues(t, "Name", plan.Input.Fields[1].Name)
	assertly.AssertValues(t, "string", plan.Input.Fields[1].Type)
	assertly.AssertValues(t, `parameter:"Name,kind=query,in=name,dataType=string,required=false" json:"name,omitempty"`, plan.Input.Fields[1].Tag)
	if len(plan.Output.Fields) != 2 {
		t.Fatalf("expected two output fields, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Data", plan.Output.Fields[0].Name)
	assertly.AssertValues(t, "[]*VendorView", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Data,kind=output,in=view,dataType=[]*VendorView" view:"VendorCatalog,dest=vendor.go" sql:"SELECT 1"`, plan.Output.Fields[0].Tag)
	assertly.AssertValues(t, "Status", plan.Output.Fields[1].Name)
	assertly.AssertValues(t, "string", plan.Output.Fields[1].Type)
}

func TestResolvePlan_UsesDefaults(t *testing.T) {
	component := &spec.Component{
		Name: "VendorCatalog",
	}

	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	assertly.AssertValues(t, "views.go", plan.ViewDest)
	assertly.AssertValues(t, "input.go", plan.Input.Destination)
	assertly.AssertValues(t, "output.go", plan.Output.Destination)
	assertly.AssertValues(t, "router.go", plan.RouterDest)
	assertly.AssertValues(t, "VendorCatalogInput", plan.Input.Type)
	assertly.AssertValues(t, "VendorCatalogOutput", plan.Output.Type)
}

func TestFieldTagRoundTripsCanonicalBindingMetadataThroughBindly(t *testing.T) {
	minimum, maximum, exact := 1, 3, 2
	required := false
	cacheable := false
	value := "id,name"
	param := &spec.Parameter{
		Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"},
		TypeExpr: "[]string", Cardinality: "Many", Required: &required, Cacheable: &cacheable,
		When: "enabled", Scope: "request", With: "Filter", ResourceRef: "assets:fields.sql", Value: &value, Async: true,
		ErrorStatusCode: 422, ErrorMessage: "bad fields",
		MinAllowedRecords: &minimum, MaxAllowedRecords: &maximum, ExpectedReturned: &exact,
	}
	field := reflect.StructField{Name: "Fields", Type: reflect.TypeOf([]string{}), Tag: reflect.StructTag(fieldTag(param, "fields", nil))}
	actual, ok, err := bindly.BindingSpecFromField(field)
	if err != nil {
		t.Fatalf("BindingSpecFromField() error = %v; tag=%s", err, field.Tag)
	}
	if !ok || actual.Location.Kind != "query" || actual.Location.In != "fields" || actual.DataType != "[]string" || actual.Cardinality != "Many" {
		t.Fatalf("unexpected identity/type metadata: %+v", actual)
	}
	if actual.MinAllowedRecords == nil || *actual.MinAllowedRecords != minimum || actual.MaxAllowedRecords == nil || *actual.MaxAllowedRecords != maximum || actual.ExpectedReturned == nil || *actual.ExpectedReturned != exact {
		t.Fatalf("lost count constraints: %+v", actual)
	}
	if actual.Required == nil || *actual.Required || actual.Cacheable == nil || *actual.Cacheable || !actual.Async {
		t.Fatalf("unexpected flags: %+v", actual)
	}
	if actual.When != "enabled" || actual.Scope != "request" || actual.With != "Filter" || actual.ResourceRef != "assets:fields.sql" || actual.DefaultValue != "id,name" {
		t.Fatalf("unexpected binding metadata: %+v", actual)
	}
	if actual.ErrorCode != 422 || actual.ErrorMessage != "bad fields" {
		t.Fatalf("unexpected error metadata: %+v", actual)
	}
}

func TestFieldTagRemovesPackageBindingTagsWhenRegeneratingContract(t *testing.T) {
	param := &spec.Parameter{
		Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"},
		Tag: `parameter:"Tenant,kind=path,in=tenantID" bind:"Tenant,kind=path,in=tenantID" json:"tenant,omitempty"`,
	}
	actual := fieldTag(param, "Tenant", nil)
	if strings.Count(actual, `parameter:"`) != 1 || strings.Contains(actual, `bind:"`) || !strings.Contains(actual, `json:"tenant,omitempty"`) {
		t.Fatalf("fieldTag() = %s", actual)
	}
}

func TestGeneratedFieldTagRoundTripsCanonicalProviderMetadata(t *testing.T) {
	param := &spec.Parameter{
		Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, TypeExpr: "[]string",
		Tag:           `codec:"stale" predicate:"stale" querySelector:"stale" desc:"stale" example:"stale" json:"fields,omitempty"`,
		Codec:         &spec.Codec{Body: "structql", Args: []string{"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1"}, OutputType: "[]string"},
		Predicates:    []*spec.Predicate{{Name: "contains", Group: 2, Args: []string{"u", "name,last"}}, {Name: "tenant", ApplyWhenAbsent: true, Args: []string{"tenant_id=7"}}},
		QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyFields},
		Description:   "selected fields", Example: "id,name",
	}
	metadata, err := canonicalFieldMetadata(param)
	if err != nil {
		t.Fatalf("canonicalFieldMetadata() error = %v", err)
	}
	field := reflect.StructField{
		Name: "Fields", Type: reflect.TypeOf([]string{}), Tag: reflect.StructTag(fieldTag(param, "fields", metadata)),
	}
	parsed, err := dtag.ParseField(field)
	if err != nil {
		t.Fatalf("ParseField() error = %v; tag=%s", err, field.Tag)
	}
	if strings.Count(string(field.Tag), `codec:"`) != 1 || strings.Count(string(field.Tag), `predicate:"`) != 2 ||
		strings.Count(string(field.Tag), `querySelector:"`) != 1 || !strings.Contains(string(field.Tag), `json:"fields,omitempty"`) {
		t.Fatalf("canonical field tags = %s", field.Tag)
	}
	if parsed.Codec == nil || parsed.Codec.Body != "structql" || parsed.Codec.OutputType != "[]string" || len(parsed.Codec.Arguments) != 1 ||
		len(parsed.Predicates) != 2 || parsed.Predicates[0].Args[1] != "name,last" || parsed.QuerySelector == nil || parsed.QuerySelector.View != "users" ||
		parsed.Description != "selected fields" || parsed.Example != "id,name" {
		t.Fatalf("round trip = %+v; tag=%s", parsed, field.Tag)
	}
}

func TestStructFileWithImportsSupportsBackticksInFieldTags(t *testing.T) {
	source := structFileWithImports("generated", "// Input is generated.", "Input", []Field{{
		Name: "ID", Type: "string", Tag: "parameter:\"id,kind=query,errorMessage=bad ` marker\"",
	}}, nil)
	if _, err := parser.ParseFile(token.NewFileSet(), "input.go", source, parser.AllErrors); err != nil {
		t.Fatalf("generated source is invalid: %v\n%s", err, source)
	}
}

func TestResolvePlanWithTypeResolver_ConcreteTypeFromTagTypeName(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/demo/body')
#import('models','example.com/demo/body/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/body", "BodyOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan, err := testPlanWithTypeResolver(t, component, func(name string) (reflect.Type, error) {
		if name != "models.Foo" {
			t.Fatalf("unexpected lookup name %q", name)
		}
		type Foo struct{}
		return reflect.TypeOf(Foo{}), nil
	})
	if err != nil {
		t.Fatalf("unexpected typed plan error: %v", err)
	}
	if len(plan.Input.Fields) != 1 {
		t.Fatalf("expected one input field, got %d", len(plan.Input.Fields))
	}
	assertly.AssertValues(t, "models.Foo", plan.Input.Fields[0].Type)
}

func TestResolvePlan_DefaultReaderOutputFields(t *testing.T) {
	component := &spec.Component{
		Name:     "VendorCatalog",
		RootView: &spec.View{Name: "vendor"},
	}

	plan := testPlan(t, component)
	if len(plan.Output.Fields) != 2 {
		t.Fatalf("expected two default output fields, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Status", plan.Output.Fields[0].Name)
	assertly.AssertValues(t, "string", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"status,kind=output,in=status"`, plan.Output.Fields[0].Tag)
	assertly.AssertValues(t, "Data", plan.Output.Fields[1].Name)
	assertly.AssertValues(t, "[]*VendorCatalogView", plan.Output.Fields[1].Type)
	assertly.AssertValues(t, `parameter:"view,kind=output,in=view" view:"vendor"`, plan.Output.Fields[1].Tag)
}

func TestResolvePlan_RootViewMetadataRoundTripsThroughPackageBootstrap(t *testing.T) {
	limit := 25
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/orders", Name: "Orders"},
		Name:     "Orders",
		Routes:   []*spec.Route{{Method: "GET", Path: "/orders"}},
		Settings: &spec.Settings{DefaultConnector: "analytics"},
		RootView: &spec.View{Name: "Orders", BatchSize: 32, BatchConcurrency: 3, Source: &spec.ViewSource{
			Table:    "orders",
			SQL:      "SELECT id FROM orders",
			URI:      "orders/orders.sql",
			Bindings: &spec.ViewBindings{Connector: "analytics", CacheName: "orders-cache"},
			Controls: &spec.ViewControls{Limit: &limit},
		}},
	}
	plan := testPlan(t, component)
	if plan.RootViewName != "Orders" || plan.RootSource != "orders/orders.sql" {
		t.Fatalf("root holder metadata = %+v", plan)
	}
	componentSource, err := componentFileText("orders", plan)
	if err != nil {
		t.Fatalf("componentFileText() error = %v", err)
	}
	for _, expected := range []string{"connector=analytics", "view=Orders", "source=orders/orders.sql"} {
		if !strings.Contains(componentSource, expected) {
			t.Fatalf("component source missing %q:\n%s", expected, componentSource)
		}
	}
	if len(plan.Output.Fields) != 2 {
		t.Fatalf("output fields = %+v", plan.Output.Fields)
	}
	outputType := reflect.StructOf([]reflect.StructField{
		{Name: "Status", Type: reflect.TypeOf(""), Tag: reflect.StructTag(plan.Output.Fields[0].Tag)},
		{Name: "Data", Type: reflect.TypeOf([]*generatedBootstrapRow{}), Tag: reflect.StructTag(plan.Output.Fields[1].Tag)},
	})
	resolved, err := (bootstrap.ContractResolver{
		Component:  &spec.Component{Key: component.Key, Name: component.Name},
		OutputType: xshape.Linked(outputType).Descriptor(),
	}).Resolve()
	if err != nil {
		t.Fatalf("ContractResolver.Resolve() error = %v", err)
	}
	view := resolved.RootView
	if view == nil || view.Name != "Orders" || view.Source == nil || view.Source.Table != "orders" ||
		view.Source.SQL != "SELECT id FROM orders" || view.Source.URI != "orders/orders.sql" || len(view.Source.Embeds) != 0 ||
		view.Source.Bindings == nil || view.Source.Bindings.Connector != "analytics" ||
		view.Source.Bindings.CacheName != "orders-cache" || view.Source.Controls == nil ||
		view.Source.Controls.Limit == nil || *view.Source.Controls.Limit != limit || view.BatchSize != 32 || view.BatchConcurrency != 3 {
		t.Fatalf("bootstrapped root view = %+v", view)
	}
}

func TestResolvePlan_ReplacesInheritedRootViewTagsWithCanonicalMetadata(t *testing.T) {
	component := &spec.Component{
		Name: "Orders",
		Parameters: []*spec.Parameter{{
			Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"},
			Tag: `parameter:"view,kind=output,in=view" view:"Old,table=old_orders" sql:"SELECT * FROM old_orders"`,
		}},
		RootView: &spec.View{Name: "Orders", Source: &spec.ViewSource{Table: "orders", SQL: "SELECT * FROM orders"}},
	}
	plan := testPlan(t, component)
	if len(plan.Output.Fields) != 1 {
		t.Fatalf("output fields = %+v", plan.Output.Fields)
	}
	fieldTag := reflect.StructTag(plan.Output.Fields[0].Tag)
	view, err := dtag.ParseView(fieldTag.Get("view"))
	if err != nil || view == nil || view.Name != "Orders" || view.Table != "orders" {
		t.Fatalf("view tag = %+v, %v (%s)", view, err, plan.Output.Fields[0].Tag)
	}
	sqlMetadata := dtag.ParseSQL(fieldTag.Get("sql"))
	if sqlMetadata == nil || sqlMetadata.Text != "SELECT * FROM orders" {
		t.Fatalf("SQL tag = %+v (%s)", sqlMetadata, plan.Output.Fields[0].Tag)
	}
}

func TestResolvePlan_PreservesInlineSQLAndURIInCanonicalTags(t *testing.T) {
	inlineSQL := "SELECT *\nFROM orders\nWHERE status = 'active'"
	component := &spec.Component{
		Name: "Orders",
		Parameters: []*spec.Parameter{{
			Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"},
		}},
		RootView: &spec.View{Name: "Orders", Source: &spec.ViewSource{
			SQL: inlineSQL, URI: "queries/orders.sql",
		}},
	}
	plan := testPlan(t, component)
	fieldTag := reflect.StructTag(plan.Output.Fields[0].Tag)
	view, err := dtag.ParseView(fieldTag.Get("view"))
	if err != nil || view == nil || view.URI != "queries/orders.sql" {
		t.Fatalf("view tag = %+v, %v (%s)", view, err, plan.Output.Fields[0].Tag)
	}
	sqlMetadata := dtag.ParseSQL(fieldTag.Get("sql"))
	if sqlMetadata == nil || sqlMetadata.Text != inlineSQL || sqlMetadata.URI != "" {
		t.Fatalf("SQL tag = %+v (%s)", sqlMetadata, plan.Output.Fields[0].Tag)
	}
}

func TestResolvePlan_GeneratesCanonicalViewColumns(t *testing.T) {
	groupable := true
	component := &spec.Component{
		Name: "Events",
		RootView: &spec.View{Name: "Events", Columns: []*spec.Column{
			{Name: "event_id", Source: "EVENT_ID", Type: spec.TypeRef{Name: "int"}, Groupable: &groupable, PrimaryKey: true, AutoIncrement: true},
			{Name: "created_at", Type: spec.TypeRef{Package: "time", Name: "Time"}, Nullable: true},
			{Name: "code", Type: spec.TypeRef{Name: "string"}, Unique: true, Tag: `json:"code" sqlx:"name=event_code,unique=false"`},
		}},
	}
	plan := testPlan(t, component)
	if len(plan.Views) != 1 || plan.Views[0].Name != "EventsView" {
		t.Fatalf("Views = %+v", plan.Views)
	}
	fields := plan.Views[0].Fields
	if len(fields) != 3 || fields[0].Name != "EventId" || fields[0].Type != "int" ||
		fields[0].Tag != `sqlx:"EVENT_ID,primaryKey=true,autoincrement=true" groupable:"true"` ||
		fields[1].Name != "CreatedAt" || fields[1].Type != "*time.Time" ||
		fields[2].Tag != `json:"code" sqlx:"name=event_code,unique=false"` {
		t.Fatalf("View fields = %+v", fields)
	}
	if len(plan.Imports) != 1 || plan.Imports[0] != (spec.ImportSpec{Alias: "time", Package: "time"}) {
		t.Fatalf("Imports = %+v", plan.Imports)
	}
	source := viewFile("events", plan)
	for _, expected := range []string{`import (`, `time "time"`, `EventId int`, `CreatedAt *time.Time`} {
		if !strings.Contains(source, expected) {
			t.Fatalf("view source missing %q:\n%s", expected, source)
		}
	}
}

func TestResolvePlan_GeneratesNestedRelationViewTypes(t *testing.T) {
	products := &spec.View{Name: "Products", Namespace: "p", Source: &spec.ViewSource{Table: "products"},
		Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}, {Name: "name", Type: spec.TypeRef{Name: "string"}}}}
	items := &spec.View{Name: "Items", Namespace: "i", Source: &spec.ViewSource{Table: "order_items", SQL: "SELECT * FROM order_items", URI: "queries/items.sql", Controls: &spec.ViewControls{OrderBy: "created_at DESC, id"}},
		Columns: []*spec.Column{{Name: "order_id", Type: spec.TypeRef{Name: "int"}}, {Name: "tenant_id", Type: spec.TypeRef{Name: "int"}}, {Name: "product_id", Type: spec.TypeRef{Name: "int"}}}}
	items.Relations = []*spec.Relation{{Name: "Products", Holder: "Product", Cardinality: spec.CardinalityOne, View: products,
		On: []*spec.RelationLink{{ParentNamespace: "i", ParentColumn: "product_id", ChildNamespace: "p", ChildColumn: "id"}}}}
	orders := &spec.View{Name: "Orders", Namespace: "o", Source: &spec.ViewSource{Table: "orders"},
		Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}, {Name: "tenant_id", Type: spec.TypeRef{Name: "int"}}},
		Relations: []*spec.Relation{{Name: "Items", Holder: "Items", Cardinality: spec.CardinalityMany, MatchStrategy: spec.MatchReadAll, View: items,
			On: []*spec.RelationLink{
				{ParentNamespace: "o", ParentColumn: "id", ChildNamespace: "i", ChildColumn: "order_id"},
				{ParentNamespace: "o", ParentColumn: "tenant_id", ChildNamespace: "i", ChildColumn: "tenant_id"},
			}}}}
	plan := testPlan(t, &spec.Component{Name: "Orders", RootView: orders})
	if len(plan.Views) != 3 || plan.Views[0].Name != "OrdersView" || plan.Views[1].Name != "ItemsView" || plan.Views[2].Name != "ProductsView" {
		t.Fatalf("Views = %+v", plan.Views)
	}
	rootRelation := plan.Views[0].Fields[2]
	if rootRelation.Name != "Items" || rootRelation.Type != "[]*ItemsView" {
		t.Fatalf("root relation = %+v", rootRelation)
	}
	metadata, err := dtag.ParseView(reflect.StructTag(rootRelation.Tag).Get("view"))
	if err != nil || metadata == nil || metadata.URI != "queries/items.sql" || metadata.Table != "order_items" ||
		metadata.OrderBy != "created_at DESC, id" || metadata.Match != "read_all" {
		t.Fatalf("view tag = %+v, %v\n%s", metadata, err, rootRelation.Tag)
	}
	sqlMetadata := dtag.ParseSQL(reflect.StructTag(rootRelation.Tag).Get("sql"))
	if sqlMetadata == nil || sqlMetadata.Text != "SELECT * FROM order_items" || sqlMetadata.URI != "" {
		t.Fatalf("SQL tag = %+v\n%s", sqlMetadata, rootRelation.Tag)
	}
	links, err := dtag.ParseRelation(reflect.StructTag(rootRelation.Tag).Get("on"))
	if err != nil || len(links) != 2 || links[1].Parent.Column != "tenant_id" || links[1].Child.Column != "tenant_id" {
		t.Fatalf("on tag = %+v, %v\n%s", links, err, rootRelation.Tag)
	}
	nested := plan.Views[1].Fields[3]
	if nested.Name != "Product" || nested.Type != "*ProductsView" {
		t.Fatalf("nested relation = %+v", nested)
	}
	source := viewFile("orders", plan)
	for _, expected := range []string{"type OrdersView struct", "Items []*ItemsView", "type ItemsView struct", "Product *ProductsView", "type ProductsView struct"} {
		if !strings.Contains(source, expected) {
			t.Fatalf("generated view source missing %q:\n%s", expected, source)
		}
	}
	if plan.Output.Fields[1].Type != "[]*OrdersView" {
		t.Fatalf("root output = %+v", plan.Output.Fields)
	}
	rootDir := t.TempDir()
	testharness.WriteGeneratedGoMod(t, rootDir)
	generatedDir := filepath.Join(rootDir, "generated")
	if _, err = EmitScaffold(generatedDir, plan); err != nil {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = rootDir
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("nested generated module does not compile: %v\n%s", runErr, output)
	}
}

func TestResolvePlan_RejectsRelationViewCycle(t *testing.T) {
	root := &spec.View{Name: "Root"}
	child := &spec.View{Name: "Child"}
	root.Relations = []*spec.Relation{{Name: "Child", Holder: "Child", View: child}}
	child.Relations = []*spec.Relation{{Name: "Root", Holder: "Root", View: root}}
	_, err := New(Input{Component: &spec.Component{Name: "Cycle", RootView: root}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "contains a cycle") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestResolvePlan_GeneratesSiblingRelations(t *testing.T) {
	items := &spec.View{Name: "Items", Source: &spec.ViewSource{Table: "items"}}
	audits := &spec.View{Name: "Audits", Source: &spec.ViewSource{Table: "audits"}}
	root := &spec.View{Name: "Orders", Relations: []*spec.Relation{
		{Name: "Items", Holder: "Items", View: items},
		{Name: "Audits", Holder: "Audits", Cardinality: spec.CardinalityOne, View: audits},
	}}
	plan := testPlan(t, &spec.Component{Name: "Orders", RootView: root})
	if len(plan.Views) != 3 || len(plan.Views[0].Fields) != 2 ||
		plan.Views[0].Fields[0].Type != "[]*ItemsView" || plan.Views[0].Fields[1].Type != "*AuditsView" {
		t.Fatalf("generated sibling views = %+v", plan.Views)
	}
}

func TestResolvePlan_EmitsExplicitViewTypesAndDestinations(t *testing.T) {
	product := &spec.View{Name: "Product", TypeName: "ProductRow", Dest: "products.go", Source: &spec.ViewSource{Table: "products"},
		Columns: []*spec.Column{{Name: "created_at", Type: spec.TypeRef{Package: "time", Name: "Time"}}}}
	item := &spec.View{Name: "Item", TypeName: "ItemRow", Source: &spec.ViewSource{Table: "items"}, Relations: []*spec.Relation{
		{Name: "Product", Holder: "Product", Cardinality: spec.CardinalityOne, View: product},
	}}
	root := &spec.View{Name: "Order", TypeName: "OrderRow", Dest: "orders.go", Source: &spec.ViewSource{Table: "orders"}, Relations: []*spec.Relation{
		{Name: "Items", Holder: "Items", View: item},
	}}
	plan := testPlan(t, &spec.Component{Name: "Orders", Settings: &spec.Settings{Generation: &spec.GenerationSettings{ViewFile: "ignored.go"}}, RootView: root})
	if plan.RootViewType != "OrderRow" || plan.ViewDest != "orders.go" ||
		len(plan.Views) != 3 || plan.Views[0].Name != "OrderRow" || plan.Views[0].Destination != "orders.go" ||
		plan.Views[1].Name != "ItemRow" || plan.Views[1].Destination != "orders.go" ||
		plan.Views[2].Name != "ProductRow" || plan.Views[2].Destination != "products.go" ||
		plan.Output.Fields[1].Type != "[]*OrderRow" {
		t.Fatalf("view plan = %+v, output = %+v", plan.Views, plan.Output.Fields)
	}
	rootTag, err := dtag.ParseView(reflect.StructTag(plan.Output.Fields[1].Tag).Get("view"))
	if err != nil || rootTag == nil || rootTag.TypeName != "OrderRow" || rootTag.Dest != "orders.go" {
		t.Fatalf("root view tag = %+v, %v", rootTag, err)
	}
	itemTag, err := dtag.ParseView(reflect.StructTag(plan.Views[0].Fields[0].Tag).Get("view"))
	if err != nil || itemTag == nil || itemTag.Dest != "orders.go" {
		t.Fatalf("inherited child view tag = %+v, %v", itemTag, err)
	}
	dir := t.TempDir()
	testharness.WriteGeneratedGoMod(t, dir)
	if _, err = EmitScaffold(filepath.Join(dir, "generated"), plan); err != nil {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	ordersSource, err := os.ReadFile(filepath.Join(dir, "generated", "orders.go"))
	if err != nil {
		t.Fatalf("read orders view: %v", err)
	}
	productsSource, err := os.ReadFile(filepath.Join(dir, "generated", "products.go"))
	if err != nil {
		t.Fatalf("read products view: %v", err)
	}
	if !strings.Contains(string(ordersSource), "type OrderRow struct") || !strings.Contains(string(ordersSource), "type ItemRow struct") ||
		strings.Contains(string(ordersSource), "type ProductRow struct") || strings.Contains(string(ordersSource), `"time"`) ||
		!strings.Contains(string(productsSource), "type ProductRow struct") || !strings.Contains(string(productsSource), `time "time"`) {
		t.Fatalf("orders source:\n%s\nproducts source:\n%s", ordersSource, productsSource)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = dir
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("split generated views do not compile: %v\n%s", runErr, output)
	}
}

func TestGeneratorRequiresExplicitMigrationForChangedViewDestination(t *testing.T) {
	dir := t.TempDir()
	component := func(childDest string) *spec.Component {
		return &spec.Component{
			Name: "Orders",
			RootView: &spec.View{Name: "Orders", Dest: "orders.go", Relations: []*spec.Relation{{
				Name: "Items", Holder: "Items", View: &spec.View{Name: "Items", Dest: childDest},
			}}},
		}
	}
	if _, err := New(Input{Component: component("items.go")}).Generate(dir); err != nil {
		t.Fatalf("first Generate() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "items.go")); err != nil {
		t.Fatalf("first child destination: %v", err)
	}
	if _, err := New(Input{Component: component("")}).Generate(dir); err == nil || !strings.Contains(err.Error(), "explicit migration required") {
		t.Fatalf("destination tag change error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "items.go")); err != nil {
		t.Fatalf("existing child destination removed: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "orders.go"))
	if err != nil || strings.Contains(string(content), "type ItemsView struct") {
		t.Fatalf("failed migration modified parent shape = %v\n%s", err, content)
	}
}

func TestResolvePlan_RejectsInvalidViewTypeAndDestination(t *testing.T) {
	for _, testCase := range []struct {
		name string
		view *spec.View
	}{
		{name: "unexported type", view: &spec.View{Name: "Orders", TypeName: "orderRow"}},
		{name: "invalid type", view: &spec.View{Name: "Orders", TypeName: "Order-Row"}},
		{name: "escaping destination", view: &spec.View{Name: "Orders", Dest: "../orders.go"}},
		{name: "nested destination", view: &spec.View{Name: "Orders", Dest: "views/orders.go"}},
		{name: "non-Go destination", view: &spec.View{Name: "Orders", Dest: "orders.txt"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(Input{Component: &spec.Component{Name: "Orders", RootView: testCase.view}}).Plan()
			if err == nil {
				t.Fatal("expected planning error")
			}
		})
	}
}

func TestResolvePlan_ValidatesComponentHolderDestination(t *testing.T) {
	valid, err := New(Input{Component: &spec.Component{
		Name: "Orders", Settings: &spec.Settings{Generation: &spec.GenerationSettings{RouterFile: "custom_routes.go"}},
	}}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if valid.RouterDest != "custom_routes.go" {
		t.Fatalf("RouterDest = %q", valid.RouterDest)
	}
	for _, destination := range []string{"routes/orders.go", "orders.txt", "../orders.go"} {
		t.Run(destination, func(t *testing.T) {
			_, err := New(Input{Component: &spec.Component{
				Name: "Orders", Settings: &spec.Settings{Generation: &spec.GenerationSettings{RouterFile: destination}},
			}}).Plan()
			if err == nil || !strings.Contains(err.Error(), "must be a package-local .go file") {
				t.Fatalf("Plan() error = %v", err)
			}
		})
	}
}

func TestEmitScaffoldValidatesComponentHolderDestination(t *testing.T) {
	plan := &Plan{
		ComponentName: "Orders", RouterDest: "routes/orders.go",
		Input:  generatedContract("OrdersInput", "input.go"),
		Output: generatedContract("OrdersOutput", "output.go"),
	}
	if _, err := EmitScaffold(t.TempDir(), plan); err == nil || !strings.Contains(err.Error(), "must be a package-local .go file") {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
}

func TestResolvePlan_RejectsViewContractAndDestinationCollisions(t *testing.T) {
	_, err := New(Input{Component: &spec.Component{Name: "Orders", RootView: &spec.View{Name: "Orders", TypeName: "OrdersInput"}}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "input contract") {
		t.Fatalf("type collision error = %v", err)
	}
	_, err = New(Input{Component: &spec.Component{
		Name: "Orders", Settings: &spec.Settings{Generation: &spec.GenerationSettings{OutputFile: "orders.go"}},
		RootView: &spec.View{Name: "Orders", Dest: "orders.go"},
	}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "generated destination") {
		t.Fatalf("destination collision error = %v", err)
	}
}

func TestResolvePlan_RejectsSharedRelationView(t *testing.T) {
	shared := &spec.View{Name: "Line"}
	root := &spec.View{Name: "Orders", Relations: []*spec.Relation{
		{Name: "Items", Holder: "Items", View: shared},
		{Name: "Returns", Holder: "Returns", View: shared},
	}}
	_, err := New(Input{Component: &spec.Component{Name: "Orders", RootView: root}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "shares view") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestResolvePlan_RejectsGeneratedViewAndFieldCollisions(t *testing.T) {
	t.Run("view type", func(t *testing.T) {
		root := &spec.View{Name: "Root", Relations: []*spec.Relation{
			{Name: "First", Holder: "First", View: &spec.View{Name: "Line"}},
			{Name: "Second", Holder: "Second", View: &spec.View{Name: "Line"}},
		}}
		_, err := New(Input{Component: &spec.Component{Name: "Root", RootView: root}}).Plan()
		if err == nil || !strings.Contains(err.Error(), "map to type") {
			t.Fatalf("Plan() error = %v", err)
		}
	})
	t.Run("relation field", func(t *testing.T) {
		root := &spec.View{Name: "Root", Columns: []*spec.Column{{Name: "items"}}, Relations: []*spec.Relation{
			{Name: "Items", Holder: "Items", View: &spec.View{Name: "Line"}},
		}}
		_, err := New(Input{Component: &spec.Component{Name: "Root", RootView: root}}).Plan()
		if err == nil || !strings.Contains(err.Error(), "collides with scalar field") {
			t.Fatalf("Plan() error = %v", err)
		}
	})
}

func TestResolvePlan_RelationHoldersUseComponentCaseFormat(t *testing.T) {
	child := &spec.View{Name: "SupplyPublisher", Source: &spec.ViewSource{Table: "publishers"},
		Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}
	root := &spec.View{Name: "Supply", Source: &spec.ViewSource{Table: "supply"},
		Columns: []*spec.Column{{Name: "publisher_id", Type: spec.TypeRef{Name: "int"}}},
		Relations: []*spec.Relation{{Name: "SupplyPublisher", Holder: "SupplyPublisher", Cardinality: spec.CardinalityOne, View: child,
			On: []*spec.RelationLink{{ParentColumn: "publisher_id", ChildColumn: "id"}}}}}

	plan := testPlan(t, &spec.Component{Name: "Supply", Settings: &spec.Settings{CaseFormat: "lc"}, RootView: root})
	if len(plan.Views) == 0 || len(plan.Views[0].Fields) < 2 {
		t.Fatalf("generated views = %+v", plan.Views)
	}
	holder := plan.Views[0].Fields[1]
	if holder.Name != "SupplyPublisher" || reflect.StructTag(holder.Tag).Get("json") != "supplyPublisher" {
		t.Fatalf("relation holder tag = %q", holder.Tag)
	}
}

func TestResolvePlan_GeneratesTypedSelfReference(t *testing.T) {
	root := &spec.View{Name: "Category", SelfReference: &spec.SelfReference{Holder: "Children", Child: "ID", Parent: "ParentID"}}
	plan := testPlan(t, &spec.Component{Name: "Categories", RootView: root})
	if len(plan.Views) != 1 || len(plan.Views[0].Fields) != 1 {
		t.Fatalf("generated views = %+v", plan.Views)
	}
	field := plan.Views[0].Fields[0]
	if field.Name != "Children" || field.Type != "[]*CategoriesView" ||
		reflect.StructTag(field.Tag).Get("self") != "child=ID,parent=ParentID" {
		t.Fatalf("self field = %+v", field)
	}
}

func TestEmitScaffold_WritesPlannedFiles(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		Routes:        []RoutePlan{{Method: "GET", Path: "/v1/api/vendors"}},
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output:        generatedContract("VendorOutput", "vendor_output.go"),
	}
	dir := t.TempDir()

	files, err := EmitScaffold(dir, plan)
	if err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected three non-empty files, got %d", len(files))
	}

	inputBytes, err := os.ReadFile(filepath.Join(dir, "vendor_input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	assertly.AssertValues(t, "package vendor_catalog\n\n// VendorInput is the generated input scaffold for VendorCatalog.\ntype VendorInput struct{}\n", string(inputBytes))

	componentBytes, err := os.ReadFile(filepath.Join(dir, "vendor_router.go"))
	if err != nil {
		t.Fatalf("failed to read generated component holder: %v", err)
	}
	tag, err := generatedComponentContractTag(componentBytes)
	if err != nil {
		t.Fatalf("failed to parse generated component tag: %v", err)
	}
	assertly.AssertValues(t, dtag.Component{Name: "VendorCatalog", Path: "/v1/api/vendors", Method: "GET"}, tag)
}

func TestEmitScaffoldEphemeralWritesNoManifest(t *testing.T) {
	plan := &Plan{
		ComponentName: "Records", Routes: []RoutePlan{{Method: "GET", Path: "/records"}},
		RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"),
	}
	dir := t.TempDir()
	if _, err := EmitScaffoldEphemeral(dir, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, scaffoldManifestName)); !os.IsNotExist(err) {
		t.Fatalf("ephemeral generation persisted ownership manifest: %v", err)
	}
}

func TestComponentFilePreservesCanonicalHolderMetadata(t *testing.T) {
	plan := &Plan{
		ComponentName: "Orders", Description: "Order lookup", Example: `{"id":1}`,
		Routes: []RoutePlan{{Method: "GET", Path: "/orders", Marshaller: "tabular"}}, Handler: "HandleOrders", Connector: "analytics",
		RootViewName: "Orders", RootSource: "queries/orders.sql",
		Report: &spec.ReportSettings{
			Enabled: true, LinkedInputType: "CubeInput",
			InputLayout: &spec.ReportInputLayout{
				Dimensions: "Dims", Measures: "Measures", Filters: "Filters",
				OrderBy: "Sort", Limit: "Take", Offset: "Skip",
			},
		},
		Input: generatedContract("OrdersInput", "input.go"), Output: generatedContract("OrdersOutput", "output.go"),
	}
	source, err := componentFileText("orders", plan)
	if err != nil {
		t.Fatalf("componentFileText() error = %v", err)
	}
	actual, err := generatedComponentContractTag([]byte(source))
	if err != nil {
		t.Fatalf("generatedComponentContractTag() error = %v", err)
	}
	want := dtag.Component{
		Name: "Orders", Description: "Order lookup", Example: `{"id":1}`,
		Path: "/orders", Method: "GET", Marshaller: "tabular", Handler: "HandleOrders", Connector: "analytics",
		View: "Orders", Source: "queries/orders.sql", Report: true, ReportLinkedInputType: "CubeInput",
		ReportDimensions: "Dims", ReportMeasures: "Measures", ReportFilters: "Filters",
		ReportOrderBy: "Sort", ReportLimit: "Take", ReportOffset: "Skip",
	}
	assertly.AssertValues(t, want, actual)
}

func TestPlanIsolatesReportInputLayout(t *testing.T) {
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme", Name: "Orders"},
		Name: "Orders",
		Settings: &spec.Settings{Report: &spec.ReportSettings{
			Enabled: true,
			InputLayout: &spec.ReportInputLayout{
				Dimensions: "Groups", Measures: "Metrics",
			},
		}},
		RootView: &spec.View{Name: "Orders"},
	}
	plan, err := New(Input{Component: component, TargetPackage: "example.com/acme"}).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan.Report == nil || plan.Report.InputLayout == nil {
		t.Fatalf("plan report settings = %+v", plan.Report)
	}
	plan.Report.InputLayout.Dimensions = "Changed"
	if component.Settings.Report.InputLayout.Dimensions != "Groups" {
		t.Fatalf("plan aliases source report input layout: %+v", component.Settings.Report.InputLayout)
	}
}

func TestComponentFilePreservesDisabledReportFacets(t *testing.T) {
	plan := &Plan{
		ComponentName: "Orders", Routes: []RoutePlan{{Method: "GET", Path: "/orders"}},
		Report: &spec.ReportSettings{LinkedInputType: "CubeInput"},
		Input:  generatedContract("OrdersInput", "input.go"), Output: generatedContract("OrdersOutput", "output.go"),
	}
	source, err := componentFileText("orders", plan)
	if err != nil {
		t.Fatalf("componentFileText() error = %v", err)
	}
	actual, err := generatedComponentContractTag([]byte(source))
	if err != nil {
		t.Fatalf("generatedComponentContractTag() error = %v", err)
	}
	if actual.Report || actual.ReportLinkedInputType != "CubeInput" {
		t.Fatalf("disabled report facets = %+v", actual)
	}
}

func TestEmitScaffoldFailureLeavesPreviousPackageIntact(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(dir, "router.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "blocked"), []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	failed := *plan
	failed.ComponentName = "Changed"
	failed.Input = plan.Input
	failed.Input.Destination = "blocked/input.go"
	if _, err = EmitScaffold(dir, &failed); err == nil {
		t.Fatal("expected staged write failure")
	}
	after, err := os.ReadFile(filepath.Join(dir, "router.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatal("failed generation changed the previous package")
	}
	blocked, err := os.ReadFile(filepath.Join(dir, "blocked"))
	if err != nil || string(blocked) != "not a directory" {
		t.Fatalf("unmanaged package file changed: %q, %v", blocked, err)
	}
}

func TestEmitScaffoldRejectsTargetSymlink(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "generated")
	if err := os.Symlink(realDir, target); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(target, plan); err == nil || !strings.Contains(err.Error(), "unsupported symlink") {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	if info, err := os.Lstat(target); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("target symlink changed: info=%v err=%v", info, err)
	}
}

func TestEmitScaffoldRejectsNestedSymlink(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(dir, "linked")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), "unsupported symlink") {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("nested symlink changed: info=%v err=%v", info, err)
	}
}

func TestEmitScaffoldSerializesConcurrentTargetWriters(t *testing.T) {
	dir := t.TempDir()
	const writers = 12
	errors := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(index int) {
			name := fmt.Sprintf("Users%d", index)
			snake := lowerSnake(name)
			_, err := EmitScaffold(dir, &Plan{
				ComponentName: name, ViewDest: snake + ".go", RouterDest: snake + "_router.go",
				Input:  generatedContract(name+"Input", snake+"_input.go"),
				Output: generatedContract(name+"Output", snake+"_output.go"),
			})
			errors <- err
		}(i)
	}
	succeeded := 0
	for i := 0; i < writers; i++ {
		if err := <-errors; err == nil {
			succeeded++
		} else if !strings.Contains(err.Error(), "generated package is owned by component") {
			t.Fatal(err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful writers = %d, want 1", succeeded)
	}
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Owner == "" || len(manifest.Files) != 3 {
		t.Fatalf("manifest files = %v", manifest.Files)
	}
	for _, name := range manifest.Files {
		if _, err = os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("committed file %q is missing: %v", name, err)
		}
	}
}

func TestEmitScaffoldRejectsUnownedGeneratedFileCollision(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "router.go"), []byte("package userowned\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), `generated file "router.go" collides with an unowned package file`) {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "router.go"))
	if err != nil || string(content) != "package userowned\n" {
		t.Fatalf("unowned file changed: %q, %v", content, err)
	}
}

func TestEmitScaffoldRejectsUnownedLinkedContractRemoval(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "input.go"), []byte("package userowned\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input:  ContractPlan{Type: "contracts.Input", Destination: "input.go", Ownership: ContractLinked},
		Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), `generated file "input.go" collides with an unowned package file`) {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "input.go")); err != nil {
		t.Fatalf("unowned linked contract was removed: %v", err)
	}
}

func TestEmitScaffoldRejectsUntrackedFileInOwnedPackage(t *testing.T) {
	dir := t.TempDir()
	initial := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, initial); err != nil {
		t.Fatal(err)
	}
	untracked := filepath.Join(dir, "custom.go")
	if err := os.WriteFile(untracked, []byte("package users\n\nconst Custom = true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed := *initial
	changed.RouterDest = "custom.go"
	if _, err := EmitScaffold(dir, &changed); err == nil || !strings.Contains(err.Error(), `generated file "custom.go" collides with an unowned package file`) {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	content, err := os.ReadFile(untracked)
	if err != nil || string(content) != "package users\n\nconst Custom = true\n" {
		t.Fatalf("untracked package file changed: %q, %v", content, err)
	}
}

func TestEmitScaffoldRejectsUntrackedRemovalInOwnedPackage(t *testing.T) {
	dir := t.TempDir()
	initial := &Plan{
		ComponentName: "Users", ViewDest: "users.go", RouterDest: "router.go",
		Input: generatedContract("UsersInput", "generated_input.go"), Output: generatedContract("UsersOutput", "output.go"),
	}
	if _, err := EmitScaffold(dir, initial); err != nil {
		t.Fatal(err)
	}
	untracked := filepath.Join(dir, "input.go")
	if err := os.WriteFile(untracked, []byte("package users\n\ntype UsersInput struct{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed := *initial
	changed.Input = ContractPlan{Type: "contracts.Input", Destination: "input.go", Ownership: ContractLinked}
	if _, err := EmitScaffold(dir, &changed); err == nil || !strings.Contains(err.Error(), `generated file "input.go" collides with an unowned package file`) {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	content, err := os.ReadFile(untracked)
	if err != nil || string(content) != "package users\n\ntype UsersInput struct{}\n" {
		t.Fatalf("untracked package file changed: %q, %v", content, err)
	}
}

func generatedComponentContractTag(componentSource []byte) (dtag.Component, error) {
	tags, err := generatedComponentContractTags(componentSource)
	if err != nil || len(tags) == 0 {
		return dtag.Component{}, err
	}
	return tags[0], nil
}

func generatedComponentContractTags(componentSource []byte) ([]dtag.Component, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "component.go", componentSource, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	var result []dtag.Component
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name == nil || typeSpec.Name.Name != "Component" {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, field := range structType.Fields.List {
				if len(field.Names) == 0 || !strings.HasPrefix(field.Names[0].Name, "Contract") || field.Tag == nil {
					continue
				}
				literal, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					return nil, err
				}
				tag, present, err := dtag.ParseComponent(reflect.StructTag(literal))
				if err != nil {
					return nil, err
				}
				if present {
					result = append(result, tag)
				}
			}
		}
	}
	return result, nil
}

func TestResolvePlanPreservesAllCanonicalRoutes(t *testing.T) {
	component := &spec.Component{Name: "Orders", Routes: []*spec.Route{
		{Name: "List", Method: "get", Path: "/orders", Internal: true, Marshaller: "json", Handler: "HandleOrders", APIKeyHeader: "X-Key", APIKeyValue: " read-key ", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "orders.list"}}},
		{Name: "Create", Method: "POST", Path: "/orders", Marshaller: "tabular", Handler: "HandleOrders", APIKeyHeader: "X-Key", APIKeyValue: "write-key"},
	}}
	plan := testPlan(t, component)
	if plan.Handler != "HandleOrders" || len(plan.Routes) != 2 {
		t.Fatalf("routes = %+v, handler = %q", plan.Routes, plan.Handler)
	}
	want := []RoutePlan{
		{Name: "List", Method: "GET", Path: "/orders", Internal: true, Marshaller: "json", APIKeyHeader: "X-Key", APIKeyValue: " read-key ", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "orders.list"}}},
		{Name: "Create", Method: "POST", Path: "/orders", Marshaller: "tabular", APIKeyHeader: "X-Key", APIKeyValue: "write-key", MCP: []*spec.MCPExposure{}},
	}
	if !reflect.DeepEqual(plan.Routes, want) {
		t.Fatalf("route plans = %+v", plan.Routes)
	}
	plan.Routes[0].MCP[0].Name = "changed"
	if component.Routes[0].MCP[0].Name != "orders.list" {
		t.Fatal("resolved route plan aliases canonical route exposure")
	}
}

func TestResolvePlanRejectsInvalidCanonicalRoutes(t *testing.T) {
	tests := []struct {
		name   string
		routes []*spec.Route
		match  string
	}{
		{name: "nil", routes: []*spec.Route{nil}, match: "route 1 is nil"},
		{name: "incomplete", routes: []*spec.Route{{Method: "GET"}}, match: "requires method and path"},
		{name: "duplicate", routes: []*spec.Route{{Method: "GET", Path: "/orders"}, {Method: "get", Path: "/orders"}}, match: "declared more than once"},
		{name: "handler", routes: []*spec.Route{{Method: "GET", Path: "/orders", Handler: "Read"}, {Method: "POST", Path: "/orders", Handler: "Write"}}, match: "conflicting handlers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Input{Component: &spec.Component{Name: "Orders", Routes: test.routes}}).Plan()
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("Plan() error = %v", err)
			}
		})
	}
}

func TestComponentFileEmitsAllCanonicalRoutes(t *testing.T) {
	plan := &Plan{
		ComponentName: "Orders", Handler: "HandleOrders",
		Routes: []RoutePlan{
			{Name: "List", Method: "GET", Path: "/orders", Internal: true, Marshaller: "json", APIKeyHeader: "X-Key", APIKeyValue: "read-key", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "orders.list"}}},
			{Name: "Create", Method: "POST", Path: "/orders", Marshaller: "tabular", APIKeyHeader: "X-Key", APIKeyValue: "write-key"},
		},
		Input: generatedContract("OrdersInput", "input.go"), Output: generatedContract("OrdersOutput", "output.go"),
	}
	source, err := componentFileText("orders", plan)
	if err != nil {
		t.Fatalf("componentFileText() error = %v", err)
	}
	if !strings.Contains(source, "Contract1 xdatly.Component") || !strings.Contains(source, "Contract2 xdatly.Component") {
		t.Fatalf("multi-route holder source:\n%s", source)
	}
	tags, err := generatedComponentContractTags([]byte(source))
	if err != nil || len(tags) != 2 {
		t.Fatalf("generated tags = %+v, %v", tags, err)
	}
	if tags[0].RouteName != "List" || tags[0].APIKeyValue != "read-key" || tags[0].Handler != "HandleOrders" ||
		tags[1].RouteName != "Create" || tags[1].APIKeyValue != "write-key" || tags[1].Handler != "HandleOrders" {
		t.Fatalf("generated tags = %+v", tags)
	}
	if !tags[0].Internal || len(tags[0].MCP) != 1 || tags[1].Internal {
		t.Fatalf("generated visibility = %+v", tags)
	}
}

func TestEmitScaffold_InvalidRouteTagValueFails(t *testing.T) {
	plan := &Plan{
		ComponentName: "Vendor,Catalog",
		Routes:        []RoutePlan{{Method: "GET", Path: "/v1/api/vendors"}},
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output:        generatedContract("VendorOutput", "vendor_output.go"),
	}
	if _, err := EmitScaffold(t.TempDir(), plan); err == nil {
		t.Fatalf("expected invalid component tag value error")
	}
}

func TestEmitScaffold_IncompleteRouteFails(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		Routes:        []RoutePlan{{Method: "GET"}},
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output:        generatedContract("VendorOutput", "vendor_output.go"),
	}
	if _, err := EmitScaffold(t.TempDir(), plan); err == nil {
		t.Fatal("expected incomplete route to fail")
	}
}

func TestEmitScaffold_WritesRouteMetadata(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		Routes:        []RoutePlan{{Method: "GET", Path: "/v1/api/vendors"}},
		Connector:     "analytics",
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output:        generatedContract("VendorOutput", "vendor_output.go"),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	routerBytes, err := os.ReadFile(filepath.Join(dir, "vendor_router.go"))
	if err != nil {
		t.Fatalf("failed to read generated router file: %v", err)
	}
	routeTag, err := generatedComponentContractTag(routerBytes)
	if err != nil || routeTag.Name != "VendorCatalog" || routeTag.Path != "/v1/api/vendors" || routeTag.Method != "GET" || routeTag.Connector != "analytics" {
		t.Fatalf("generated holder tag = %+v, %v\n%s", routeTag, err, routerBytes)
	}
	if _, err = os.Stat(filepath.Join(dir, "component.go")); !os.IsNotExist(err) {
		t.Fatalf("unexpected duplicate component.go: %v", err)
	}
}

func TestEmitScaffoldMigratesOwnedDuplicateComponentHolder(t *testing.T) {
	dir := t.TempDir()
	fingerprints := map[string]string{}
	for name, content := range map[string]string{
		"component.go": "package users\n\ntype Component struct{}\n",
		"router.go":    "package users\n\ntype Route struct{}\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		fingerprints[name] = scaffoldFingerprint([]byte(content))
	}
	if err := writeScaffoldManifest(dir, "Users", []string{"component.go", "router.go"}, &scaffoldManifest{Roles: map[string]string{"component.go": "artifact", "router.go": "artifact"}, Fingerprints: fingerprints}); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{
		ComponentName: "Users", Routes: []RoutePlan{{Method: "GET", Path: "/users"}},
		RouterDest: "router.go",
		Input:      ContractPlan{Type: "Input", Ownership: ContractLinked},
		Output:     ContractPlan{Type: "Output", Ownership: ContractLinked},
	}
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "component.go")); !os.IsNotExist(err) {
		t.Fatalf("stale component.go was not removed: %v", err)
	}
	holder, err := os.ReadFile(filepath.Join(dir, "router.go"))
	if err != nil {
		t.Fatalf("router holder = %q, %v", holder, err)
	}
	routeTag, err := generatedComponentContractTag(holder)
	if err != nil || routeTag.Name != "Users" || routeTag.Path != "/users" || routeTag.Method != "GET" {
		t.Fatalf("router holder tag = %+v, %v", routeTag, err)
	}
}

func TestEmitScaffold_WritesTypedInputFields(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input: generatedContract("VendorInput", "vendor_input.go",
			Field{Name: "VendorID", Type: "int", Tag: `parameter:"vendorID,kind=path,in=vendorID"`},
			Field{Name: "Name", Type: "string", Tag: `parameter:"name,kind=query,in=name"`},
		),
		Output: generatedContract("VendorOutput", "vendor_output.go"),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	inputBytes, err := os.ReadFile(filepath.Join(dir, "vendor_input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	setterBytes, err := os.ReadFile(filepath.Join(dir, "input_setters.go"))
	if err != nil {
		t.Fatalf("failed to read generated input setters: %v", err)
	}
	content += "\n" + string(setterBytes)
	for _, expected := range []string{
		"type VendorInputHas struct {\n\tVendorID bool\n\tName bool\n}",
		"func (input *VendorInput) SetVendorID(value int)",
		"input.Has = &VendorInputHas{}",
		"input.Has.VendorID = true",
		"func (input *VendorInput) SetName(value string)",
		"input.Has.Name = true",
	} {
		if !containsNormalized(content, expected) {
			t.Fatalf("generated input missing %q:\n%s", expected, content)
		}
	}
}

func TestEmitScaffold_WritesTypedOutputFields(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output: generatedContract("VendorOutput", "vendor_output.go",
			Field{Name: "Data", Type: "[]*VendorView", Tag: `parameter:"view,kind=output,in=view"`},
			Field{Name: "Status", Type: "string", Tag: `parameter:"status,kind=output,in=status"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputBytes, err := os.ReadFile(filepath.Join(dir, "vendor_output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	assertly.AssertValues(t, "package vendor_catalog\n\n// VendorOutput is the generated output scaffold for VendorCatalog.\ntype VendorOutput struct {\n\tData   []*VendorView `parameter:\"view,kind=output,in=view\"`\n\tStatus string        `parameter:\"status,kind=output,in=status\"`\n}\n", string(outputBytes))
}

func TestEmitScaffold_WritesDefaultOutputFields(t *testing.T) {
	plan := &Plan{
		ComponentName: "VendorCatalog",
		ViewDest:      "vendor.go",
		RouterDest:    "vendor_router.go",
		Input:         generatedContract("VendorInput", "vendor_input.go"),
		Output: generatedContract("VendorOutput", "vendor_output.go",
			Field{Name: "Status", Type: "string", Tag: `parameter:"status,kind=output,in=status"`},
			Field{Name: "Data", Type: "[]*View", Tag: `parameter:"view,kind=output,in=view"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputBytes, err := os.ReadFile(filepath.Join(dir, "vendor_output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	assertly.AssertValues(t, "package vendor_catalog\n\n// VendorOutput is the generated output scaffold for VendorCatalog.\ntype VendorOutput struct {\n\tStatus string  `parameter:\"status,kind=output,in=status\"`\n\tData   []*View `parameter:\"view,kind=output,in=view\"`\n}\n", string(outputBytes))
}

func TestResolvePlan_OutputOptionPromotesBodyFieldToOutput(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#define($_ = $Foos<[]*Foo>(body/).Output().Tag('anonymous:"true"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/body", "BodyOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if len(plan.Input.Fields) != 0 {
		t.Fatalf("expected no input fields, got %d", len(plan.Input.Fields))
	}
	if len(plan.Output.Fields) != 1 {
		t.Fatalf("expected one output field, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Foos", plan.Output.Fields[0].Name)
	assertly.AssertValues(t, "[]*Foo", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Foos,kind=output,in=body,dataType=[]*Foo" anonymous:"true"`, plan.Output.Fields[0].Tag)
}

func TestResolvePlan_OutputOptionUsesTypeNameTagWhenTypeExprIsEmpty(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#define($_ = $Foos<?>(body/).Output().Tag('anonymous:"true" typeName:"Foo"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/body", "BodyOutTagType", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if len(plan.Output.Fields) != 1 {
		t.Fatalf("expected one output field, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Foo", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Foos,kind=output,in=body" anonymous:"true" typeName:"Foo"`, plan.Output.Fields[0].Tag)
}

func TestResolvePlan_NestedPatchBodyOutputPreservesTypeNameAndBodyTag(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/foos', 'PATCH'))
#set($_ = $Foos<Foos>(body/).Required())
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true" typeName:"Foos"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/foos", "NestedPatchBodyOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if len(plan.Input.Fields) != 1 {
		t.Fatalf("expected one input field, got %d", len(plan.Input.Fields))
	}
	if len(plan.Output.Fields) != 1 {
		t.Fatalf("expected one output field, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Foos", plan.Input.Fields[0].Name)
	assertly.AssertValues(t, "Foos", plan.Input.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Foos,kind=body,in=,dataType=Foos,required=true"`, plan.Input.Fields[0].Tag)
	assertly.AssertValues(t, "Foos", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Foos,kind=output,in=body" anonymous:"true" typeName:"Foos"`, plan.Output.Fields[0].Tag)
}

func TestResolvePlan_OutputViewWithoutTypeUsesCardinalityConvention(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/view', 'GET'))
#define($_ = $Data<?>(output/view).Cardinality('One').Embed())
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/view", "ViewOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if len(plan.Output.Fields) != 1 {
		t.Fatalf("expected one output field, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Data", plan.Output.Fields[0].Name)
	assertly.AssertValues(t, "*ViewOutView", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Data,kind=output,in=view,cardinality=One" anonymous:"true" view:"ViewOut" sql:"SELECT 1"`, plan.Output.Fields[0].Tag)
}

func TestEmitScaffold_EmbedsAnonymousStatusOutputField(t *testing.T) {
	source := `#import('response','github.com/viant/xdatly/response')
#setting($_ = $route('/v1/api/example/status', 'GET'))
#define($_ = $Status<response.Status>(output/status).Tag('anonymous:"true"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/status", "StatusOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	dir := t.TempDir()
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputFile := filepath.Join(dir, plan.Output.Destination)
	file, err := parser.ParseFile(token.NewFileSet(), outputFile, nil, 0)
	if err != nil {
		t.Fatalf("parse generated output: %v", err)
	}
	field := generatedEmbeddedField(t, file, plan.Output.Type)
	tag, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		t.Fatalf("unquote status tag: %v", err)
	}
	if tag != `parameter:",kind=output,in=status"` {
		t.Fatalf("unexpected status tag: %#v", field.Tag)
	}
}

func TestEmitScaffold_EmbedsInferredAnonymousStatusOutputField(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/status', 'GET'))
#set($_ = $Status<?>(output/status).WithTag('anonymous:"true"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/status", "InferredStatusOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	dir := t.TempDir()
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputFile := filepath.Join(dir, plan.Output.Destination)
	file, err := parser.ParseFile(token.NewFileSet(), outputFile, nil, 0)
	if err != nil {
		t.Fatalf("parse generated output: %v", err)
	}
	field := generatedEmbeddedField(t, file, plan.Output.Type)
	tag, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		t.Fatalf("unquote status tag: %v", err)
	}
	if tag != `parameter:",kind=output,in=status"` {
		t.Fatalf("unexpected inferred status tag: %#v", field.Tag)
	}
}

func TestEmitScaffold_InfersMetricsOutputField(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/metrics', 'GET'))
#set($_ = $Metrics<?>(output/metrics).WithTag('json:"metrics"'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/metrics", "MetricsOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	dir := t.TempDir()
	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputFile := filepath.Join(dir, plan.Output.Destination)
	file, err := parser.ParseFile(token.NewFileSet(), outputFile, nil, 0)
	if err != nil {
		t.Fatalf("parse generated output: %v", err)
	}
	field := generatedNamedField(t, file, plan.Output.Type, "Metrics")
	selector, ok := field.Type.(*ast.SelectorExpr)
	if !ok {
		t.Fatalf("expected selector type, got %#v", field.Type)
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "response" || selector.Sel.Name != "Metrics" {
		t.Fatalf("unexpected metrics type: %#v", field.Type)
	}
	tag, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		t.Fatalf("unquote metrics tag: %v", err)
	}
	structTag := reflect.StructTag(tag)
	if structTag.Get("parameter") != `,kind=output,in=metrics` || structTag.Get("json") != "metrics" {
		t.Fatalf("unexpected metrics tag: %#v", field.Tag)
	}
}

func generatedEmbeddedField(t *testing.T, file *ast.File, typeName string) *ast.Field {
	t.Helper()
	var field *ast.Field
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != typeName {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("output type = %#v", typeSpec.Type)
			}
			for _, candidate := range structType.Fields.List {
				if len(candidate.Names) == 0 {
					field = candidate
					break
				}
			}
		}
	}
	if field == nil {
		t.Fatal("generated embedded field was not found")
	}
	if len(field.Names) != 0 {
		t.Fatalf("expected embedded field, got names=%v", field.Names)
	}
	if field.Tag == nil {
		t.Fatal("expected embedded field tag")
	}
	return field
}

func generatedNamedField(t *testing.T, file *ast.File, typeName, fieldName string) *ast.Field {
	t.Helper()
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != typeName {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("output type = %#v", typeSpec.Type)
			}
			for _, candidate := range structType.Fields.List {
				for _, name := range candidate.Names {
					if name.Name == fieldName {
						if candidate.Tag == nil {
							t.Fatalf("expected %s field tag", fieldName)
						}
						return candidate
					}
				}
			}
		}
	}
	t.Fatalf("generated field %s was not found", fieldName)
	return nil
}

func TestResolvePlan_RootViewCardinalityControlsImplicitOutput(t *testing.T) {
	component := &spec.Component{
		Name:     "SingleView",
		RootView: &spec.View{Name: "View", Cardinality: spec.CardinalityOne},
	}
	plan, err := (&planResolver{input: Input{Component: component}}).resolveBase()
	if err != nil {
		t.Fatalf("planResolver.resolveBase() error = %v", err)
	}
	if len(plan.Output.Fields) != 2 {
		t.Fatalf("output fields = %+v", plan.Output.Fields)
	}
	if actual := plan.Output.Fields[1]; actual.Type != "*SingleView" || actual.Tag != `parameter:"view,kind=output,in=view,cardinality=One" view:"View"` {
		t.Fatalf("view output = %+v", actual)
	}
}

func TestResolvePlan_DerivedOutputNormalizesOriginalDeclarationTag(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/meta', 'GET'))
#define($_ = $Summary<?>(output/summary) /* SELECT 1 AS summary_value */)
#define($_ = $Data<?>(output/view).Cardinality('One'))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/meta", "MetaOut", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	component.RootView.Relations = append(component.RootView.Relations, &spec.Relation{
		Name: "Summary", Kind: spec.RelationKindDerived, Holder: "Summary", Cardinality: spec.CardinalityOne,
		View: &spec.View{Name: "Summary", Source: &spec.ViewSource{SQL: "SELECT 1 AS summary_value"}},
	})
	plan := testPlan(t, component)
	if len(plan.Output.Fields) != 2 {
		t.Fatalf("expected two output fields, got %d", len(plan.Output.Fields))
	}
	assertly.AssertValues(t, "Summary", plan.Output.Fields[0].Name)
	assertly.AssertValues(t, "*SummaryView", plan.Output.Fields[0].Type)
	assertly.AssertValues(t, `parameter:"Summary,kind=output,in=derived" view:"Summary" sql:"SELECT 1 AS summary_value"`, plan.Output.Fields[0].Tag)
	assertly.AssertValues(t, "Data", plan.Output.Fields[1].Name)
	assertly.AssertValues(t, "*MetaOutView", plan.Output.Fields[1].Type)
	assertly.AssertValues(t, `parameter:"Data,kind=output,in=view,cardinality=One" view:"MetaOut" sql:"SELECT 1"`, plan.Output.Fields[1].Tag)
}

func TestEmitScaffold_WritesOutputSummaryField(t *testing.T) {
	plan := &Plan{
		ComponentName: "MetaOut",
		ViewDest:      "meta.go",
		RouterDest:    "meta_router.go",
		Input:         generatedContract("MetaOutInput", "meta_input.go"),
		Output: generatedContract("MetaOutOutput", "meta_output.go",
			Field{Name: "Summary", Type: "any", Tag: `parameter:"summary,kind=output,in=summary"`},
			Field{Name: "Data", Type: "*View", Tag: `parameter:"view,kind=output,in=view"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputBytes, err := os.ReadFile(filepath.Join(dir, "meta_output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	assertly.AssertValues(t, "package meta_out\n\n// MetaOutOutput is the generated output scaffold for MetaOut.\ntype MetaOutOutput struct {\n\tSummary any   `parameter:\"summary,kind=output,in=summary\"`\n\tData    *View `parameter:\"view,kind=output,in=view\"`\n}\n", string(outputBytes))
}

func TestEmitScaffold_WritesCombinedOutputChannels(t *testing.T) {
	plan := &Plan{
		ComponentName: "MetaStatusOut",
		ViewDest:      "meta_status.go",
		RouterDest:    "meta_status_router.go",
		Input:         generatedContract("MetaStatusOutInput", "meta_status_input.go"),
		Output: generatedContract("MetaStatusOutOutput", "meta_status_output.go",
			Field{Name: "Summary", Type: "any", Tag: `parameter:"summary,kind=output,in=summary"`},
			Field{Name: "Status", Type: "string", Tag: `parameter:"status,kind=output,in=status"`},
			Field{Name: "Data", Type: "*View", Tag: `parameter:"view,kind=output,in=view"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputBytes, err := os.ReadFile(filepath.Join(dir, "meta_status_output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	assertly.AssertValues(t, "package meta_status_out\n\n// MetaStatusOutOutput is the generated output scaffold for MetaStatusOut.\ntype MetaStatusOutOutput struct {\n\tSummary any    `parameter:\"summary,kind=output,in=summary\"`\n\tStatus  string `parameter:\"status,kind=output,in=status\"`\n\tData    *View  `parameter:\"view,kind=output,in=view\"`\n}\n", string(outputBytes))
}

func TestEmitScaffold_WritesNestedPatchBodyOutputField(t *testing.T) {
	plan := &Plan{
		ComponentName: "NestedPatchBodyOut",
		ViewDest:      "nested_patch_body_out.go",
		RouterDest:    "nested_patch_body_out_router.go",
		Input: generatedContract("NestedPatchBodyOutInput", "nested_patch_input.go",
			Field{Name: "Foos", Type: "Foos", Tag: `parameter:"Foos,kind=body,in="`},
		),
		Output: generatedContract("NestedPatchBodyOutOutput", "nested_patch_body_out_output.go",
			Field{Name: "Foos", Type: "Foos", Tag: `parameter:"Foos,kind=output,in=body" anonymous:"true" typeName:"Foos"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	outputBytes, err := os.ReadFile(filepath.Join(dir, "nested_patch_body_out_output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	assertly.AssertValues(t, "package nested_patch_body_out\n\n// NestedPatchBodyOutOutput is the generated output scaffold for NestedPatchBodyOut.\ntype NestedPatchBodyOutOutput struct {\n\tFoos Foos `parameter:\"Foos,kind=output,in=body\" anonymous:\"true\" typeName:\"Foos\"`\n}\n", string(outputBytes))
}

func TestEmitScaffold_WritesReferencedBodyPlaceholderTypes(t *testing.T) {
	plan := &Plan{
		ComponentName: "NestedPatchTypes",
		ViewDest:      "nested_patch_types.go",
		RouterDest:    "nested_patch_types_router.go",
		Input: generatedContract("NestedPatchTypesInput", "nested_patch_types_input.go",
			Field{Name: "Foos", Type: "Foos", Tag: `parameter:"Foos,kind=body,in="`},
		),
		Output: generatedContract("NestedPatchTypesOutput", "nested_patch_types_output.go",
			Field{Name: "Foos", Type: "Foos", Tag: `parameter:"Foos,kind=output,in=body" anonymous:"true" typeName:"Foos"`},
			Field{Name: "FoosPerformance", Type: "[]*FoosPerformance", Tag: `parameter:"FoosPerformance,kind=output,in=body"`},
		),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	viewBytes, err := os.ReadFile(filepath.Join(dir, "nested_patch_types.go"))
	if err != nil {
		t.Fatalf("failed to read generated view file: %v", err)
	}
	content := string(viewBytes)
	if !strings.Contains(content, "type Foos struct{}") {
		t.Fatalf("expected Foos placeholder type, got:\n%s", content)
	}
	if !strings.Contains(content, "type FoosPerformance struct{}") {
		t.Fatalf("expected FoosPerformance placeholder type, got:\n%s", content)
	}
}

func TestEmitScaffold_WritesReferencedTagPlaceholderTypes(t *testing.T) {
	plan := &Plan{
		ComponentName: "NestedPatchMarkers",
		ViewDest:      "nested_patch_markers.go",
		RouterDest:    "nested_patch_markers_router.go",
		Input: generatedContract("NestedPatchMarkersInput", "nested_patch_markers_input.go",
			Field{Name: "Foos", Type: "Foos", Tag: `parameter:"Foos,kind=body,in=" setMarker:"true" typeName:"FoosHas"`},
		),
		Output: generatedContract("NestedPatchMarkersOutput", "nested_patch_markers_output.go"),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	viewBytes, err := os.ReadFile(filepath.Join(dir, "nested_patch_markers.go"))
	if err != nil {
		t.Fatalf("failed to read generated view file: %v", err)
	}
	content := string(viewBytes)
	if !strings.Contains(content, "type Foos struct{}") {
		t.Fatalf("expected Foos placeholder type, got:\n%s", content)
	}
	if !strings.Contains(content, "type FoosHas struct{}") {
		t.Fatalf("expected FoosHas placeholder type from tag reference, got:\n%s", content)
	}
}

func TestEmitScaffold_WritesFlatHasMarkerStruct(t *testing.T) {
	plan := &Plan{
		ComponentName: "FlatPatchInput",
		ViewDest:      "flat_patch_input.go",
		RouterDest:    "flat_patch_input_router.go",
		Input: generatedContract("FlatPatchInput", "flat_patch_input_input.go",
			Field{Name: "ID", Type: "int", Tag: `parameter:"id,kind=body,in=id"`},
			Field{Name: "Name", Type: "*string", Tag: `parameter:"name,kind=body,in=name"`},
		),
		Output: generatedContract("FlatPatchOutput", "flat_patch_input_output.go"),
	}
	dir := t.TempDir()

	if _, err := EmitScaffold(dir, plan); err != nil {
		t.Fatalf("unexpected emit error: %v", err)
	}
	inputBytes, err := os.ReadFile(filepath.Join(dir, "flat_patch_input_input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !containsNormalized(content, "Has *FlatPatchInputHas") {
		t.Fatalf("expected Has marker field, got:\n%s", content)
	}
	if !containsNormalized(content, "type FlatPatchInputHas struct {\n\tID bool\n\tName bool\n}") {
		t.Fatalf("expected flat Has struct, got:\n%s", content)
	}
}

func TestGeneratePackage_ProducesBuildablePackage(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors', 'GET'))
#setting($_ = $input_type('VendorInput'))
#setting($_ = $output_type('VendorOutput'))
#define($_ = $VendorID<int>(path/vendorID))
#define($_ = $Data<[]*VendorView>(output/view))
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/vendors", "VendorCatalog", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	pkgDir := filepath.Join(root, "vendor_catalog")
	generator, err := newTestGenerator(component, nil)
	if err != nil {
		t.Fatalf("unexpected generator error: %v", err)
	}
	result, err := generator.Generate(pkgDir)
	if err != nil {
		t.Fatalf("unexpected generate error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	if result.Plan.Input.Type != "VendorInput" || result.Plan.Output.Type != "VendorOutput" {
		t.Fatalf("unexpected generated type names: %#v", result.Plan)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated package did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageWithLinkedContractsProducesBuildablePackage(t *testing.T) {
	type linkedInput struct{}
	type linkedOutput struct{}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	contractsDir := filepath.Join(root, "contracts")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contractsDir, "contracts.go"), []byte("package contracts\n\ntype Input struct{}\ntype Output struct{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	inputDescriptor := x.NewType(reflect.TypeOf(linkedInput{}), x.WithName("Input"), x.WithPkgPath("example.com/generated/contracts"))
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/generated/contracts"))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, inputDescriptor, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	packageDir := filepath.Join(root, "users")
	result, err := New(Input{
		Component: &spec.Component{Name: "Users"}, TypeResolver: resolver, TargetPackage: "example.com/generated/users",
		Contracts: ContractReferences{
			Input:  &ContractReference{Expression: "Input", DescriptorKey: inputDescriptor.Key()},
			Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()},
		},
	}).Generate(packageDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Files) != 1 {
		t.Fatalf("linked generation files = %+v", result.Files)
	}
	for _, name := range []string{"input.go", "output.go"} {
		if _, err = os.Stat(filepath.Join(packageDir, name)); !os.IsNotExist(err) {
			t.Fatalf("linked contract file %s was emitted: %v", name, err)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("linked generated package did not compile: %v\n%s", err, output)
	}
}

func TestGeneratePackageFromSource_ProducesBuildablePackage(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors', 'GET'))
#define($_ = $VendorID<int>(path/vendorID))
#define($_ = $Data<[]*VendorView>(output/view))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "vendor_catalog")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/vendors", "VendorCatalog", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	assertly.AssertValues(t, "VendorCatalog", result.Plan.ComponentName)

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated package from source did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageFromSource_PreservesCacheWarmupExcludeDefault(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/records', 'GET'))
#setting($_ = $cache('records'))
#setting($_ = $cache_warmup('order_id','IndexParameter=OrderID','ExcludeDefault=Period','Period=today,week,last_complete_7d'))
#define($_ = $OrderID<int>(query/order_id))
#define($_ = $Data<[]*RecordView>(output/view))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "records")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/records", "Records", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	componentSource, err := os.ReadFile(filepath.Join(pkgDir, result.Plan.RouterDest))
	if err != nil {
		t.Fatal(err)
	}
	tag, err := generatedComponentContractTag(componentSource)
	if err != nil {
		t.Fatal(err)
	}
	warmup := tag.Settings.Cache.Warmup
	if warmup == nil || len(warmup.Cases) != 1 || len(warmup.Cases[0].Set) != 1 {
		t.Fatalf("warmup=%+v", warmup)
	}
	param := warmup.Cases[0].Set[0]
	if param.Name != "Period" || !param.ExcludeDefault {
		t.Fatalf("param=%+v", param)
	}
}

func TestGeneratePackageFromSource_ImportsQualifiedTimeInputTypes(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/keywords', 'GET'))
#set($_ = $KeywordDate<time.Time>(form/keyword_date).Tag('format:"dateFormat=YYYY-MM-DD"').Optional())
#set($_ = $KeywordFrom<*time.Time>(form/keyword_from).Tag('format:"dateFormat=YYYY-MM-DD"').Optional())
#set($_ = $KeywordWindows<[]time.Time>(form/keyword_windows).Optional())
#define($_ = $Data<[]*KeywordView>(output/view))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "keywords")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/keywords", "Keywords", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputSource, err := os.ReadFile(filepath.Join(pkgDir, result.Plan.Input.Destination))
	if err != nil {
		t.Fatal(err)
	}
	settersSource, err := os.ReadFile(filepath.Join(pkgDir, result.Plan.Generation.File("input_setters", "input_setters.go")))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []struct {
		name   string
		source string
	}{
		{"input.go", string(inputSource)},
		{"input_setters.go", string(settersSource)},
	} {
		if !strings.Contains(file.source, `time "time"`) {
			t.Fatalf("%s missing time import:\n%s", file.name, file.source)
		}
	}
	for _, expected := range []string{"KeywordDate", "time.Time", "KeywordFrom", "*time.Time", "KeywordWindows", "[]time.Time", `format:"dateFormat=YYYY-MM-DD"`} {
		if !strings.Contains(string(inputSource), expected) {
			t.Fatalf("input.go missing %q:\n%s", expected, inputSource)
		}
	}

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated package with time inputs did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageFromSource_LoadsWithViantXAstLoader(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors', 'GET'))
#define($_ = $VendorID<int>(path/vendorID))
#define($_ = $Data<[]*VendorView>(output/view))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "vendor_catalog")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/vendors", "VendorCatalog", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}

	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "vendor_catalog")
	if err != nil {
		t.Fatalf("LoadPackageFS failed: %v", err)
	}
	assertly.AssertValues(t, "vendor_catalog", pkg.Name)
	assertly.AssertValues(t, "example.com/generated/vendor_catalog", pkg.PkgPath)
	if !pkg.HasType(result.Plan.Input.Type) || !pkg.HasType(result.Plan.Output.Type) || !pkg.HasType("VendorView") || !pkg.HasType("Component") {
		var names []string
		for _, item := range pkg.Types {
			if item != nil {
				names = append(names, item.Name)
			}
		}
		t.Fatalf("expected generated package types to load through viant/x, got %v", names)
	}
}

func TestGeneratePackageFromSource_NestedPatchProducesBuildablePackage(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/foos', 'PATCH'))
#set($_ = $Foos<Foos>(body/).Required())
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true" typeName:"Foos"'))
#set($_ = $FoosPerformance<[]*FoosPerformance>(body/foosPerformance).Output())
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "nested_patch_body_out")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/foos", "NestedPatchBodyOut", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated nested patch package did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageFromSourceWithTypeResolver_UsesTypeContextImports(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out")
	result, err := GeneratePackageFromSourceWithTypeResolver(pkgDir, "example.com/demo/body", "BodyOut", source, func(name string) (reflect.Type, error) {
		if name != "models.Foo" {
			t.Fatalf("unexpected lookup type name %q", name)
		}
		type Foo struct{}
		return reflect.TypeOf(Foo{}), nil
	})
	if err != nil {
		t.Fatalf("unexpected typed generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !strings.Contains(content, `import (`+"\n\tmodels \"example.com/generated/models\"\n)\n") {
		t.Fatalf("expected models import, got:\n%s", content)
	}
	if !strings.Contains(content, "Foos models.Foo") {
		t.Fatalf("expected concrete imported type, got:\n%s", content)
	}

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("typed generated package did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageFromSourceWithModuleLookup_UsesTypeContextImports(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_module")
	result, err := GeneratePackageFromSourceWithModuleLookup(pkgDir, root, "example.com/demo/body", "BodyOutModule", source)
	if err != nil {
		t.Fatalf("unexpected module-lookup generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !strings.Contains(content, `models "example.com/generated/models"`) {
		t.Fatalf("expected models import, got:\n%s", content)
	}
	if !strings.Contains(content, "Foos models.Foo") {
		t.Fatalf("expected concrete imported type, got:\n%s", content)
	}

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("module-lookup generated package did not compile:\n%s\n%v", string(output), err)
	}
}

func TestGeneratePackageFromSource_QualifiedTypeWithoutAuthorityFailsClosed(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "body_out_default_placeholder")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutDefaultPlaceholder", source)
	if err == nil || !strings.Contains(err.Error(), "package type") {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
	if _, statErr := os.Stat(pkgDir); !os.IsNotExist(statErr) {
		t.Fatalf("failed planning wrote package directory: %v", statErr)
	}
}

func TestGeneratePackageFromSource_DefaultPathUsesTypeContextModuleLookup(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_default_module")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutDefaultModule", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !strings.Contains(content, `models "example.com/generated/models"`) {
		t.Fatalf("expected module import, got:\n%s", content)
	}
	if !strings.Contains(content, "Foos models.Foo") {
		t.Fatalf("expected concrete imported type, got:\n%s", content)
	}
}

func TestGeneratePackageFromSource_EmptyModulePathCannotResolveQualifiedType(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	gomod := generatedGoModWithoutModule()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatalf("failed to write go.mod: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_empty_module")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutEmptyModule", source)
	if err == nil || !strings.Contains(err.Error(), "package type was not found") {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestGeneratePackageFromSource_PackageLoadFailureCannotResolveQualifiedType(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct {\n"), 0o644); err != nil {
		t.Fatalf("failed to write invalid model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_invalid_models")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutInvalidModels", source)
	if err == nil || !strings.Contains(err.Error(), "package type was not found") {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestGeneratePackageFromSource_UnresolvedQualifiedTypeFailsClosed(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $Foos<?>(body/).Tag('anonymous:"true" typeName:"models.Foo"'))
#set($_ = $Bars<?>(body/bar).Tag('typeName:"models.Bar"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_default_mixed")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutDefaultMixed", source)
	if err == nil || !strings.Contains(err.Error(), `package type was not found`) || !strings.Contains(err.Error(), `models.Bar`) {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestGeneratePackageFromSource_DefaultPathResolvesWrappedQualifiedTypes(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $A<?>(body/a).Tag('typeName:"models.Foo"'))
#set($_ = $B<?>(body/b).Tag('typeName:"*models.Foo"'))
#set($_ = $C<?>(body/c).Tag('typeName:"[]*models.Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_wrapped")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutWrapped", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !containsNormalized(content, "A models.Foo") {
		t.Fatalf("expected resolved bare qualified type, got:\n%s", content)
	}
	if !containsNormalized(content, "B *models.Foo") {
		t.Fatalf("expected resolved pointer qualified type, got:\n%s", content)
	}
	if !containsNormalized(content, "C []*models.Foo") {
		t.Fatalf("expected resolved slice pointer qualified type, got:\n%s", content)
	}
}

func TestGeneratePackageFromSource_WrappedMissingQualifiedTypeFailsClosed(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/body')
#import('models','example.com/generated/models')
#set($_ = $A<?>(body/a).Tag('typeName:"*models.Missing"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_wrapped_missing")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutWrappedMissing", source)
	if err == nil || !strings.Contains(err.Error(), `package type was not found`) || !strings.Contains(err.Error(), `*models.Missing`) {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestGeneratePackageFromSource_DefaultPathUsesDefaultPackageForUnqualifiedNames(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/body', 'POST'))
#package('example.com/generated/models')
#set($_ = $A<?>(body/a).Tag('typeName:"Foo"'))
#set($_ = $B<?>(body/b).Tag('typeName:"*Foo"'))
#set($_ = $C<?>(body/c).Tag('typeName:"[]*Foo"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foo struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "body_out_default_package")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/body", "BodyOutDefaultPackage", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	content := string(inputBytes)
	if !strings.Contains(content, `models "example.com/generated/models"`) {
		t.Fatalf("expected models import from default package, got:\n%s", content)
	}
	if !containsNormalized(content, "A models.Foo") {
		t.Fatalf("expected default-package bare type resolution, got:\n%s", content)
	}
	if !containsNormalized(content, "B *models.Foo") {
		t.Fatalf("expected default-package pointer type resolution, got:\n%s", content)
	}
	if !containsNormalized(content, "C []*models.Foo") {
		t.Fatalf("expected default-package slice pointer type resolution, got:\n%s", content)
	}
	viewBytes, err := os.ReadFile(filepath.Join(pkgDir, "views.go"))
	if err != nil {
		t.Fatalf("failed to read generated view file: %v", err)
	}
	if strings.Contains(string(viewBytes), "type Foo struct{}") {
		t.Fatalf("resolved default-package type leaked a local placeholder:\n%s", viewBytes)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	if output, runErr := cmd.CombinedOutput(); runErr != nil {
		t.Fatalf("default-package generated module did not compile: %v\n%s", runErr, output)
	}
}

func TestGeneratePackageFromSource_DefaultPathKeepsDepthOneChildTypeConcrete(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/foos', 'PATCH'))
#package('example.com/generated/models')
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true" typeName:"Foos"'))
#set($_ = $FoosPerformance<?>(body/foosPerformance).Output().Tag('typeName:"[]*FoosPerformance"'))
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	if err := os.MkdirAll(filepath.Join(root, "models"), 0o755); err != nil {
		t.Fatalf("failed to create models dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "models", "types.go"), []byte("package models\n\ntype Foos struct{}\ntype FoosPerformance struct{}\n"), 0o644); err != nil {
		t.Fatalf("failed to write model types: %v", err)
	}

	pkgDir := filepath.Join(root, "nested_patch_child_type")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/foos", "NestedPatchChildType", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	outputBytes, err := os.ReadFile(filepath.Join(pkgDir, "output.go"))
	if err != nil {
		t.Fatalf("failed to read generated output file: %v", err)
	}
	content := string(outputBytes)
	if !containsNormalized(content, "Foos models.Foos") {
		t.Fatalf("expected concrete parent type, got:\n%s", content)
	}
	if !containsNormalized(content, "FoosPerformance []*models.FoosPerformance") {
		t.Fatalf("expected concrete depth-1 child type, got:\n%s", content)
	}
}

func TestResolvePlan_PrefersDefineOverShadowedSetDeclarationForShaping(t *testing.T) {
	// Both a declaration-shaped #set and a #define target the same logical params
	// (User input, Result output). Field shaping must prefer the #define-backed
	// param and must not emit it twice, independent of parser precedence.
	component := &spec.Component{
		Name: "PreferDefineShaping",
		Parameters: []*spec.Parameter{
			{
				Name:        "User",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "LegacyUser",
			},
			{
				Name:        "User",
				Declaration: spec.DeclarationKindDefine,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "PreferredUser",
			},
			{
				Name:        "Audit",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "query", Name: "audit"},
				TypeExpr:    "string",
			},
			{
				Name:        "Result",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "LegacyResult",
				EmitOutput:  true,
			},
			{
				Name:        "Result",
				Declaration: spec.DeclarationKindDefine,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "PreferredResult",
				EmitOutput:  true,
			},
		},
	}

	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}

	var userFields []Field
	for _, field := range plan.Input.Fields {
		if field.Name == "User" {
			userFields = append(userFields, field)
		}
	}
	if len(userFields) != 1 {
		t.Fatalf("expected single define-backed User input field, got %d: %+v", len(userFields), plan.Input.Fields)
	}
	assertly.AssertValues(t, "PreferredUser", userFields[0].Type)

	var auditFound bool
	for _, field := range plan.Input.Fields {
		if field.Name == "Audit" {
			auditFound = true
			assertly.AssertValues(t, "string", field.Type)
		}
	}
	if !auditFound {
		t.Fatalf("expected unmatched set param Audit to remain, got %+v", plan.Input.Fields)
	}

	var resultFields []Field
	for _, field := range plan.Output.Fields {
		if field.Name == "Result" {
			resultFields = append(resultFields, field)
		}
	}
	if len(resultFields) != 1 {
		t.Fatalf("expected single define-backed Result output field, got %d: %+v", len(resultFields), plan.Output.Fields)
	}
	assertly.AssertValues(t, "PreferredResult", resultFields[0].Type)
}

func containsNormalized(actual, expected string) bool {
	return strings.Contains(strings.Join(strings.Fields(actual), " "), strings.Join(strings.Fields(expected), " "))
}

func TestResolvePlan_KeepsSetOnlyInputShapingUnchanged(t *testing.T) {
	component := &spec.Component{
		Name: "SetOnlyShaping",
		Parameters: []*spec.Parameter{
			{
				Name:        "User",
				Declaration: spec.DeclarationKindSet,
				Source:      spec.BindSource{Kind: "body"},
				TypeExpr:    "LegacyUser",
			},
		},
	}
	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var userFields []Field
	for _, field := range plan.Input.Fields {
		if field.Name == "User" {
			userFields = append(userFields, field)
		}
	}
	if len(userFields) != 1 {
		t.Fatalf("expected set-only User field preserved, got %d: %+v", len(userFields), plan.Input.Fields)
	}
	assertly.AssertValues(t, "LegacyUser", userFields[0].Type)
}

func TestResolvePlan_InfersRowHelperTypeFromDeclarationSQL(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events-row-helper', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $EventTypes<?>(param/Events) /*
SELECT Price, Timestamp FROM /EventsPerformance
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/events", "EventsRowHelper", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testSyntaxPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var helperField *Field
	for i := range plan.Input.Fields {
		if plan.Input.Fields[i].Name == "EventTypes" {
			helperField = &plan.Input.Fields[i]
			break
		}
	}
	if helperField == nil {
		t.Fatalf("expected EventTypes helper field, got %+v", plan.Input.Fields)
	}
	assertly.AssertValues(t, "[]EventTypesRow", helperField.Type)
	if len(plan.HelperTypes) != 1 {
		t.Fatalf("expected one helper type, got %+v", plan.HelperTypes)
	}
	assertly.AssertValues(t, "EventTypesRow", plan.HelperTypes[0].Name)
	assertly.AssertValues(t, []Field{
		{Name: "Price"},
		{Name: "Timestamp"},
	}, plan.HelperTypes[0].Fields)
}

func TestGeneratePackageFromSourceRejectsUntypedRowHelper(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events-row-helper', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $EventTypes<?>(param/Events) /*
SELECT Price, Timestamp FROM /EventsPerformance
*/)
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "events_row_helper")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/events", "EventsRowHelper", source)
	if err == nil || !strings.Contains(err.Error(), "generated helper EventTypesRow field Price has no concrete Go type") {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestResolvePlanWithTypeResolver_InfersRowHelperFieldTypesFromLinkedBody(t *testing.T) {
	// A row helper over a linked source derives concrete projected field types
	// from the child element struct.
	source := `#setting($_ = $route('/v1/api/example/events-row-helper-typed', 'POST'))
#import('models','example.com/demo/events/models')
#define($_ = $Events<*models.Event>(body/).Required())
#define($_ = $EventTypes<?>(param/Events) /*
SELECT Price, Timestamp FROM /EventsPerformance
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/events", "EventsRowHelperTyped", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan, err := testPlanWithTypeResolver(t, component, func(name string) (reflect.Type, error) {
		if name != "models.Event" {
			return nil, nil
		}
		type performance struct {
			Price     float64
			Timestamp string
			Notes     []string
		}
		type event struct {
			Id                int
			EventsPerformance []performance
		}
		return reflect.TypeOf(event{}), nil
	})
	if err != nil {
		t.Fatalf("unexpected typed plan error: %v", err)
	}
	if len(plan.HelperTypes) != 1 {
		t.Fatalf("expected one helper type, got %+v", plan.HelperTypes)
	}
	assertly.AssertValues(t, "EventTypesRow", plan.HelperTypes[0].Name)
	assertly.AssertValues(t, []Field{
		{Name: "Price", Type: "float64"},
		{Name: "Timestamp", Type: "string"},
	}, plan.HelperTypes[0].Fields)
}

func TestResolvePlanWithTypeResolverRejectsUntypedRowHelper(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events-row-helper-untyped', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $EventTypes<?>(param/Events) /*
SELECT Price, Timestamp FROM /EventsPerformance
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/events", "EventsRowHelperUntyped", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	_, err = testPlanWithTypeResolver(t, component, func(name string) (reflect.Type, error) {
		return nil, nil // no linked type information available
	})
	if err == nil || !strings.Contains(err.Error(), "generated helper EventTypesRow field Price has no concrete Go type") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestResolvePlan_InfersArrayAggHelperType(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events-agg', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $CurEventsId<?>(param/Events) /*
SELECT ARRAY_AGG(Id) AS Values FROM EVENTS LIMIT 1
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/events", "EventsAgg", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testSyntaxPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var helperField *Field
	for i := range plan.Input.Fields {
		if plan.Input.Fields[i].Name == "CurEventsId" {
			helperField = &plan.Input.Fields[i]
			break
		}
	}
	if helperField == nil {
		t.Fatalf("expected CurEventsId helper field, got %+v", plan.Input.Fields)
	}
	assertly.AssertValues(t, "CurEventsIdHelper", helperField.Type)
	assertly.AssertValues(t, `parameter:"CurEventsId,kind=param,in=Events"`, helperField.Tag)
	if len(plan.HelperTypes) != 1 {
		t.Fatalf("expected one helper type, got %+v", plan.HelperTypes)
	}
	assertly.AssertValues(t, "CurEventsIdHelper", plan.HelperTypes[0].Name)
	assertly.AssertValues(t, []Field{
		{Name: "Values"},
	}, plan.HelperTypes[0].Fields)
}

func TestResolvePlanWithTypeResolver_InfersArrayAggValueType(t *testing.T) {
	source := "#setting($_ = $route('/events', 'PATCH'))\n" +
		"#import('models','example.com/events/models')\n" +
		"#define($_ = $Events<[]*models.Event>(body/Data).Cardinality('Many').Required())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /* SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1 */)\n" +
		"SELECT 1"
	component, err := parseTestComponentSource("example.com/events", "Events", source)
	if err != nil {
		t.Fatal(err)
	}
	type event struct{ Id *int64 }
	plan, err := testPlanWithTypeResolver(t, component, func(name string) (reflect.Type, error) {
		if name == "models.Event" || name == "[]*models.Event" {
			return reflect.TypeOf(event{}), nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.HelperTypes) != 1 || len(plan.HelperTypes[0].Fields) != 1 || plan.HelperTypes[0].Fields[0].Type != "[]*int64" {
		t.Fatalf("helper types = %+v", plan.HelperTypes)
	}
}

func TestGeneratePackageFromSourceRejectsUntypedArrayAggHelper(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/events-agg', 'POST'))
#define($_ = $Events<?>(body/).Required())
#define($_ = $CurEventsId<?>(param/Events) /*
SELECT ARRAY_AGG(Id) AS Values FROM EVENTS LIMIT 1
*/)
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	pkgDir := filepath.Join(root, "events_agg")
	_, err := GeneratePackageFromSource(pkgDir, "example.com/demo/events", "EventsAgg", source)
	if err == nil || !strings.Contains(err.Error(), "generated helper CurEventsIdHelper field Values has no concrete Go type") {
		t.Fatalf("GeneratePackageFromSource() error = %v", err)
	}
}

func TestGeneratePackageFromSource_ArrayAggHelperFieldIsUsable(t *testing.T) {
	// The generated helper exposes the exact source field slice type.
	source := "#setting($_ = $route('/v1/api/example/events-agg', 'POST'))\n" +
		"#import('models','example.com/generated/models')\n" +
		"#define($_ = $Events<[]*models.Event>(body/).Required())\n" +
		"#define($_ = $CurEventsId<?>(param/Events) /*\n" +
		"SELECT ARRAY_AGG(Id) AS Values FROM `/` LIMIT 1\n" +
		"*/)\nSELECT 1"

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	modelsDir := filepath.Join(root, "models")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "event.go"), []byte("package models\n\ntype Event struct { Id int }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgDir := filepath.Join(root, "events_agg")
	result, err := GeneratePackageFromSourceWithModuleLookup(pkgDir, root, "example.com/generated/events_agg", "EventsAgg", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	pkgName := strings.TrimSpace(strings.TrimPrefix(strings.SplitN(string(inputBytes), "\n", 2)[0], "package"))
	consumer := "package " + pkgName + "\n\n" +
		"func _useArrayAggHelperValues(in *" + result.Plan.Input.Type + ") []int {\n" +
		"\treturn in.CurEventsId.Values\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(pkgDir, "consumer_test_helper.go"), []byte(consumer), 0o644); err != nil {
		t.Fatalf("failed to write consumer: %v", err)
	}
	cmd := exec.Command("go", "build", "-mod=mod", "./...")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated ARRAY_AGG helper field is not usable (compile failed): %v\n%s", err, out)
	}
}

func TestResolvePlan_UsesDeclarationCommentDataType(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors-auth', 'GET'))
#define($_ = $Jwt<string>(header/Authorization).WithCodec('JwtClaim'))
#define($_ = $Authorization /*
!!403
SELECT Authorized /* {"DataType":"bool"} */
FROM (
    SELECT 1 AS Authorized
) t
WHERE Authorized
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/vendors", "VendorsAuth", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var authField *Field
	for i := range plan.Input.Fields {
		if plan.Input.Fields[i].Name == "Authorization" {
			authField = &plan.Input.Fields[i]
			break
		}
	}
	if authField == nil {
		t.Fatalf("expected Authorization field, got %+v", plan.Input.Fields)
	}
	assertly.AssertValues(t, "bool", authField.Type)
}

func TestGeneratePackageFromSource_EmitsDeclarationCommentDataType(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors-auth', 'GET'))
#define($_ = $Jwt<string>(header/Authorization).WithCodec('JwtClaim'))
#define($_ = $Authorization /*
!!403
SELECT Authorized /* {"DataType":"bool"} */
FROM (
    SELECT 1 AS Authorized
) t
WHERE Authorized
*/)
SELECT 1`

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)

	pkgDir := filepath.Join(root, "vendors_auth")
	result, err := GeneratePackageFromSource(pkgDir, "example.com/demo/vendors", "VendorsAuth", source)
	if err != nil {
		t.Fatalf("unexpected generate-from-source error: %v", err)
	}
	if result == nil || result.Plan == nil {
		t.Fatalf("expected generation result with plan")
	}
	inputBytes, err := os.ReadFile(filepath.Join(pkgDir, "input.go"))
	if err != nil {
		t.Fatalf("failed to read generated input file: %v", err)
	}
	if !strings.Contains(string(inputBytes), "Authorization bool") {
		t.Fatalf("expected Authorization bool field, got:\n%s", string(inputBytes))
	}
}

func TestResolvePlan_DataTypeDoesNotOverrideOutputViewTyping(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/users-view-meta', 'GET'))
#define($_ = $Data<?>(output/view) /*
SELECT Value /* {"DataType":"bool"} */
FROM (SELECT 1 AS Value) t
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/users", "UsersViewMeta", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var dataField *Field
	for i := range plan.Output.Fields {
		if plan.Output.Fields[i].Name == "Data" {
			dataField = &plan.Output.Fields[i]
			break
		}
	}
	if dataField == nil {
		t.Fatalf("expected Data output field, got %+v", plan.Output.Fields)
	}
	assertly.AssertValues(t, "[]*UsersViewMetaView", dataField.Type)
}

func TestResolvePlan_UnsupportedDeclarationCommentDataTypeFallsBackToAny(t *testing.T) {
	source := `#setting($_ = $route('/v1/api/example/vendors-auth-weird', 'GET'))
#define($_ = $Authorization /*
SELECT Authorized /* {"DataType":"integerish"} */
FROM (SELECT 1 AS Authorized) t
*/)
SELECT 1`

	component, err := parseTestComponentSource("example.com/demo/vendors", "VendorsAuthWeird", source)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	plan := testPlan(t, component)
	if plan == nil {
		t.Fatalf("expected plan")
	}
	var authField *Field
	for i := range plan.Input.Fields {
		if plan.Input.Fields[i].Name == "Authorization" {
			authField = &plan.Input.Fields[i]
			break
		}
	}
	if authField == nil {
		t.Fatalf("expected Authorization field, got %+v", plan.Input.Fields)
	}
	assertly.AssertValues(t, "any", authField.Type)
}
