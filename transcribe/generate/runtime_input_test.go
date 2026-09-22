package generate

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/xdatly/response"
)

type runtimeInputCustomer struct {
	ID int
}

func TestGeneratorRuntimeInputTypeUsesCanonicalFieldsAndPresenceMarker(t *testing.T) {
	component := &spec.Component{
		Name: "Search",
		Parameters: []*spec.Parameter{
			{Name: "ID", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}},
			{Name: "Names", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "name"}},
		},
	}
	inputType, err := New(Input{Component: component}).RuntimeInputType()
	if err != nil {
		t.Fatalf("RuntimeInputType() error = %v", err)
	}
	if inputType.Kind() != reflect.Struct || inputType.NumField() != 3 {
		t.Fatalf("input type = %v", inputType)
	}
	if field, _ := inputType.FieldByName("ID"); field.Type != reflect.TypeOf(int(0)) || field.Tag.Get("parameter") == "" {
		t.Fatalf("ID field = %+v", field)
	}
	if field, _ := inputType.FieldByName("Names"); field.Type != reflect.TypeOf([]string{}) {
		t.Fatalf("Names field = %+v", field)
	}
	marker, ok := inputType.FieldByName("Has")
	if !ok || marker.Type.Kind() != reflect.Pointer || marker.Type.Elem().NumField() != 2 || marker.Tag.Get("setMarker") != "true" {
		t.Fatalf("Has field = %+v", marker)
	}
}

func TestGeneratorRuntimeOutputTypeUsesGeneratedViewFields(t *testing.T) {
	component := &spec.Component{
		Name: "Events",
		Parameters: []*spec.Parameter{{
			Name: "Events", TypeExpr: "[]*EventsView", Source: spec.BindSource{Kind: "output", Name: "view"},
		}},
		RootView: &spec.View{Name: "Events", Columns: []*spec.Column{
			{Name: "ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
	}
	outputType, err := New(Input{Component: component}).RuntimeOutputType()
	if err != nil {
		t.Fatalf("RuntimeOutputType() error = %v", err)
	}
	field, ok := outputType.FieldByName("Events")
	if !ok || field.Type.Kind() != reflect.Slice || field.Type.Elem().Kind() != reflect.Ptr {
		t.Fatalf("Events field = %+v", field)
	}
	row := field.Type.Elem().Elem()
	if row.NumField() != 2 || row.Field(0).Name != "Id" || row.Field(1).Name != "Name" {
		t.Fatalf("generated output row = %v", row)
	}
}

func TestGeneratorRuntimeOutputTypeEmbedsAnonymousStatus(t *testing.T) {
	component := &spec.Component{
		Name: "StatusOut",
		Parameters: []*spec.Parameter{{
			Name: "Status", Tag: `anonymous:"true"`,
			Source: spec.BindSource{Kind: "output", Name: "status"},
		}},
	}
	outputType, err := New(Input{Component: component}).RuntimeOutputType()
	if err != nil {
		t.Fatalf("RuntimeOutputType() error = %v", err)
	}
	field, ok := outputType.FieldByName("Status")
	if !ok || !field.Anonymous || field.Type != reflect.TypeFor[response.Status]() {
		t.Fatalf("Status field = %+v", field)
	}
	value := reflect.New(outputType).Elem()
	value.FieldByIndex(field.Index).Set(reflect.ValueOf(response.Status{Status: "ok"}))
	data, err := json.Marshal(value.Interface())
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(data) != `{"status":"ok"}` {
		t.Fatalf("json=%s", data)
	}
}

func TestGeneratorRuntimeOutputTypeInfersMetrics(t *testing.T) {
	component := &spec.Component{
		Name: "MetricsOut",
		Parameters: []*spec.Parameter{{
			Name: "Metrics", Tag: `json:"metrics"`,
			Source: spec.BindSource{Kind: "output", Name: "metrics"},
		}},
	}
	outputType, err := New(Input{Component: component}).RuntimeOutputType()
	if err != nil {
		t.Fatalf("RuntimeOutputType() error = %v", err)
	}
	field, ok := outputType.FieldByName("Metrics")
	if !ok || field.Anonymous || field.Type != reflect.TypeFor[response.Metrics]() {
		t.Fatalf("Metrics field = %+v", field)
	}
	if field.Tag.Get("json") != "metrics" {
		t.Fatalf("Metrics json tag = %q", field.Tag.Get("json"))
	}
}

func TestGeneratorRuntimeInputTypeResolvesLinkedFieldThroughTypeCatalog(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(runtimeInputCustomer{}),
		x.WithName("Customer"),
		x.WithPkgPath("example.com/models"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}},
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	component := &spec.Component{
		Name:        "Search",
		TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}},
		Parameters: []*spec.Parameter{{
			Name: "Customer", TypeExpr: "models.Customer",
			Source: spec.BindSource{Kind: "body", Name: "Customer"},
		}},
	}
	inputType, err := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"}).RuntimeInputType()
	if err != nil {
		t.Fatalf("RuntimeInputType() error = %v", err)
	}
	field, ok := inputType.FieldByName("Customer")
	if !ok || field.Type != reflect.TypeOf(runtimeInputCustomer{}) {
		t.Fatalf("Customer field = %+v", field)
	}
}

func TestGeneratorRuntimeInputTypeUsesCanonicalGeneratedViewFields(t *testing.T) {
	component := &spec.Component{
		Name: "Events",
		Parameters: []*spec.Parameter{{
			Name: "Events", TypeExpr: "[]*EventsView", Source: spec.BindSource{Kind: "body", Name: "Events"},
		}},
		RootView: &spec.View{Name: "Events", Columns: []*spec.Column{
			{Name: "ID", Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
	}
	inputType, err := New(Input{Component: component}).RuntimeInputType()
	if err != nil {
		t.Fatalf("RuntimeInputType() error = %v", err)
	}
	field, ok := inputType.FieldByName("Events")
	if !ok || field.Type.Kind() != reflect.Slice || field.Type.Elem().Kind() != reflect.Ptr {
		t.Fatalf("Events field = %+v", field)
	}
	rowType := field.Type.Elem().Elem()
	if rowType.NumField() != 2 || rowType.Field(0).Name != "Id" || rowType.Field(0).Type.Kind() != reflect.Int64 || rowType.Field(1).Name != "Name" {
		t.Fatalf("generated runtime row = %v", rowType)
	}
}

func TestGeneratorRuntimeInputTypeOmitsUnresolvedDerivedHelperDuringDiscovery(t *testing.T) {
	events := &spec.Parameter{Name: "Events", TypeExpr: "[]*EventsView", Source: spec.BindSource{Kind: "body", Name: "Events"}}
	helper := &spec.Parameter{Name: "CurEventsID", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	component := &spec.Component{
		Name: "Events", Parameters: []*spec.Parameter{events, helper}, RootView: &spec.View{Name: "Events"},
	}
	inputType, err := New(Input{
		Component:    component,
		Declarations: Declarations{helper.Identity(): {Projection: []DeclarationProjection{{Name: "Values", Source: "ID", Aggregate: true}}}},
	}).RuntimeInputType()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := inputType.FieldByName("Events"); !ok {
		t.Fatalf("source field missing from discovery type: %v", inputType)
	}
	if _, ok := inputType.FieldByName("CurEventsID"); ok {
		t.Fatalf("unresolved helper leaked into discovery type: %v", inputType)
	}
}

func TestGeneratorRuntimeInputTypeIncludesHelperAfterColumnRefinement(t *testing.T) {
	events := &spec.Parameter{Name: "Events", TypeExpr: "[]*EventsView", Source: spec.BindSource{Kind: "body", Name: "Events"}}
	helper := &spec.Parameter{Name: "CurEventsID", Source: spec.BindSource{Kind: "param", Name: "Events"}}
	component := &spec.Component{
		Name: "Events", Parameters: []*spec.Parameter{events, helper}, RootView: &spec.View{Name: "Events", Columns: []*spec.Column{
			{Name: "ID", Type: spec.TypeRef{Name: "int64"}},
		}},
	}
	inputType, err := New(Input{
		Component:    component,
		Declarations: Declarations{helper.Identity(): {Projection: []DeclarationProjection{{Name: "Values", Source: "ID", Aggregate: true}}}},
	}).RuntimeInputType()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := inputType.FieldByName("CurEventsID")
	if !ok || field.Type.Kind() != reflect.Struct || field.Type.NumField() != 1 ||
		field.Type.Field(0).Name != "Values" || field.Type.Field(0).Type != reflect.TypeOf([]int64{}) {
		t.Fatalf("refined helper field = %+v", field)
	}
}

func TestRuntimeInputIncludesTypedRowProjectionAfterRefinement(t *testing.T) {
	records := &spec.Parameter{Name: "Records", TypeExpr: "[]*RecordsView", Source: spec.BindSource{Kind: "body", Name: "Records"}}
	keys := &spec.Parameter{Name: "RecordKeys", Cardinality: "Many", Source: spec.BindSource{Kind: "param", Name: "Records"}}
	component := &spec.Component{Name: "Records", Parameters: []*spec.Parameter{records, keys}, RootView: &spec.View{Name: "Records", Columns: []*spec.Column{
		{Name: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}},
		{Name: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}},
	}}}
	inputType, err := New(Input{Component: component, Declarations: Declarations{keys.Identity(): {Projection: []DeclarationProjection{{Name: "TENANT_ID", Source: "TenantId"}, {Name: "ID", Source: "Id"}}}}}).RuntimeInputType()
	if err != nil {
		t.Fatal(err)
	}
	field, found := inputType.FieldByName("RecordKeys")
	if !found || field.Type.Kind() != reflect.Slice {
		t.Fatalf("typed StructQL row helper missing: %+v", field)
	}
	row := field.Type.Elem()
	if row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	if row.NumField() != 2 || row.Field(0).Name != "TENANT_ID" || row.Field(0).Type != reflect.TypeFor[int64]() || row.Field(1).Name != "ID" || row.Field(1).Type != reflect.TypeFor[*int64]() {
		t.Fatalf("projection aliases/types=%v", row)
	}
}

func TestGeneratedHelperFieldsPreserveQualifiedSource(t *testing.T) {
	for _, test := range []struct{ path, want string }{
		{"", "int64"}, {"Children", "string"}, {"Children/Details", "bool"}, {"Missing", "any"},
	} {
		t.Run(test.path, func(t *testing.T) {
			param := &spec.Parameter{Name: "Keys", Source: spec.BindSource{Kind: "param", Name: "Records"}}
			plan := &Plan{
				Input:       ContractPlan{Fields: []Field{{Name: "Records", Type: "[]*alpha.Record"}}},
				HelperTypes: []HelperType{{Name: "KeysRow", Fields: []Field{{Name: "Key", Type: "any"}}}},
				Views: []ViewPlan{
					{Type: "beta.Record", Fields: []Field{{Name: "ID", Type: "float64"}}},
					{Type: "beta.Child", Fields: []Field{{Name: "ID", Type: "float64"}}},
					{Type: "beta.Detail", Fields: []Field{{Name: "ID", Type: "float64"}}},
					{Type: "alpha.Record", Fields: []Field{{Name: "ID", Type: "int64"}, {Name: "Children", Type: "[]*alpha.Child"}}},
					{Type: "alpha.Child", Fields: []Field{{Name: "ID", Type: "string"}, {Name: "Details", Type: "[]*alpha.Detail"}}},
					{Type: "alpha.Detail", Fields: []Field{{Name: "ID", Type: "bool"}}},
				},
			}
			resolver := &planResolver{plan: plan, input: Input{
				Component:    &spec.Component{Parameters: []*spec.Parameter{param}},
				Declarations: Declarations{param.Identity(): {Projection: []DeclarationProjection{{Name: "Key", Source: "ID"}}, NestedChildField: test.path}},
			}}
			resolver.concretizeGeneratedHelperFields()
			if got := plan.HelperTypes[0].Fields[0].Type; got != test.want {
				t.Fatalf("qualified source %q: got %s, want %s", test.path, got, test.want)
			}
		})
	}
}

func TestGeneratedCurrentKeyHelpersSupportPhysicalAndProjectedColumns(t *testing.T) {
	for _, tc := range []struct {
		name, parameterTag, want string
	}{
		{name: "physical", want: "pod_id"},
		{name: "projected alias", parameterTag: `compositeAlias:"PodId"`, want: "PodId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := &spec.Parameter{Name: "PatchKeys", Source: spec.BindSource{Kind: "param", Name: "Patch"}, Tag: tc.parameterTag}
			plan := &Plan{
				Input:       ContractPlan{Fields: []Field{{Name: "Patch", Type: "*Entitlement"}}},
				HelperTypes: []HelperType{{Name: "PatchKeysRow", Fields: []Field{{Name: "PodId", Type: "any"}}}},
				Views:       []ViewPlan{{Type: "Entitlement", Fields: []Field{{Name: "PodId", Type: "*string", Tag: `sqlx:"pod_id,primaryKey=true"`}}}},
			}
			resolver := &planResolver{plan: plan, input: Input{
				Component:    &spec.Component{Parameters: []*spec.Parameter{param}},
				Declarations: Declarations{param.Identity(): {Projection: []DeclarationProjection{{Name: "PodId", Source: "PodId"}}}},
			}}
			if err := resolver.concretizeGeneratedHelperFields(); err != nil {
				t.Fatal(err)
			}
			field := plan.HelperTypes[0].Fields[0]
			if got := reflect.StructTag(field.Tag).Get("sqlx"); got != tc.want {
				t.Fatalf("sqlx key column=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestRuntimeRowHelpersUseExactGeneratedNestedSource(t *testing.T) {
	for _, test := range []struct {
		path string
		want reflect.Type
	}{
		{"", reflect.TypeFor[int64]()}, {"Children", reflect.TypeFor[string]()}, {"Children.Details", reflect.TypeFor[bool]()},
	} {
		t.Run(test.path, func(t *testing.T) {
			records := &spec.Parameter{Name: "Records", TypeExpr: "[]*RecordsView", Source: spec.BindSource{Kind: "body", Name: "Records"}}
			helper := &spec.Parameter{Name: "Keys", Source: spec.BindSource{Kind: "param", Name: "Records"}}
			details := &spec.View{Name: "Details", Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "bool"}}}}
			children := &spec.View{Name: "Children", Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "string"}}}, Relations: []*spec.Relation{{Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany, View: details}}}
			root := &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int64"}}}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, View: children}}}
			typ, err := New(Input{Component: &spec.Component{Name: "Records", Parameters: []*spec.Parameter{records, helper}, RootView: root}, Declarations: Declarations{helper.Identity(): {Projection: []DeclarationProjection{{Name: "Key", Source: "ID"}}, NestedChildField: test.path}}}).RuntimeInputType()
			if err != nil {
				t.Fatal(err)
			}
			field, found := typ.FieldByName("Keys")
			if !found {
				t.Fatal("nested row helper disappeared")
			}
			if actual := field.Type.Elem().Field(0).Type; actual != test.want {
				t.Fatalf("nested source %q type=%v want=%v", test.path, actual, test.want)
			}
		})
	}
}

func TestGeneratedSelectorNamesRetainDistinctSourceIdentity(t *testing.T) {
	component := &spec.Component{Name: "Selectors", Parameters: []*spec.Parameter{
		{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "inventory", Property: spec.SelectorPropertyFields}},
		{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "product_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "products", Property: spec.SelectorPropertyFields}},
	}}
	typ, err := New(Input{Component: component}).RuntimeInputType()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, source string }{{"Fields", "_fields"}, {"ProductFields", "product_fields"}} {
		field, ok := typ.FieldByName(test.name)
		if !ok || !field.IsExported() || !strings.Contains(field.Tag.Get("parameter"), "in="+test.source) {
			t.Fatalf("field %s: %+v", test.name, field)
		}
	}
	component.Parameters[0].Source.Name = "productFields"
	if _, err = New(Input{Component: component}).RuntimeInputType(); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("physical source collision: %v", err)
	}
}
