package generate

import (
	"github.com/viant/datly/spec"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedSelectorCompanionKeepsBusinessInputAndRegenerates(t *testing.T) {
	component := &spec.Component{Name: "Agency", Routes: []*spec.Route{{Method: "GET", Path: "/agency"}, {Method: "GET", Path: "/agency/{id}"}}, Parameters: []*spec.Parameter{
		{Name: "Name", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "name"}},
		{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "agency", Property: spec.SelectorPropertyPage}},
		{Name: "Id", TypeExpr: "int", Source: spec.BindSource{Kind: "path", Name: "id"}},
	}}
	for _, entry := range []struct {
		name, query, typ string
		property         spec.SelectorProperty
	}{{"Fields", "fields", "[]string", spec.SelectorPropertyFields}, {"Limit", "limit", "int", spec.SelectorPropertyLimit}, {"Offset", "offset", "int", spec.SelectorPropertyOffset}, {"OrderBy", "sort", "[]string", spec.SelectorPropertyOrderBy}} {
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: entry.name, TypeExpr: entry.typ, Source: spec.BindSource{Kind: "query", Name: entry.query}, QuerySelector: &spec.QuerySelectorBinding{View: "agency", Property: entry.property}})
	}
	plan, err := New(Input{Component: component, PackageName: "agency"}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Input.Fields) != 2 || len(plan.QuerySelectors) != 5 {
		t.Fatalf("public=%v side=%v", plan.Input.Fields, plan.QuerySelectors)
	}
	dir := t.TempDir()
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	before := map[string]string{}
	for _, name := range []string{plan.Input.Destination, "input_setters.go", plan.RouterDest} {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		before[name] = string(body)
	}
	if strings.Contains(before[plan.Input.Destination], "Page") || strings.Contains(before["input_setters.go"], "SetPage") {
		t.Fatal("side Page leaked into public API")
	}
	holder := strings.Join(strings.Fields(before[plan.RouterDest]), " ")
	for _, want := range []string{"DatlyQuerySelectors()", "Page int", "Sort []string", "OrderBy,kind=query,in=sort", "Contract1", "Contract2"} {
		if !strings.Contains(holder, want) {
			t.Fatalf("missing %q in %s", want, holder)
		}
	}
	if _, err = EmitScaffold(dir, plan); err != nil {
		t.Fatal(err)
	}
	for name, old := range before {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(body) != old {
			t.Fatalf("second generation changed %s: %v", name, err)
		}
	}
}

func TestSelectorCompanionPreservesExternalLinkedDirectFields(t *testing.T) {
	input := namedDiscoveryDeclaration(t, "DirectInput", "struct { Page int `parameter:\"Page,kind=query,in=page\" querySelector:\"view=agency,property=page\"` }")
	output := namedDiscoveryDeclaration(t, "DirectOutput", "struct {}")
	resolver := namedDiscoveryResolver(t, input, output)
	component := &spec.Component{Name: "Agency", Routes: []*spec.Route{{Method: "GET", Path: "/agency"}}, Parameters: []*spec.Parameter{
		{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "agency", Property: spec.SelectorPropertyPage}},
		{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "agency", Property: spec.SelectorPropertyFields}},
	}}
	plan, err := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/agency", PackageName: "agency", Contracts: ContractReferences{
		Input: &ContractReference{Expression: "models.DirectInput", DescriptorKey: input.Key()}, Output: &ContractReference{Expression: "models.DirectOutput", DescriptorKey: output.Key()},
	}}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Input.Ownership != ContractLinked || len(plan.QuerySelectors) != 1 || plan.QuerySelectors[0].Name != "Fields" {
		t.Fatalf("direct external contract lost: input=%+v side=%+v", plan.Input, plan.QuerySelectors)
	}
	public := false
	for _, field := range plan.Input.Fields {
		if field.Name == "Page" {
			public = true
		}
	}
	if !public {
		t.Fatal("direct Page removed from existing linked input metadata")
	}
	holder, err := componentFileText("agency", plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(holder, "Page int") || !strings.Contains(holder, "DatlyQuerySelectors") {
		t.Fatal("mixed direct/side projection changed")
	}
}

func TestGeneratedPostReaderSelectorsAreSideInputs(t *testing.T) {
	for _, methods := range [][]string{{"POST"}, {"GET", "POST"}} {
		c := &spec.Component{Name: "Rows", Parameters: []*spec.Parameter{
			{Name: "Minimum", TypeExpr: "int", Source: spec.BindSource{Kind: "body", Name: "minimum"}},
			{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyPage}},
		}}
		for _, method := range methods {
			c.Routes = append(c.Routes, &spec.Route{Method: method, Path: "/rows"})
		}
		plan, err := New(Input{Component: c, PackageName: "rows"}).Plan()
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.QuerySelectors) != 1 || len(plan.Input.Fields) != 1 || plan.Input.Fields[0].Name != "Minimum" {
			t.Fatalf("POST side/public mismatch: %+v / %+v", plan.QuerySelectors, plan.Input.Fields)
		}
	}
}

func TestCustomSelectorTransportPreservesExistingBindingObligations(t *testing.T) {
	required := true
	value := "2"
	component := &spec.Component{Name: "Proxy", Routes: []*spec.Route{{Method: "GET", Path: "/proxy", Handler: "NewProxy"}}, Parameters: []*spec.Parameter{
		{Name: "Page", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "page"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyPage}},
		{Name: "Limit", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyLimit}},
		{Name: "Offset", TypeExpr: "int", Value: &value, Source: spec.BindSource{Kind: "query", Name: "offset"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyOffset}},
		{Name: "Fields", TypeExpr: "[]string", Scope: "custom", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyFields}},
	}}
	plan, err := New(Input{Component: component, PackageName: "proxy"}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.QuerySelectors) != 1 || plan.QuerySelectors[0].Name != "Page" {
		t.Fatalf("transport fields: %+v", plan.QuerySelectors)
	}
	for _, name := range []string{"Limit", "Offset", "Fields"} {
		if _, ok := plan.Input.Field(name); !ok {
			t.Fatalf("existing public binding %s lost", name)
		}
	}
	if _, ok := plan.Input.Field("Page"); ok {
		t.Fatal("optional transport Page leaked into Input")
	}
	side, err := New(Input{Component: component}).SelectorSideParameters()
	if err != nil || len(side) != 1 || side[0] != nil && side[0].Identity() != component.Parameters[0].Identity() {
		t.Fatalf("source admission differs from generated storage: %v %v", side, err)
	}
}

func TestCustomSelectorTransportRetainsErrorAndCountBindings(t *testing.T) {
	count := 1
	for _, parameter := range []*spec.Parameter{
		{ErrorStatusCode: 422}, {ErrorMessage: "selector failure"}, {MinAllowedRecords: &count}, {MaxAllowedRecords: &count}, {ExpectedReturned: &count},
	} {
		parameter.Name = "Limit"
		parameter.TypeExpr = "int"
		parameter.Source = spec.BindSource{Kind: "query", Name: "limit"}
		parameter.QuerySelector = &spec.QuerySelectorBinding{View: "rows", Property: spec.SelectorPropertyLimit}
		component := &spec.Component{Name: "Proxy", Routes: []*spec.Route{{Method: "GET", Path: "/proxy", Handler: "NewProxy"}}, Parameters: []*spec.Parameter{parameter}}
		plan, err := New(Input{Component: component, PackageName: "proxy"}).Plan()
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.QuerySelectors) != 0 {
			t.Fatalf("binding responsibility moved to transport: %+v", parameter)
		}
		if _, ok := plan.Input.Field("Limit"); !ok {
			t.Fatal("existing custom binding lost")
		}
	}
}
