package bootstrap_test

import (
	"context"
	"fmt"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const generatedDiscoveryGoMod = "module example.com/app\n\ngo 1.21\n"

func writeGeneratedDiscoveryFile(t *testing.T, base, relative, content string) {
	t.Helper()
	name := filepath.Join(base, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

// TestDiscoverComponentsFromPackages_DiscoversGeneratedHolder proves a
// gen-emitted scaffold is discovered by the same convention as handwritten
// holders: generated and handwritten route holders are indistinguishable to
// package-authority bootstrap.
func TestDiscoverComponentsFromPackages_DiscoversGeneratedHolder(t *testing.T) {
	base := t.TempDir()
	writeGeneratedDiscoveryFile(t, base, "go.mod", generatedDiscoveryGoMod)
	generatedDir := filepath.Join(base, "svc", "vendors")
	if err := os.MkdirAll(generatedDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	plan := &gen.Plan{
		ComponentName: "VendorCatalog", Description: "Vendor catalog", Example: `{"id":1}`,
		Routes: []gen.RoutePlan{{Method: "GET", Path: "/v1/api/vendors", Marshaller: "tabular", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "vendors.search", Description: "Search vendors"}}}},
		Report: &spec.ReportSettings{Enabled: true, LinkedInputType: "ReportInput", InputLayout: &spec.ReportInputLayout{Dimensions: "Dimensions"}},
		Settings: dtag.Settings{
			CaseFormat: "lc", Format: "tabular_json", DateFormat: "2006-01-02",
			JSONMarshalType: "codec.JSON", JSONUnmarshalType: "codec.Input", XMLUnmarshalType: "codec.XML",
			Cache: &spec.CacheSettings{Enabled: true, Name: "vendors", TTL: "5m", Warmup: &spec.CacheWarmupSettings{IndexColumn: "vendor_id"}},
		},
		ViewDest:   "vendor.go",
		RouterDest: "vendor_router.go",
		Input: gen.ContractPlan{
			Type: "VendorInput", Destination: "vendor_input.go", Ownership: gen.ContractGenerated,
		},
		Output: gen.ContractPlan{
			Type: "VendorOutput", Destination: "vendor_output.go", Ownership: gen.ContractGenerated,
		},
	}
	if _, err := gen.EmitScaffold(generatedDir, plan); err != nil {
		t.Fatalf("emit scaffold failed: %v", err)
	}

	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/vendors"}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected 1 generated holder discovered, got %d: %+v", len(sources), sources)
	}
	got := sources[0]
	if got.HolderType != "Component" || got.FieldName != "Contract" {
		t.Fatalf("expected generated holder Component.Contract, got %s.%s", got.HolderType, got.FieldName)
	}
	if got.Tag.Name != "VendorCatalog" || got.Tag.Method != "GET" || got.Tag.Path != "/v1/api/vendors" || got.Tag.Marshaller != "tabular" ||
		got.Tag.Description != "Vendor catalog" || got.Tag.Example != `{"id":1}` || !got.Tag.Report ||
		got.Tag.Settings.CaseFormat != "lc" || got.Tag.Settings.Cache == nil || len(got.Tag.MCP) != 1 || got.Tag.MCP[0].Name != "vendors.search" {
		t.Fatalf("unexpected generated route metadata: %+v", got)
	}
	if got.InputType != "VendorInput" || got.OutputType != "VendorOutput" {
		t.Fatalf("expected generated contract types, got in=%q out=%q", got.InputType, got.OutputType)
	}
	if got.PackagePath != "example.com/app/svc/vendors" {
		t.Fatalf("expected module-qualified generated package path, got %q", got.PackagePath)
	}
	component, err := got.Resolve(nil, nil)
	if err != nil {
		t.Fatalf("resolve generated holder: %v", err)
	}
	if component.Name != "VendorCatalog" || component.Description != "Vendor catalog" || component.Example != `{"id":1}` ||
		len(component.Routes) != 1 || component.Routes[0].Marshaller != "tabular" || component.Settings == nil ||
		component.Settings.Report == nil || !component.Settings.Report.Enabled || component.Settings.Report.LinkedInputType != "ReportInput" ||
		component.Settings.Report.InputLayout == nil || component.Settings.Report.InputLayout.Dimensions != "Dimensions" ||
		component.Settings.CaseFormat != "lc" || component.Settings.Format != "tabular_json" || component.Settings.DateFormat != "2006-01-02" ||
		component.Settings.JSONMarshalType != "codec.JSON" || component.Settings.JSONUnmarshalType != "codec.Input" || component.Settings.XMLUnmarshalType != "codec.XML" ||
		component.Settings.Cache == nil || component.Settings.Cache.Warmup == nil || component.Settings.Cache.Warmup.IndexColumn != "vendor_id" ||
		len(component.Routes[0].MCP) != 1 || component.Routes[0].MCP[0].Name != "vendors.search" {
		t.Fatalf("generated holder round trip = %+v", component)
	}
}

func TestDiscoverComponentsFromPackagesDiscoversEveryGeneratedRoute(t *testing.T) {
	base := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, base)
	generatedDir := filepath.Join(base, "svc", "orders")
	if err := os.MkdirAll(generatedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	apiKey := " secret,`quoted`=\"value\" "
	plan := &gen.Plan{
		ComponentName: "Orders", Handler: "HandleOrders",
		Routes: []gen.RoutePlan{
			{Name: "List", Method: "GET", Path: "/orders", Marshaller: "json", APIKeyHeader: "X-Key", APIKeyValue: apiKey},
			{Name: "Create", Method: "POST", Path: "/orders", Marshaller: "tabular", APIKeyHeader: "X-Key", APIKeyValue: "write-key"},
		},
		RouterDest: "orders_router.go",
		Input:      gen.ContractPlan{Type: "OrdersInput", Destination: "orders_input.go", Ownership: gen.ContractGenerated},
		Output:     gen.ContractPlan{Type: "OrdersOutput", Destination: "orders_output.go", Ownership: gen.ContractGenerated},
	}
	if _, err := gen.EmitScaffold(generatedDir, plan); err != nil {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/orders"}, nil)
	if err != nil || len(sources) != 2 {
		t.Fatalf("discovered routes = %+v, %v", sources, err)
	}
	for index, want := range []struct {
		field, method, name, marshaller, key string
	}{
		{field: "Contract1", method: "GET", name: "List", marshaller: "json", key: apiKey},
		{field: "Contract2", method: "POST", name: "Create", marshaller: "tabular", key: "write-key"},
	} {
		source := sources[index]
		if source.FieldName != want.field || source.Tag.Method != want.method || source.Tag.RouteName != want.name ||
			source.Tag.Marshaller != want.marshaller || source.Tag.APIKeyHeader != "X-Key" || source.Tag.APIKeyValue != want.key ||
			source.Tag.Handler != "HandleOrders" {
			t.Fatalf("route source %d = %+v", index, source)
		}
		component, resolveErr := source.Resolve(nil, nil)
		if resolveErr != nil {
			t.Fatalf("Resolve() error = %v", resolveErr)
		}
		if component.Name != "Orders" || len(component.Routes) != 1 || component.Routes[0].Name != want.name ||
			component.Routes[0].Method != want.method || component.Routes[0].Marshaller != want.marshaller ||
			component.Routes[0].APIKeyValue != want.key || component.Routes[0].Handler != "HandleOrders" {
			t.Fatalf("resolved route %d = %+v", index, component)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = base
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated package does not compile: %v\n%s", runErr, output)
	}
}

func TestDiscoverComponentsFromPackagesPreservesGeneratedRouteOrder(t *testing.T) {
	base := t.TempDir()
	writeGeneratedDiscoveryFile(t, base, "go.mod", generatedDiscoveryGoMod)
	generatedDir := filepath.Join(base, "svc", "ordered")
	if err := os.MkdirAll(generatedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	plan := &gen.Plan{
		ComponentName: "Ordered", RouterDest: "ordered_router.go",
		Input:  gen.ContractPlan{Type: "OrderedInput", Destination: "ordered_input.go", Ownership: gen.ContractGenerated},
		Output: gen.ContractPlan{Type: "OrderedOutput", Destination: "ordered_output.go", Ownership: gen.ContractGenerated},
	}
	for index := 1; index <= 12; index++ {
		plan.Routes = append(plan.Routes, gen.RoutePlan{Method: "GET", Path: fmt.Sprintf("/routes/%02d", index)})
	}
	if _, err := gen.EmitScaffold(generatedDir, plan); err != nil {
		t.Fatalf("EmitScaffold() error = %v", err)
	}
	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/ordered"}, nil)
	if err != nil || len(sources) != 12 {
		t.Fatalf("discovered routes = %+v, %v", sources, err)
	}
	for index, source := range sources {
		wantField := fmt.Sprintf("Contract%d", index+1)
		wantPath := fmt.Sprintf("/routes/%02d", index+1)
		if source.FieldName != wantField || source.Tag.Path != wantPath {
			t.Fatalf("route %d = %s %s, want %s %s", index, source.FieldName, source.Tag.Path, wantField, wantPath)
		}
	}
}

func TestGeneratedHolderPreservesDisabledReportFacets(t *testing.T) {
	base := t.TempDir()
	writeGeneratedDiscoveryFile(t, base, "go.mod", generatedDiscoveryGoMod)
	source := &bootstrap.RouteSource{
		FieldName: "ReportOnly", PackagePath: "example.com/app/svc/source", InputType: "Input", OutputType: "Output",
		Tag: dtag.Component{Name: "ReportOnly", Method: "GET", Path: "/reports", ReportLinkedInputType: "ReportInput"},
	}
	component, err := source.Resolve(nil, nil)
	if err != nil {
		t.Fatalf("resolve source holder: %v", err)
	}
	if component.Settings == nil || component.Settings.Report == nil || component.Settings.Report.Enabled ||
		component.Settings.Report.LinkedInputType != "ReportInput" {
		t.Fatalf("source report metadata = %+v", component.Settings)
	}
	plan, err := gen.New(gen.Input{Component: component, TargetPackage: "example.com/app/svc/generated"}).Plan()
	if err != nil {
		t.Fatalf("plan generated holder: %v", err)
	}
	generatedDir := filepath.Join(base, "svc", "generated")
	if _, err = gen.EmitScaffold(generatedDir, plan); err != nil {
		t.Fatalf("emit generated holder: %v", err)
	}
	sources, err := bootstrap.DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/generated"}, nil)
	if err != nil || len(sources) != 1 {
		t.Fatalf("discover regenerated holder = %+v, %v", sources, err)
	}
	if sources[0].Tag.Report || sources[0].Tag.ReportLinkedInputType != "ReportInput" {
		t.Fatalf("regenerated tag = %+v", sources[0].Tag)
	}
	reloaded, err := sources[0].Resolve(nil, nil)
	if err != nil {
		t.Fatalf("resolve regenerated holder: %v", err)
	}
	if reloaded.Settings == nil || reloaded.Settings.Report == nil || reloaded.Settings.Report.Enabled ||
		reloaded.Settings.Report.LinkedInputType != "ReportInput" {
		t.Fatalf("regenerated report metadata = %+v", reloaded.Settings)
	}
}
