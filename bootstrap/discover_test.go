package bootstrap

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/testharness"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
)

// writeFile writes a file at baseDir/relPath, creating parent directories.
func writeFile(t *testing.T, baseDir, relPath, content string) {
	t.Helper()
	full := filepath.Join(baseDir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

const testGoMod = "module example.com/app\n\ngo 1.21\n"

const usersHolderSource = `package users

import xdatly "github.com/viant/xdatly"

type UsersInput struct{}
type UsersOutput struct{}

type Routes struct {
	List  xdatly.Component[UsersInput, UsersOutput] ` + "`" + `component:",path=/v1/api/users,method=GET,view=UserView"` + "`" + `
	Plain xdatly.Component[UsersInput, UsersOutput]
}
`

// accountsHolderSource uses a non-default import alias to prove alias-aware
// component type resolution.
const accountsHolderSource = `package accounts

import xd "github.com/viant/xdatly"

type AccountsInput struct{}
type AccountsOutput struct{}

type Holder struct {
	List xd.Component[AccountsInput, AccountsOutput] ` + "`" + `component:",path=/v1/api/accounts,method=GET"` + "`" + `
}
`

const importedContractHolderSource = `package imported

import (
	xdatly "github.com/viant/xdatly"
	model "example.com/app/model"
)

type Holder struct {
	List xdatly.Component[*model.Input, []*model.Output] ` + "`" + `component:",path=/v1/api/imported,method=GET"` + "`" + `
}
`

const unaliasedContractHolderSource = `package imported

import (
	xdatly "github.com/viant/xdatly"
	"example.com/app/model"
)

type Holder struct {
	List xdatly.Component[model.Input, model.Output] ` + "`" + `component:",path=/v1/api/imported,method=GET"` + "`" + `
}
`

const unknownContractAliasHolderSource = `package imported

import xdatly "github.com/viant/xdatly"

type Holder struct {
	List xdatly.Component[missing.Input, missing.Output] ` + "`" + `component:",path=/v1/api/imported,method=GET"` + "`" + `
}
`

// taggedTestFileSource lives in a _test.go file and must be ignored.
const taggedTestFileSource = `package users

import xdatly "github.com/viant/xdatly"

type testRoutes struct {
	Hidden xdatly.Component[UsersInput, UsersOutput] ` + "`" + `component:",path=/v1/api/hidden,method=GET"` + "`" + `
}
`

func sourceByRoutePath(sources []*RouteSource, path string) *RouteSource {
	for _, s := range sources {
		if s.Tag.Path == path {
			return s
		}
	}
	return nil
}

// TestDiscoverComponentsFromPackages_DiscoversTaggedHolder proves one package
// pattern discovers a tagged Go holder field with full metadata, while untagged
// Component fields and _test.go files are ignored.
func TestDiscoverComponentsFromPackages_DiscoversTaggedHolder(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/users/holder.go", usersHolderSource)
	writeFile(t, base, "svc/users/holder_extra_test.go", taggedTestFileSource)

	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected 1 discovered holder (untagged + _test.go ignored), got %d: %+v", len(sources), sources)
	}
	got := sources[0]
	if got.HolderType != "Routes" || got.FieldName != "List" {
		t.Fatalf("expected holder Routes.List, got %s.%s", got.HolderType, got.FieldName)
	}
	if got.PackageName != "users" {
		t.Fatalf("expected package name 'users', got %q", got.PackageName)
	}
	if got.PackagePath != "example.com/app/svc/users" {
		t.Fatalf("expected module-qualified package path, got %q", got.PackagePath)
	}
	if got.Tag.Method != "GET" || got.Tag.Path != "/v1/api/users" || got.Tag.View != "UserView" {
		t.Fatalf("unexpected route metadata: %+v", got)
	}
	if got.InputType != "UsersInput" || got.OutputType != "UsersOutput" {
		t.Fatalf("expected generic-argument contract types, got in=%q out=%q", got.InputType, got.OutputType)
	}
	if filepath.Base(got.SourceFile) != "holder.go" || filepath.Base(got.Dir) != "users" {
		t.Fatalf("expected source file/dir to be populated, got file=%q dir=%q", got.SourceFile, got.Dir)
	}
}

// TestDiscoverComponentsFromPackages_AliasedImportAndExclude proves alias-aware
// component resolution and exclude patterns over import paths.
func TestDiscoverComponentsFromPackages_AliasedImportAndExclude(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/users/holder.go", usersHolderSource)
	writeFile(t, base, "svc/accounts/holder.go", accountsHolderSource)

	all, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 discovered holders, got %d: %+v", len(all), all)
	}
	accounts := sourceByRoutePath(all, "/v1/api/accounts")
	if accounts == nil || accounts.InputType != "AccountsInput" {
		t.Fatalf("expected aliased import holder discovered, got %+v", accounts)
	}

	filtered, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, []string{"example.com/app/svc/accounts"})
	if err != nil {
		t.Fatalf("discover with exclude failed: %v", err)
	}
	if len(filtered) != 1 || filtered[0].PackagePath != "example.com/app/svc/users" {
		t.Fatalf("expected accounts package excluded, got %+v", filtered)
	}
}

func TestDiscoverComponentsFromPackagesRetainsOnlyContractImports(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/imported/holder.go", importedContractHolderSource)

	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 {
		t.Fatalf("sources = %+v", sources)
	}
	actual := sources[0]
	if actual.InputType != "*model.Input" || actual.OutputType != "[]*model.Output" || len(actual.Imports) != 1 ||
		actual.Imports[0].Alias != "model" || actual.Imports[0].Package != "example.com/app/model" {
		t.Fatalf("contract authority = %+v", actual)
	}
	component, err := actual.Resolve(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings == nil || component.Settings.InputType != "*model.Input" ||
		component.Settings.OutputType != "[]*model.Output" || component.TypeContext == nil || len(component.TypeContext.Imports) != 1 {
		t.Fatalf("canonical contract context = %+v", component)
	}
}

func TestDiscoverComponentsFromPackagesRejectsUnknownContractAlias(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/imported/holder.go", unknownContractAliasHolderSource)

	_, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown package alias") {
		t.Fatalf("expected unknown contract alias error, got %v", err)
	}
}

func TestDiscoverComponentsFromPackagesRejectsGuessedContractAlias(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/imported/holder.go", unaliasedContractHolderSource)

	_, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err == nil || !strings.Contains(err.Error(), "must use an explicit import alias") {
		t.Fatalf("expected explicit contract alias error, got %v", err)
	}
}

// TestDiscoverComponentsFromPackages_EmptyIncludeFails proves an empty include
// set is an explicit error, not a silent repo-wide scan.
func TestDiscoverComponentsFromPackages_EmptyIncludeFails(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)

	if _, err := DiscoverComponentsFromPackages(context.Background(), base, nil, nil); err == nil {
		t.Fatalf("expected empty include to fail")
	}
}

// TestDiscoverComponentsFromPackages_IncompleteTagFails proves an incomplete
// component tag (missing method) is a hard error, not a silent skip.
func TestDiscoverComponentsFromPackages_IncompleteTagFails(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/bad/holder.go", `package bad

import xdatly "github.com/viant/xdatly"

type BadInput struct{}
type BadOutput struct{}

type Holder struct {
	List xdatly.Component[BadInput, BadOutput] `+"`"+`component:",path=/v1/api/bad"`+"`"+`
}
`)

	_, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err == nil {
		t.Fatalf("expected incomplete component tag to fail discovery")
	}
	if !strings.Contains(err.Error(), "incomplete component tag") {
		t.Fatalf("expected incomplete-tag error, got %v", err)
	}
}

func TestScanFileHolders_AcceptsInterpretedStructTagLiteral(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holder.go")
	writeFile(t, filepath.Dir(path), filepath.Base(path), `package users

import xdatly "github.com/viant/xdatly"

type Input struct{}
type Output struct{}
type Routes struct {
	List xdatly.Component[Input, Output] "component:\",path=/v1/users,method=GET\""
}
`)
	holders, err := scanFileHolders(path)
	if err != nil {
		t.Fatalf("scanFileHolders() error = %v", err)
	}
	if len(holders.fields) != 1 || holders.fields[0].tag.Path != "/v1/users" {
		t.Fatalf("unexpected holders: %+v", holders.fields)
	}
}

// TestDiscoverComponentsFromPackages_TaggedNonHolderFails proves a
// component-tagged field that is not a Component[I, O] holder is a hard error.
func TestDiscoverComponentsFromPackages_TaggedNonHolderFails(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/wrong/holder.go", `package wrong

type Holder struct {
	List string `+"`"+`component:",path=/v1/api/wrong,method=GET"`+"`"+`
}
`)

	_, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/..."}, nil)
	if err == nil {
		t.Fatalf("expected tagged non-holder field to fail discovery")
	}
	if !strings.Contains(err.Error(), "is not a") {
		t.Fatalf("expected non-holder error, got %v", err)
	}
}

// TestDiscoverComponentsFromPackages_DiscoversGeneratedHolder proves a
// gen-emitted scaffold is discovered by the same convention as handwritten
// holders: generated and handwritten route holders are indistinguishable to
// package-authority bootstrap.
func TestDiscoverComponentsFromPackages_DiscoversGeneratedHolder(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
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

	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/vendors"}, nil)
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
	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/orders"}, nil)
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
	writeFile(t, base, "go.mod", testGoMod)
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
	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/ordered"}, nil)
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
	writeFile(t, base, "go.mod", testGoMod)
	source := &RouteSource{
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
	sources, err := DiscoverComponentsFromPackages(context.Background(), base, []string{"example.com/app/svc/generated"}, nil)
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
