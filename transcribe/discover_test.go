package transcribe

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	gen "github.com/viant/datly/transcribe/generate"
)

const discoverGoMod = "module example.com/app\n\ngo 1.25.0\n"

const usersSource = `#setting($_ = $route('/v1/api/users', 'GET'))
#setting($_ = $input_type('UsersInput'))
#setting($_ = $output_type('UsersOutput'))
SELECT * FROM users`

const accountsSource = `#setting($_ = $route('/v1/api/accounts', 'GET'))
SELECT * FROM accounts`

const discoveredPackageComponent = `package users

import (
	model "example.com/generated/model"
	xdatly "github.com/viant/xdatly"
)

type UserRow struct {
	ID int ` + "`" + `sqlx:"ID"` + "`" + `
}

type UsersInput struct {
	Tenant string            ` + "`" + `parameter:"Tenant,kind=query,in=tenant"` + "`" + `
	Audit  []*model.AuditRow ` + "`" + `parameter:"Audit,kind=view,in=Audit" view:"Audit,table=audit"` + "`" + `
}

type UsersOutput struct {
	Data []*UserRow ` + "`" + `parameter:"Data,kind=output,in=view" view:"Users,table=users" sql:"SELECT ID FROM users"` + "`" + `
}

type Component struct {
	Contract xdatly.Component[UsersInput, UsersOutput] ` + "`" + `component:"Users,path=/v1/users,method=GET,view=Users,handler=UsersHandler"` + "`" + `
}
`

func writeSourceFile(t *testing.T, baseDir, relPath, content string) {
	t.Helper()
	full := filepath.Join(baseDir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

func TestDiscoveryCompilesCanonicalProjectOnce(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/users/users.sql", usersSource)
	writeSourceFile(t, base, "svc/users/migration.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	writeSourceFile(t, base, "svc/accounts/accounts.dql", accountsSource)

	project, err := (&Discovery{
		BaseDir: base, Include: []string{"example.com/app/svc/..."}, Connector: "analytics",
	}).Compile(context.Background())
	if err != nil {
		t.Fatalf("compile discovery: %v", err)
	}
	if len(project.Components) != 2 {
		t.Fatalf("components = %d: %+v", len(project.Components), project.Components)
	}
	accounts, users := project.Components[0], project.Components[1]
	if accounts.Source.Scope != "example.com/app/svc/accounts" || users.Source.Scope != "example.com/app/svc/users" {
		t.Fatalf("component order/scopes = %q, %q", accounts.Source.Scope, users.Source.Scope)
	}
	if users.Source.Name != "users" || users.Source.Connector != "analytics" || users.Source.Resources == nil {
		t.Fatalf("users source = %+v", users.Source)
	}
	resourceData, err := users.Source.Resources.ReadFile("migration.sql")
	if err != nil || string(resourceData) != "CREATE TABLE users (id INTEGER PRIMARY KEY);" {
		t.Fatalf("users source resource = %q, %v", resourceData, err)
	}
	if users.Source.Types != accounts.Source.Types {
		t.Fatal("discovered components do not share project type authority")
	}
	if len(users.Component.Routes) != 1 || users.Component.Routes[0].Method != "GET" || users.Component.Routes[0].Path != "/v1/api/users" {
		t.Fatalf("users routes = %+v", users.Component.Routes)
	}
}

func TestDiscoveryComposesExactPackageAndDQLAuthority(t *testing.T) {
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "model/audit.go", "package model\n\ntype AuditRow struct { ID int `sqlx:\"ID\"` }\n")
	writeSourceFile(t, base, "unrelated/noise.go", "package unrelated\n\ntype Noise struct{}\n")
	writeSourceFile(t, base, "svc/users/component.go", discoveredPackageComponent)
	writeSourceFile(t, base, "svc/users/Users.sql", `#setting($_ = $route('/v1/users', 'GET'))
SELECT ID FROM users`)

	project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/generated/svc/users"}}).Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("components = %+v", project.Components)
	}
	compiled := project.Components[0]
	if compiled.Source.PackageComponent == nil || compiled.Source.PackageComponent.Name != "Users" {
		t.Fatalf("package authority = %+v", compiled.Source.PackageComponent)
	}
	if compiled.Component.Settings == nil || compiled.Component.Settings.InputType != "UsersInput" {
		t.Fatalf("compiled settings = %+v", compiled.Component.Settings)
	}
	if len(compiled.Component.Parameters) != 3 {
		t.Fatalf("compiled param count = %d", len(compiled.Component.Parameters))
	}
	for index, expected := range []string{"Tenant", "Audit", "Data"} {
		if compiled.Component.Parameters[index] == nil || compiled.Component.Parameters[index].Name != expected {
			t.Fatalf("compiled param[%d] = %+v, want %s", index, compiled.Component.Parameters[index], expected)
		}
	}
	if len(compiled.Component.Routes) != 1 || compiled.Component.Routes[0].Handler != "UsersHandler" {
		t.Fatalf("compiled routes = %+v", compiled.Component.Routes)
	}
	if compiled.Contracts.Input == nil || compiled.Contracts.Input.Expression != "UsersInput" {
		t.Fatalf("linked input contract = %+v", compiled.Contracts.Input)
	}
	var linkedAudit bool
	for _, view := range compiled.Views {
		if view != nil && view.DescriptorKey == "example.com/generated/model.AuditRow" {
			linkedAudit = true
		}
	}
	if !linkedAudit {
		t.Fatalf("linked independent views = %+v", compiled.Views)
	}
	descriptor, err := compiled.TypeResolver.Descriptor("UsersInput")
	if err != nil || descriptor == nil || descriptor.SynteticType == nil || descriptor.Type != nil {
		t.Fatalf("package descriptor = %+v, %v", descriptor, err)
	}
	if unrelated, resolveErr := compiled.TypeResolver.Descriptor("example.com/generated/unrelated.Noise"); resolveErr != nil || unrelated != nil {
		t.Fatalf("unrelated package entered authority: %+v, %v", unrelated, resolveErr)
	}
	generated, err := project.Generate(context.Background(), base)
	if err != nil || len(generated.Components) != 1 || generated.Components[0].Result.Plan.Input.Ownership != gen.ContractLinked {
		t.Fatalf("Generate() = %+v, %v", generated, err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = base
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated package+DQL project failed: %v\n%s", runErr, output)
	}
}

func TestDiscoveryIgnoresMalformedGoOutsideSelectedAuthority(t *testing.T) {
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "svc/users/component.go", discoveredPackageComponent)
	writeSourceFile(t, base, "model/audit.go", "package model\n\ntype AuditRow struct{}\n")
	writeSourceFile(t, base, "svc/users/Users.sql", `#setting($_ = $route('/v1/users', 'GET'))
SELECT ID FROM users`)
	writeSourceFile(t, base, "unrelated/bad.go", "package unrelated\n\ntype {\n")

	project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/generated/svc/users"}}).Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(project.Components) != 1 || project.Components[0].Source.PackageComponent == nil {
		t.Fatalf("components = %+v", project.Components)
	}
}

func TestDiscoveryDoesNotCaptureCaseDifferentPackageComponent(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/users/component.go", strings.ReplaceAll(discoveredPackageComponent, "example.com/generated/model", "example.com/app/model"))
	writeSourceFile(t, base, "model/audit.go", "package model\n\ntype AuditRow struct{}\n")
	writeSourceFile(t, base, "svc/users/users.sql", `#setting($_ = $route('/v1/lower-users', 'GET'))
SELECT 1`)

	project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/app/svc/users"}}).Compile(context.Background())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(project.Components) != 2 {
		t.Fatalf("expected independent Go and DQL components: %+v", project.Components)
	}
	for _, compiled := range project.Components {
		switch compiled.Source.Name {
		case "users":
			if compiled.Source.PackageComponent != nil || compiled.Contracts.Input != nil || compiled.Component.Routes[0].Path != "/v1/lower-users" {
				t.Fatalf("case-different DQL captured package authority: %+v", compiled)
			}
		case "Users":
			if compiled.Source.PackageComponent == nil || compiled.Contracts.Input == nil || compiled.Component.Routes[0].Path != "/v1/users" {
				t.Fatalf("Go-only package authority lost: %+v", compiled)
			}
		default:
			t.Fatalf("unexpected component %q", compiled.Source.Name)
		}
	}
}

func TestDiscoveryRejectsConflictingPackageContractAuthorities(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/users/component.go", `package users

import xdatly "github.com/viant/xdatly"

type InputA struct{}
type OutputA struct{}
type InputB struct{}
type OutputB struct{}

type Component struct {
	A xdatly.Component[InputA, OutputA] `+"`"+`component:"Users,path=/a,method=GET"`+"`"+`
	B xdatly.Component[InputB, OutputB] `+"`"+`component:"Users,path=/b,method=GET"`+"`"+`
}
`)

	_, err := (&Discovery{BaseDir: base, Include: []string{"example.com/app/svc/users"}}).Compile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "more than one contract authority") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestDiscoveryExcludeFiltersPackages(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/users/users.sql", usersSource)
	writeSourceFile(t, base, "svc/accounts/accounts.sql", accountsSource)

	project, err := (&Discovery{
		BaseDir: base, Include: []string{"example.com/app/svc/..."}, Exclude: []string{"example.com/app/svc/accounts"},
	}).Compile(context.Background())
	if err != nil {
		t.Fatalf("compile discovery: %v", err)
	}
	if len(project.Components) != 1 || project.Components[0].Source.Scope != "example.com/app/svc/users" {
		t.Fatalf("filtered components = %+v", project.Components)
	}
}

func TestDiscoveryRequiresConfiguration(t *testing.T) {
	if _, err := (*Discovery)(nil).Compile(context.Background()); err == nil {
		t.Fatal("expected nil discovery to fail")
	}
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	if _, err := (&Discovery{BaseDir: base}).Compile(context.Background()); err == nil {
		t.Fatal("expected empty include to fail")
	}
}

func TestDiscoveryIgnoresPlainSQL(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/users/migration.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/app/svc/..."}}).Compile(context.Background())
	if err != nil {
		t.Fatalf("compile discovery: %v", err)
	}
	if len(project.Components) != 0 {
		t.Fatalf("components = %+v", project.Components)
	}
}

func TestDiscoveryReportsMalformedComponentPath(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	bad := filepath.Join(base, "svc/users/bad.sql")
	writeSourceFile(t, base, "svc/users/bad.sql", `#setting($_ = $route('', 'GET'))
SELECT * FROM users`)

	_, err := (&Discovery{BaseDir: base, Include: []string{"example.com/app/svc/..."}}).Compile(context.Background())
	var compileErr *CompileError
	if err == nil || !errors.As(err, &compileErr) || !strings.Contains(err.Error(), bad) {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestDiscoveryGenerateResolvesSourceLocalResources(t *testing.T) {
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "svc/users/users.sql", `#setting($_ = $route('/v1/api/users', 'GET'))
SELECT * FROM (${embed:sql/users.sql}) users`)
	writeSourceFile(t, base, "svc/users/sql/users.sql", "SELECT 1 AS id")
	writeSourceFile(t, base, "svc/accounts/accounts.dql", accountsSource)

	generated, err := (&Discovery{BaseDir: base, Include: []string{"example.com/generated/svc/..."}}).Generate(context.Background(), base)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if len(generated.Components) != 2 || len(generated.Manifest.Components) != 2 {
		t.Fatalf("generated project = %+v", generated)
	}
	var entry ProjectComponent
	for _, component := range generated.Manifest.Components {
		if component.Key.Name == "users" {
			entry = component
			break
		}
	}
	if entry.Key.Name == "" {
		t.Fatalf("users component missing from manifest: %+v", generated.Manifest.Components)
	}
	if _, err = os.Stat(filepath.Join(base, filepath.FromSlash(entry.Package), "users_router.go")); err != nil {
		t.Fatalf("generated component missing: %v", err)
	}
}

func TestDiscoveryHonorsCancellation(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&Discovery{BaseDir: base, Include: []string{"example.com/app/..."}}).Compile(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Compile() error = %v", err)
	}
}
