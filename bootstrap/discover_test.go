package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestPackageDiscoveryRequiresUserSelectedDefaultImport(t *testing.T) {
	base := t.TempDir()
	writeFile(t, base, "go.mod", testGoMod)
	writeFile(t, base, "svc/users/holder.go", usersHolderSource)
	_, err := (PackageDiscovery{
		BaseDir: base, Include: []string{"example.com/app/svc/users"}, RequireLinked: true,
	}).Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "is not linked into the executable") {
		t.Fatalf("expected missing default import error, got %v", err)
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
