package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func sourceHandlerFixture(t *testing.T) (string, *Source) {
	t.Helper()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	data, err := os.ReadFile("testdata/handleronly/contracts.go")
	require.NoError(t, err)
	// Same business contract, but a NEW package identity absent from this test
	// binary. Discovery has neither HandlerBindings nor a contract registry.
	code := strings.Replace(string(data), `"context"`, "\"context\"\n\"os\"", 1)
	code += `
func init() {
    if os.Getenv("DATLY_HANDLER_GENERATING") == "1" { panic("generation executed package init") }
}
`
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	text := fmt.Sprintf(`/* {"URI":"/convert", "Method":"POST", "Name":"Convert", "MCPTool":true,
"Factory":%q, "InputType":%q, "OutputType":%q} */
#package(%q)
#set($_ = $Debug<bool>(form/debug).Optional())
#set($_ = $TenantID<int>(query/tenant_id).Optional())`, handlerFixtureModule+"/business.NewConvert", handlerFixtureModule+"/business.Alias", handlerFixtureModule+"/business.Output", handlerFixtureModule+"/registration")
	return root, &Source{Scope: handlerFixtureModule + "/dql", Name: "convert", Path: root, Text: text,
		GoBuild: &gobuild.Context{Dir: root, Env: []string{"DATLY_HANDLER_GENERATING=1"}}}
}

func writeSourceHandlerFile(t *testing.T, root, name, content string) {
	t.Helper()
	file := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
	require.NoError(t, os.WriteFile(file, []byte(content), 0600))
}

func sourceHandlerSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result[name] = "directory"
			return nil
		}
		data, err := os.ReadFile(name)
		result[name] = string(data)
		return err
	}))
	return result
}

func TestSourceHandlerDiscoveryGenerationRuntime(t *testing.T) {
	ctx := context.Background()
	root, source := sourceHandlerFixture(t)
	writeSourceHandlerFile(t, root, "dql/convert.dql", source.Text)
	db := &forbiddenHandlerDB{}
	discovery := Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild, ColumnRefiner: column.New(db)}
	for range []string{"first", "repeat"} {
		project, err := discovery.Compile(ctx)
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		compiled := project.Components[0]
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Settings.DefaultConnector)
		result, err := (Generator{Operation: "handler"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Equal(t, generate.ContractLinked, result.Result.Plan.Input.Ownership)
		require.Empty(t, result.Result.Plan.Views)
		require.Nil(t, result.Result.Plan.MutationHandler)
	}
	require.Zero(t, db.calls)
	runtimeTest, err := os.ReadFile("testdata/handleronly/runtime_test.go.txt")
	require.NoError(t, err)
	code := strings.ReplaceAll(string(runtimeTest), handlerFixturePackage, handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "registration/runtime_test.go", code)
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-count=1", "-timeout=2m", "./registration")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func TestSourceHandlerFailuresDoNotPublish(t *testing.T) {
	root, original := sourceHandlerFixture(t)
	g := Generator{Operation: "handler"}
	_, err := g.Generate(context.Background(), GenerationRequest{Source: original, Destination: root})
	require.NoError(t, err)
	writeSourceHandlerFile(t, root, "registration/handwritten.go", "package registration\nfunc Handwritten() {}\n")
	// Regeneration preserves authored files and replaces generated artifacts.
	_, err = g.Generate(context.Background(), GenerationRequest{Source: original, Destination: root})
	require.NoError(t, err)
	for _, tc := range []struct{ name, old, new, message string }{
		{"different contract", "/business.Alias", "/business.Different", "exact signature"},
		{"lookalike interface", "/business.NewConvert", "/business.WrongContract", "exact signature"},
		{"missing factory", "/business.NewConvert", "/business.Absent", "not found"},
		{"inaccessible factory", "/business.NewConvert", "/business.hidden", "exported"},
		{"parameter type", "$Debug<bool>", "$Debug<string>", "type conflicts"},
		{"binding", "form/debug", "query/debug", "binding conflicts"},
		{"optionality", "$Debug<bool>(form/debug).Optional()", "$Debug<bool>(form/debug).Required()", "requiredness conflicts"},
		{"obsolete parameter", "$Debug<bool>", "$Obsolete<bool>", "not in the source contract"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := *original
			source.Text = strings.ReplaceAll(source.Text, tc.old, tc.new)
			before := sourceHandlerSnapshot(t, root)
			_, err := g.Generate(context.Background(), GenerationRequest{Source: &source, Destination: root})
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestSourceHandlerStagedBuildAndBuildSelection(t *testing.T) {
	for _, kind := range []string{"cycle", "internal", "excluded", "tags", "excluded cycle", "excluded syntax"} {
		t.Run(kind, func(t *testing.T) {
			root, source := sourceHandlerFixture(t)
			switch kind {
			case "excluded syntax":
				writeSourceHandlerFile(t, root, "registration/excluded.go", "//go:build excluded\n\npackage registration\nthis is not valid Go\n")
			case "cycle", "excluded cycle":
				prefix := ""
				if kind == "excluded cycle" {
					prefix = "//go:build excluded\n\n"
				}
				writeSourceHandlerFile(t, root, "registration/handwritten.go", prefix+"package registration\nimport _ \""+handlerFixtureModule+"/cycle\"\n")
				writeSourceHandlerFile(t, root, "cycle/cycle.go", "package cycle\nimport _ \""+handlerFixtureModule+"/registration\"\n")
			case "internal":
				data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
				require.NoError(t, err)
				writeSourceHandlerFile(t, root, "private/internal/business/contracts.go", string(data))
				source.Text = strings.ReplaceAll(source.Text, "/business.", "/private/internal/business.")
			case "excluded", "tags":
				data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
				require.NoError(t, err)
				writeSourceHandlerFile(t, root, "business/contracts.go", "//go:build customhandler\n\n"+string(data))
				if kind == "tags" {
					source.GoBuild.Tags = "customhandler"
				}
			}
			before := sourceHandlerSnapshot(t, root)
			_, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
			if kind == "tags" || kind == "excluded cycle" || kind == "excluded syntax" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if kind == "cycle" {
				require.ErrorContains(t, err, "import cycle")
			}
			if kind == "internal" {
				require.ErrorContains(t, err, "internal package")
			}
			if kind == "excluded" {
				require.ErrorContains(t, err, "build constraints")
			}
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestSourceHandlerImportedAliasesAndBindings(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	code := strings.Replace(string(data), "Debug     bool", "Common\n\tIgnored bool", 1)
	code += "\ntype Common struct { Debug bool `parameter:\",kind=form,in=debug\"` }\ntype RequestAlias = Request\n"
	// Remove the original debug binding from Ignored; only promoted Common.Debug binds it.
	code = strings.Replace(code, "Ignored bool     `parameter:\",kind=form,in=debug\"`", "Ignored bool", 1)
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text = strings.ReplaceAll(source.Text, handlerFixtureModule+"/business.", "business.")
	writeSourceHandlerFile(t, root, "business/excluded.go", "//go:build excluded\n\npackage handleronly\nthis is not valid Go\n")
	source.Text += "\n#import('business', '" + handlerFixtureModule + "/business')\n#set($_ = $Request<*business.RequestAlias>(body/request).Optional())"
	writeSourceHandlerFile(t, root, "dql/convert.dql", source.Text)
	// Handler imports use the selected Go build, not SQL's AST/resource loader;
	// no reflected identities or excluded declarations are needed.
	discovery := Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild}
	project, err := discovery.Compile(context.Background())
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	_, err = (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: project.Components[0], Destination: root})
	require.NoError(t, err)
}

func TestSourceHandlerExplicitConnectorAndCancellation(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	source.Connector = "explicit_runtime_only"
	db := &forbiddenHandlerDB{}
	source.ColumnRefiner = column.New(db)
	result, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	require.Equal(t, "explicit_runtime_only", result.Result.Plan.Connector)
	require.Zero(t, db.calls)
	before := sourceHandlerSnapshot(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (Generator{Operation: "handler"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
}

func TestSourceHandlerCompiledMappingConflictAndCatalogIsolation(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	source.Text = strings.Replace(source.Text, `"Factory":`, `"Type":"legacy.Handler", "Factory":`, 1)
	source.HandlerBindings = handlerSource(t).HandlerBindings
	source.Types = typecatalog.NewCatalog()
	require.NoError(t, source.Types.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: "example.com/other", Name: "Keep"}))
	before := sourceHandlerSnapshot(t, root)
	_, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.ErrorContains(t, err, "conflicts with compiled mapping")
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
	_, found, err := source.Types.Resolve(typecatalog.TranscribeAuthority, handlerFixtureModule+"/business.Input")
	require.NoError(t, err)
	require.False(t, found)
}

func TestSourceHandlerCompatibleCompiledMapping(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	source := handlerSource(t)
	source.Text = strings.Replace(source.Text, `"Type":"legacy.Handler"`, `"Type":"legacy.Handler", "Factory":"`+handlerFixturePackage+`.NewConvert"`, 1)
	source.Text = strings.Replace(source.Text, "legacy.Input", handlerFixturePackage+".Alias", 1)
	source.Text = strings.Replace(source.Text, "legacy.Output", handlerFixturePackage+".Output", 1)
	source.Text += "\n#package('" + handlerFixtureModule + "/registration')"
	_, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
}

func TestSourceHandlerRequiresHandlerOperation(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	before := sourceHandlerSnapshot(t, root)
	for _, operation := range []string{"get", "post", "put", "patch"} {
		_, err := (Generator{Operation: operation}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
		require.ErrorContains(t, err, "handler")
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	}

}

func TestSourceHandlerWithoutOwnershipManifest(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	generator := Generator{Operation: "handler"}
	for _, path := range []string{"/convert", "/convert/updated"} {
		source.Text = strings.Replace(source.Text, `"URI":"/convert"`, `"URI":"`+path+`"`, 1)
		_, err := generator.Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(root, "registration", ".datly-gen.json"))
		require.True(t, os.IsNotExist(err), "manifest persisted: %v", err)
		router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), "path="+path)
	}
	// Retained application code must still be included in build validation,
	// and a failed validation must leave every destination byte unchanged.
	writeSourceHandlerFile(t, root, "registration/broken.go", "package registration\nvar broken = undefinedSymbol\n")
	before := sourceHandlerSnapshot(t, root)
	_, err := generator.Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.Error(t, err)
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
}

func TestDeclarativeSourceHandlerDiscoveryGenerationRuntime(t *testing.T) {
	ctx := context.Background()
	root, source := sourceHandlerFixture(t)
	source.Text = fmt.Sprintf(`#package(%q)
#import('business',%q)
#setting($_ = $handler_factory('business.NewConvert','Convert'))
#setting($_ = $route('/convert','POST'))
#setting($_ = $input_type('business.Alias'))
#setting($_ = $output_type('business.Output'))
#setting($_ = $mcp('Convert','Typed conversion'))
#setting($_ = $case_format('lc'))
#define($_ = $Debug<bool>(form/debug).Optional())
#define($_ = $TenantID<int>(query/tenant_id).Optional())`, handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "dql/convert.dql", source.Text)
	db := &forbiddenHandlerDB{}
	discovery := Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild, ColumnRefiner: column.New(db)}
	var first map[string]string
	for pass := 0; pass < 2; pass++ {
		project, err := discovery.Compile(ctx)
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		compiled := project.Components[0]
		require.Nil(t, compiled.Component.RootView)
		require.Equal(t, "Convert", compiled.Component.Name)
		require.Equal(t, "Convert", compiled.Component.Routes[0].MCP[0].Name)
		require.Equal(t, "Typed conversion", compiled.Component.Routes[0].MCP[0].Description)
		generated, err := (Generator{Operation: "handler"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Equal(t, generate.ContractLinked, generated.Result.Plan.Input.Ownership)
		snapshot := sourceHandlerSnapshot(t, root)
		if pass == 0 {
			first = snapshot
		} else {
			require.Equal(t, first, snapshot)
		}
	}
	require.Zero(t, db.calls)
	runtimeTest, err := os.ReadFile("testdata/handleronly/runtime_test.go.txt")
	require.NoError(t, err)
	code := strings.ReplaceAll(string(runtimeTest), handlerFixturePackage, handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "registration/runtime_test.go", code)
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-count=1", "-timeout=2m", "./registration")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
