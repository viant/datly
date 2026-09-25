package transcribe

import (
	"context"
	"database/sql"
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
	fixture "github.com/viant/datly/transcribe/testdata/handleronly"
)

const handlerFixturePackage = "github.com/viant/datly/transcribe/testdata/handleronly"
const handlerFixtureModule = "github.com/viant/datly/handlerfixture"
const handlerDQL = `/* {
"URI":"/convert", "Method":"POST", "Name":"Convert", "Description":"Convert expressions",
"Type":"legacy.Handler", "InputType":"legacy.Input", "OutputType":"legacy.Output", "MCPTool":true
} */
#set($_ = $Debug<bool>(form/debug).Optional())`

func handlerMapping() HandlerMapping {
	return HandlerMapping{LegacyType: "legacy.Handler", LegacyInput: "legacy.Input", LegacyOutput: "legacy.Output",
		FactoryPackage: handlerFixturePackage, FactoryName: "NewConvert", DestinationPackage: handlerFixtureModule + "/registration"}
}

func handlerSource(t *testing.T) *Source {
	t.Helper()
	binding, err := NewHandlerBinding[fixture.Input, fixture.Output](handlerMapping(), fixture.NewConvert)
	require.NoError(t, err)
	return &Source{Scope: handlerFixtureModule, Name: "convert", Text: handlerDQL,
		HandlerBindings: []*HandlerBinding{binding}, ColumnRefiner: column.New(nil)}
}

func TestHandlerBindingIdentity(t *testing.T) {
	before := fixture.Constructions.Load()
	_, err := NewHandlerBinding[fixture.Alias, fixture.Output](handlerMapping(), fixture.NewConvert)
	require.NoError(t, err)
	for _, factory := range []any{nil, fixture.WrongInput, fixture.WrongContract, func() {}} {
		_, err = NewHandlerBinding[fixture.Input, fixture.Output](handlerMapping(), factory)
		require.ErrorContains(t, err, "exact signature")
	}
	mapping := handlerMapping()
	mapping.FactoryName = "Other"
	_, err = NewHandlerBinding[fixture.Input, fixture.Output](mapping, fixture.NewConvert)
	require.ErrorContains(t, err, "does not resolve")
	require.Equal(t, before, fixture.Constructions.Load())
}

func TestHandlerOnlyCompilation(t *testing.T) {
	before := fixture.Constructions.Load()
	for _, connector := range []string{"", "explicit"} {
		source := handlerSource(t)
		source.Text += "\n#setting($_ = $case_format('lc'))"
		source.Connector = connector
		compiled, err := NewCompiler().Compile(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Settings.Mutation)
		require.Equal(t, connector, compiled.Component.Settings.DefaultConnector)
		require.Equal(t, "lc", compiled.Component.Settings.CaseFormat)
		require.Equal(t, "POST", compiled.Component.Routes[0].Method)
		require.Equal(t, "Convert", compiled.Component.Routes[0].MCP[0].Name)
		require.NotNil(t, compiled.Contracts.Input)
	}
	require.Equal(t, before, fixture.Constructions.Load())
}

func TestHandlerOnlyParameterUsesLinkedBindingName(t *testing.T) {
	source := handlerSource(t)
	source.Text += "\n#set($_ = $TenantID<int>(query/tenant_id).Optional())"
	_, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	source.Text = strings.ReplaceAll(source.Text, "$TenantID<int>", "$TenantID<string>")
	_, err = NewCompiler().Compile(context.Background(), source)
	require.ErrorContains(t, err, "type conflicts")
}

type forbiddenHandlerDB struct{ calls int }

func (r *forbiddenHandlerDB) ResolveDB(context.Context, string) (*sql.DB, error) {
	r.calls++
	return nil, fmt.Errorf("handler discovery must not open a connector")
}

func TestHandlerOnlyDiscovery(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "dql"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dql", "convert.dql"), []byte(handlerDQL), 0600))
	source := handlerSource(t)
	db := &forbiddenHandlerDB{}
	discovery := Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, HandlerBindings: source.HandlerBindings, ColumnRefiner: column.New(db), Connector: "explicit"}
	before := fixture.Constructions.Load()
	for range 2 {
		project, err := discovery.Compile(ctx)
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		compiled := project.Components[0]
		require.Equal(t, "explicit", compiled.Component.Settings.DefaultConnector)
		_, err = (Generator{Operation: "handler", GenerationPolicy: generate.GenerationPolicyOverwrite}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
	}
	require.Zero(t, db.calls)
	require.Equal(t, before, fixture.Constructions.Load())
}

func TestHandlerOnlyUnusedDeclaration(t *testing.T) {
	mapping := handlerMapping()
	mapping.UnusedParameters = map[string]string{"Obsolete": "v1 adapter has no use for this legacy flag"}
	binding, err := NewHandlerBinding[fixture.Input, fixture.Output](mapping, fixture.NewConvert)
	require.NoError(t, err)
	// Caller-owned maps cannot change a validated binding.
	mapping.UnusedParameters["Obsolete"] = ""
	source := handlerSource(t)
	source.HandlerBindings = []*HandlerBinding{binding}
	source.Text = strings.ReplaceAll(source.Text, "$Debug<bool>", "$Obsolete<bool>")
	compiled, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	require.Len(t, compiled.Diagnostics, 1)
	require.Contains(t, compiled.Diagnostics[0].Message, "v1 adapter has no use")
	for _, p := range compiled.Component.Parameters {
		require.NotEqual(t, "Obsolete", p.Name)
	}
	source.Text = strings.ReplaceAll(source.Text, ".Optional()", "")
	_, err = NewCompiler().Compile(context.Background(), source)
	require.ErrorContains(t, err, "explicitly optional")
}

func TestHandlerOnlyGeneration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	source := handlerSource(t)
	before := fixture.Constructions.Load()
	for _, policy := range []generate.GenerationPolicy{"", "", generate.GenerationPolicyOverwrite} {
		result, err := (Generator{Operation: "handler", GenerationPolicy: policy}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
		require.Equal(t, generate.ContractLinked, result.Result.Plan.Input.Ownership)
		require.Equal(t, generate.ContractLinked, result.Result.Plan.Output.Ownership)
		require.Empty(t, result.Result.Plan.Views)
		require.Nil(t, result.Result.Plan.MutationHandler)
		require.NotNil(t, result.Result.Plan.FactoryLink)
	}
	require.Equal(t, before, fixture.Constructions.Load())
	router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
	require.NoError(t, err)
	require.Contains(t, string(router), "handler="+handlerFixturePackage+".NewConvert")
	require.NotContains(t, string(router), "connector=")
	require.NotContains(t, string(router), "view=")
	runtimeTest, err := os.ReadFile("testdata/handleronly/runtime_test.go.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "registration", "runtime_test.go"), runtimeTest, 0600))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-timeout=2m", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func TestHandlerOnlyFailureDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	source := handlerSource(t)
	source.Text += "\n#setting($_ = $case_format('lc'))"
	g := Generator{Operation: "handler", GenerationPolicy: generate.GenerationPolicyOverwrite}
	_, err := g.Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	snapshot := func(directory string) map[string]string {
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(directory, func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if e.IsDir() {
				files[path] = "directory"
				return nil
			}
			b, err := os.ReadFile(path)
			files[path] = string(b)
			return err
		}))
		return files
	}
	before := snapshot(root)
	for _, change := range []func(*Source){
		func(s *Source) { s.HandlerBindings = nil },
		func(s *Source) { s.HandlerBindings = append(s.HandlerBindings, s.HandlerBindings[0]) },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "legacy.Input", "wrong.Input") },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "legacy.Output", "wrong.Output") },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "legacy.Handler", "wrong.Handler") },
		func(s *Source) { s.Text += "\nSELECT 1" },
		func(s *Source) { s.Text += "\n#setting($_ = $route('/other','POST'))" },
		func(s *Source) { s.Text += "\n#setting($_ = $limit(10))" },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "form/debug", "query/debug") },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "$Debug<bool>", "$Unknown<bool>") },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, "$Debug<bool>", "$Debug<string>") },
		func(s *Source) { s.Text = strings.ReplaceAll(s.Text, ".Optional()", ".Optional().Value('true')") },
	} {
		copy := *source
		change(&copy)
		_, err := g.Generate(ctx, GenerationRequest{Source: &copy, Destination: root})
		require.Error(t, err)
		require.Equal(t, before, snapshot(root))
	}
	// Failed fresh generation must not even create the destination directory.
	fresh := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, fresh)
	freshBefore := snapshot(fresh)
	bad := *source
	bad.HandlerBindings = nil
	_, err = g.Generate(ctx, GenerationRequest{Source: &bad, Destination: fresh})
	require.Error(t, err)
	require.Equal(t, freshBefore, snapshot(fresh))
	entries, err := os.ReadDir(fresh)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, entry.IsDir(), entry.Name())
	}
	// An edited owned file remains protected even under overwrite policy.
	routerPath := filepath.Join(root, "registration", "router.go")
	router, err := os.ReadFile(routerPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(routerPath, append(router, []byte("\n// handwritten edit\n")...), 0600))
	edited := snapshot(root)
	_, err = g.Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.Error(t, err)
	require.Equal(t, edited, snapshot(root))
}
