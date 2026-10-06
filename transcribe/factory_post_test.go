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
)

func generatedPostFactoryFixture(t *testing.T) (string, *Source) {
	t.Helper()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	writeSourceHandlerFile(t, root, "archive/business.go", `package archive
import (
 "context"
 "os"
 "github.com/viant/xdatly/handler"
)
type ArchiveRequest struct { Ids []string `+"`json:\"ids\"`"+` }
type ResultItem struct { Id string `+"`json:\"id\"`"+`; Status string `+"`json:\"status\"`"+`; Error string `+"`json:\"error,omitempty\"`"+` }
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput, ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(_ context.Context, _ handler.Session, in *ArchiveInput, out *ArchiveOutput) error {
 out.Status = "ok"
 if in.Data != nil { for _, id := range in.Data.Ids { out.Results = append(out.Results, &ResultItem{Id:id,Status:"archived"}) } }
 return nil
}
func init() { if os.Getenv("DATLY_HANDLER_GENERATING") == "1" { panic("generation executed application init") } }
`)
	text := fmt.Sprintf(`#package(%q)
#import('archive',%q)
#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $route('/archive','POST'))
#setting($_ = $input_type('ArchiveInput'))
#setting($_ = $output_type('ArchiveOutput'))
#setting($_ = $case_format('lc'))
#define($_ = $Data<*archive.ArchiveRequest>(body/data).Optional())
#define($_ = $Status<string>(output/status))
#define($_ = $Results<[]*archive.ResultItem>(output/body).WithTag('json:"results"'))`, handlerFixtureModule+"/archive", handlerFixtureModule+"/archive")
	source := &Source{Scope: handlerFixtureModule + "/dql", Name: "archive", Path: root, Text: text, GoBuild: &gobuild.Context{Dir: root, Env: []string{"DATLY_HANDLER_GENERATING=1"}}}
	return root, source
}

func TestGeneratedPostFactoryFirstGenerationAndStableRegeneration(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	// The application factory references contracts that do not yet exist.
	_, err := os.Stat(filepath.Join(root, "archive/input.go"))
	require.True(t, os.IsNotExist(err))
	db := &forbiddenHandlerDB{}
	discovery := Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: column.New(db)}
	var first map[string]string
	for range 2 {
		compiled, err := discovery.CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		plan := generated.Result.Plan
		require.Equal(t, generate.ContractGenerated, plan.Input.Ownership)
		require.Equal(t, generate.ContractGenerated, plan.Output.Ownership)
		require.Equal(t, "ArchiveInput", plan.Input.Type)
		require.Equal(t, "ArchiveOutput", plan.Output.Type)
		require.Equal(t, "NewArchive", plan.FactoryExpression)
		require.Empty(t, plan.Views)
		require.Nil(t, plan.MutationHandler)
		require.Empty(t, compiled.Component.Settings.Mutation)
		snapshot := sourceHandlerSnapshot(t, root)
		if first == nil {
			first = snapshot
		} else {
			require.Equal(t, first, snapshot)
		}
	}
	require.Zero(t, db.calls)
	writeSourceHandlerFile(t, root, "archive/runtime_test.go", generatedPostFactoryRuntimeTest)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-count=1", "-timeout=2m", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestGeneratedPostFactoryFailuresDoNotPublish(t *testing.T) {
	for _, method := range []string{"POST", "PATCH"} {
		t.Run(method, func(t *testing.T) { assertGeneratedFactoryFailuresDoNotPublish(t, method) })
	}
}

func assertGeneratedFactoryFailuresDoNotPublish(t *testing.T, method string) {
	for _, tc := range []struct{ name, old, replacement, message string }{
		{"function variable", "func NewArchive() handler.Contract[ArchiveInput, ArchiveOutput] { return &Handler{} }", "var NewArchive = func() handler.Contract[ArchiveInput, ArchiveOutput] { return &Handler{} }", "declared function"},
		{"argument", "func NewArchive()", "func NewArchive(unused int)", "handler source build"},
		{"wrong result", "func NewArchive() handler.Contract[ArchiveInput, ArchiveOutput] { return &Handler{} }", "func NewArchive() *Handler { return &Handler{} }", "handler source build"},
		{"wrong input", "func NewArchive() handler.Contract[ArchiveInput, ArchiveOutput] { return &Handler{} }", "func NewArchive() handler.Contract[ArchiveRequest, ArchiveOutput] { return nil }", "handler source build"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := generatedFactoryTransportFixture(t, method)
			file := filepath.Join(root, "archive/business.go")
			code, err := os.ReadFile(file)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(file, []byte(strings.Replace(string(code), tc.old, tc.replacement, 1)), 0600))
			before := sourceHandlerSnapshot(t, root)
			compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
			if err == nil {
				_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
			}
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
	for _, operation := range []string{"get", "put", "patch", "handler"} {
		t.Run(operation, func(t *testing.T) {
			root, source := generatedFactoryTransportFixture(t, method)
			compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
			require.NoError(t, err)
			before := sourceHandlerSnapshot(t, root)
			_, err = (Generator{Operation: operation}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
			require.Error(t, err)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
	t.Run("parent SQL", func(t *testing.T) {
		root, source := generatedFactoryTransportFixture(t, method)
		source.Text += "\nSELECT 1"
		before := sourceHandlerSnapshot(t, root)
		_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
		require.ErrorContains(t, err, "cannot contain SQL")
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	})
}

const generatedPostFactoryRuntimeTest = `package archive
import (
 "context"
 "io"
 "os"
 "path/filepath"
 "net/http/httptest"
 "strings"
 "testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
)
func TestGeneratedPostFactoryRuntime(t *testing.T) {
 cwd, err := os.Getwd(); require.NoError(t,err)
 for _, eager := range []bool{false,true} {
  ctx,cancel := context.WithCancel(context.Background())
  server,err := standalone.New(ctx,standalone.Options{
   Config:&config.Config{BaseDir:filepath.Dir(cwd),GoBootstrap:&config.Packages{Packages:[]string{"github.com/viant/datly/handlerfixture/archive"},EagerComponents:eager},Endpoint:config.Endpoint{Address:"127.0.0.1:0"}},
   Holders:[]any{ArchiveDatly},
  })
  require.NoError(t,err)
  done:=make(chan error,1); go func(){done<-server.Serve(ctx,io.Discard)}()
  _,err=server.WaitReady(ctx);require.NoError(t,err)
  for _, tc := range []struct{body,want string}{
   {"{}","{\"status\":\"ok\",\"results\":null}"},
   {"{\"data\":null}","{\"status\":\"ok\",\"results\":null}"},
   {"{\"data\":{\"ids\":[]}}","{\"status\":\"ok\",\"results\":null}"},
   {"{\"data\":{\"ids\":[\"7\",\"7\",\"8\"]}}","{\"status\":\"ok\",\"results\":[{\"id\":\"7\",\"status\":\"archived\"},{\"id\":\"7\",\"status\":\"archived\"},{\"id\":\"8\",\"status\":\"archived\"}]}"},
  } {
   req:=httptest.NewRequest("POST","/archive",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json")
   res:=httptest.NewRecorder();server.ServeHTTP(res,req)
   require.Equal(t,200,res.Code,res.Body.String())
   require.JSONEq(t,tc.want,res.Body.String())
  }
  cancel()
  require.NoError(t,server.Shutdown(context.Background()))
  require.NoError(t,<-done)
 }
}
`

func TestGeneratedPostFactoryQualifiedAuthoredContractsRemainSourceBacked(t *testing.T) {
	for _, sameDestination := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_destination_%v", sameDestination), func(t *testing.T) {
			root, source := sourceHandlerFixture(t)
			destination := handlerFixtureModule + "/registration"
			if sameDestination {
				destination = handlerFixtureModule + "/business"
			}
			source.Text = fmt.Sprintf(`#package(%q)
#import('business',%q)
#setting($_ = $handler_factory('business.NewConvert','Convert'))
#setting($_ = $route('/convert','POST'))
#setting($_ = $input_type('business.Alias'))
#setting($_ = $output_type('business.Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Debug<bool>(form/debug).Optional())
#define($_ = $TenantID<int>(query/tenant_id).Optional())`, destination, handlerFixtureModule+"/business")
			before := sourceHandlerSnapshot(t, root)
			compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
			if sameDestination {
				// The existing source-backed path rejects authored contracts in its
				// registration destination. Generated ownership must not bypass that gate.
				require.ErrorContains(t, err, "must be separate from destination")
				require.Equal(t, before, sourceHandlerSnapshot(t, root))
				return
			}
			require.NoError(t, err)
			require.False(t, compiled.ExternalHandler.GeneratedContracts)
			result, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, err)
			require.Equal(t, generate.ContractLinked, result.Result.Plan.Input.Ownership)
			require.Equal(t, generate.ContractLinked, result.Result.Plan.Output.Ownership)
			after := sourceHandlerSnapshot(t, root)
			require.Equal(t, before[filepath.Join(root, "business/contracts.go")], after[filepath.Join(root, "business/contracts.go")])
		})
	}
}
