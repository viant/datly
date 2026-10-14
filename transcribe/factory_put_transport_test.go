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
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/program"
)

func TestGeneratedFactoryPutTransport(t *testing.T) {
	for _, qualified := range []bool{false, true} {
		t.Run(fmt.Sprintf("qualified_%v", qualified), func(t *testing.T) {
			root, source := generatedPostFactoryFixture(t)
			source.Text = strings.Replace(source.Text, "'/archive','POST'", "'/archive','PUT'", 1)
			source.Text += "\n#define($_ = $Mode<string>(body/mode).Optional())"
			businessFile := filepath.Join(root, "archive/business.go")
			business, err := os.ReadFile(businessFile)
			require.NoError(t, err)
			business = []byte(strings.Replace(string(business), `out.Status = "ok"`, `out.Status = "ok"; if in.Mode != "" { out.Status = in.Mode }`, 1))
			require.NoError(t, os.WriteFile(businessFile, business, 0600))
			if !qualified {
				source.Text = strings.Replace(source.Text, "'archive.NewArchive'", "'NewArchive'", 1)
			}
			before := sourceHandlerSnapshot(t, root)
			db := &forbiddenHandlerDB{}
			discovery := Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: column.New(db)}
			var first map[string]string
			for round := range 2 {
				compiled, err := discovery.CompileSource(context.Background(), source)
				require.NoError(t, err)
				require.Nil(t, compiled.Component.RootView)
				require.Empty(t, compiled.Component.Views)
				require.Empty(t, compiled.Component.Settings.Mutation)
				require.Equal(t, "PUT", compiled.Component.Routes[0].Method)
				require.True(t, compiled.ExternalHandler.GeneratedContracts)
				request := GenerationRequest{Compiled: compiled, Destination: root}
				if round == 1 {
					request = GenerationRequest{Source: compiled.Source, Destination: root}
				}
				generated, err := (Generator{Operation: "post"}).Generate(context.Background(), request)
				require.NoError(t, err)
				require.Nil(t, generated.Result.Plan.MutationHandler)
				require.Empty(t, generated.Result.Plan.Views)
				current := sourceHandlerSnapshot(t, root)
				require.Equal(t, before[filepath.Join(root, "archive/business.go")], current[filepath.Join(root, "archive/business.go")])
				if first == nil {
					first = current
				} else {
					require.Equal(t, first, current)
				}
				for _, op := range []string{"get", "put", "patch", "handler"} {
					_, err = (Generator{Operation: op}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
					require.Error(t, err)
					require.Equal(t, current, sourceHandlerSnapshot(t, root))
				}
			}
			require.Zero(t, db.calls)
			runtimeSource := strings.ReplaceAll(generatedPostFactoryRuntimeTest, `httptest.NewRequest("POST"`, `httptest.NewRequest("PUT"`)
			runtimeSource = strings.Replace(runtimeSource, "  } {", `   {"{\"mode\":\"selected\",\"data\":{\"ids\":[\"scalar\"]}}","{\"status\":\"selected\",\"results\":[{\"id\":\"scalar\",\"status\":\"archived\"}]}"},
  } {`, 1)

			runtimeSource = strings.Replace(runtimeSource, ` "testing"`, ` "testing"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"`, 1)
			runtimeSource = strings.Replace(runtimeSource, "  cancel()", `
  typed, invokeErr := server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/archive",Name:"Archive"},Route:spec.RouteRef{Method:"PUT",Path:"/archive"}},Input:&ArchiveInput{Mode:"typed-selected",Data:&ArchiveRequest{Ids:[]string{"typed"}}}})
  require.NoError(t,invokeErr)
  require.Equal(t,"typed",typed.(*ArchiveOutput).Results[0].Id)
  require.Equal(t,"typed-selected",typed.(*ArchiveOutput).Status)
  cancel()`, 1)
			writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeSource)
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestGeneratedFactoryPutSQLBody(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	writeSourceHandlerFile(t, root, "archive/business.go", `package archive
import("context";"github.com/viant/xdatly/handler")
type Handler struct{}
var observed *ArchiveInput
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(_ context.Context,_ handler.Session,in *ArchiveInput,out *ArchiveOutput) error { observed=in;out.Result=in.Data;return nil }
`)
	source.Text = `#package('github.com/viant/datly/handlerfixture/archive')
#import('archive','github.com/viant/datly/handlerfixture/archive')
#setting($_ = $route('/archive','PUT'))
#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $input_type('archive.ArchiveInput'))
#setting($_ = $output_type('archive.ArchiveOutput'))
#setting($_ = $connector('fixture'))
#define($_ = $Data<*archive.Record>(body/data).Optional())
#define($_ = $Result<*archive.Record>(output/body))
SELECT r.*,type(r,'archive.Record') FROM (RECORDS) r`
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "records.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY,NAME TEXT NOT NULL)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"fixture": db})
	for round := 0; round < 2; round++ {
		if round == 1 {
			_, err = db.Exec("ALTER TABLE RECORDS ADD COLUMN DESCRIPTION TEXT")
			require.NoError(t, err)
		}
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Views)
		require.True(t, compiled.ExternalHandler.GeneratedContracts)
		require.NotNil(t, compiled.ExternalHandler.InputShape)
		require.Equal(t, "PUT", compiled.Component.Routes[0].Method)
		generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Nil(t, generated.Result.Plan.MutationHandler)
		if round == 1 {
			found := false
			entries, err := os.ReadDir(filepath.Join(root, "archive"))
			require.NoError(t, err)
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
					raw, err := os.ReadFile(filepath.Join(root, "archive", entry.Name()))
					require.NoError(t, err)
					if strings.Contains(string(raw), "DESCRIPTION") {
						found = true
					}
				}
			}
			require.True(t, found, "regeneration must derive added column from SQL")
		}
	}
	writeSourceHandlerFile(t, root, "archive/scalar_runtime_test.go", generatedFactoryScalarRuntimeTest)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)

}

func TestGeneratedFactoryTransportEligibility(t *testing.T) {
	const destination = "example.com/app/records"
	prepared := &dql.PreparedSource{TypeContext: &spec.TypeContext{PackagePath: destination, Imports: []spec.ImportSpec{{Alias: "rows", Package: destination}}}}
	for _, method := range []string{"POST", "PATCH", "PUT", "put", "DELETE", "delete", "GET", "HEAD", "OPTIONS"} {
		header := &dql.HandlerHeader{Declarative: true, Method: method, Factory: "rows.NewSave", InputType: "rows.Input", OutputType: "rows.Output"}
		_, ok := program.FactoryContracts(header, prepared)
		require.Equal(t, method == "POST" || method == "PATCH" || strings.EqualFold(method, "PUT") || strings.EqualFold(method, "DELETE"), ok, method)
	}
	for _, ref := range []string{"example.com/foreign.Input", "*rows.Input", "[]rows.Input", "rows.Input[string]", "rows.input", "unknown.Input"} {
		header := &dql.HandlerHeader{Declarative: true, Method: "PUT", Factory: "rows.NewSave", InputType: ref, OutputType: "rows.Output"}
		_, ok := program.FactoryContracts(header, prepared)
		require.False(t, ok, "foreign or malformed ownership %s", ref)
	}
}

const generatedFactoryScalarRuntimeTest = `package archive
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
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
)
func TestScalarFactoryRuntime(t *testing.T) {
 cwd,err:=os.Getwd();require.NoError(t,err)
 ctx,cancel:=context.WithCancel(t.Context());defer cancel()
 server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:filepath.Dir(cwd),GoBootstrap:&config.Packages{Packages:[]string{"github.com/viant/datly/handlerfixture/archive"}},Endpoint:config.Endpoint{Address:"127.0.0.1:0"}},Holders:[]any{ArchiveDatly}})
 require.NoError(t,err)
 done:=make(chan error,1);go func(){done<-server.Serve(ctx,io.Discard)}()
 _,err=server.WaitReady(ctx);require.NoError(t,err)
 for _,tc:=range []struct{name,body string;nilBody,present bool}{
  {"absent","{}",true,false},{"null","{\"data\":null}",true,true},{"object","{\"data\":{\"ID\":0,\"NAME\":\"\"}}",false,true},
 } {
  t.Run(tc.name,func(t *testing.T){
   req:=httptest.NewRequest("PUT","/archive",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json")
   rec:=httptest.NewRecorder();server.ServeHTTP(rec,req);require.Equal(t,200,rec.Code,rec.Body.String())
   require.Equal(t,tc.nilBody,observed.Data==nil)
   if tc.present { require.NotNil(t,observed.Has);require.True(t,observed.Has.Data) } else { require.Nil(t,observed.Has) }
   if !tc.nilBody { require.IsType(t,(*Record)(nil),observed.Data);require.NotNil(t,observed.Data.Has) }
  })
 }
 typed:=&Record{}
 result,err:=server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/archive",Name:"Archive"},Route:spec.RouteRef{Method:"PUT",Path:"/archive"}},Input:&ArchiveInput{Data:typed,Has:&ArchiveInputHas{Data:true}}})
 require.NoError(t,err);require.Same(t,typed,result.(*ArchiveOutput).Result)
 cancel();require.NoError(t,server.Shutdown(context.Background()));require.NoError(t,<-done)
}
`

func TestFactoryScalarBodyOwnershipGuards(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"value", "Record"}, {"value slice", "[]Record"}, {"double pointer", "**Record"}, {"foreign", "*foreign.Record"},
		{"alternate output", "*Record,*Record"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			source.Text = strings.Replace(source.Text, "'/archive','PATCH'", "'/archive','PUT'", 1)
			source.Text = strings.Replace(source.Text, "$InpData<[]*Res>", "$InpData<"+strings.ReplaceAll(tc.body, "Record", "Res")+">", 1)
			before := sourceHandlerSnapshot(t, root)
			db := &forbiddenHandlerDB{}
			source.ColumnRefiner = column.New(db)
			_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
			require.Error(t, err)
			require.Zero(t, db.calls)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestFactoryScalarBodyCardinalityConflict(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	source.Text = strings.Replace(source.Text, "$InpData<[]*Res>(body/data).Optional()", "$InpData<*Res>(body/data).Optional().Cardinality('Many')", 1)
	before := sourceHandlerSnapshot(t, root)
	db := &forbiddenHandlerDB{}
	source.ColumnRefiner = column.New(db)
	_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
	require.ErrorContains(t, err, "cardinality conflicts")
	require.Zero(t, db.calls)
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
}
