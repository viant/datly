package transcribe

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFactoryPluralRoutesShareGeneratedContracts(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	source.Text = strings.Replace(source.Text, "#setting($_ = $route('/archive','PATCH'))", "#setting($_ = $route('/archive','PATCH'))\n#setting($_ = $route('/restore','PATCH'))\n#define($_ = $Uri<string>(http_request/uri))", 1)
	source.Text = strings.Replace(source.Text, "#setting", "#setting($_ = $api_key('X-Key','fixture-key'))\n#setting($_ = $request_body_mode('eager'))\n#setting", 1)
	business, err := os.ReadFile(filepath.Join(root, "archive/business.go"))
	require.NoError(t, err)
	writeSourceHandlerFile(t, root, "archive/business.go", strings.Replace(string(business), `out.Status="ok"`, `out.Status=in.Uri`, 1))
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	var stable map[string]string
	for round := 0; round < 2; round++ {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
		require.NoError(t, err)
		require.Len(t, compiled.Component.Routes, 2)
		for _, route := range compiled.Component.Routes {
			require.Equal(t, "eager", route.RequestBodyMode)
			require.Equal(t, "X-Key", route.APIKeyHeader)
			require.Equal(t, "fixture-key", route.APIKeyValue)
			require.True(t, route.Internal)
		}
		generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Len(t, generated.Result.Plan.Routes, 2)
		now := sourceHandlerSnapshot(t, root)
		if round == 0 {
			stable = now
		} else {
			require.Equal(t, stable, now)
		}
	}
	runtimeSource := strings.Replace(factoryInputShapeRuntime, `FieldByName("Contract")`, `FieldByName("Contract1")`, 1)
	runtimeSource = strings.Replace(runtimeSource, " component,err:=", ` for _,contractName:=range []string{"Contract1","Contract2"}{f,ok:=holder.FieldByName(contractName);require.True(t,ok);m,present,err:=dtag.ParseComponent(f.Tag);require.NoError(t,err);require.True(t,present);require.Equal(t,"eager",m.RequestBodyMode);require.Equal(t,"X-Key",m.APIKeyHeader);require.Equal(t,"fixture-key",m.APIKeyValue);require.True(t,m.Internal);input,ok:=f.Type.FieldByName("Input");require.True(t,ok);require.Equal(t,reflect.TypeFor[ArchiveInput](),input.Type);output,ok:=f.Type.FieldByName("Output");require.True(t,ok);require.Equal(t,reflect.TypeFor[ArchiveOutput](),output.Type)}
 component,err:=`, 1)
	runtimeSource = strings.Replace(runtimeSource, `require.Nil(t,observed.Has)`, `require.NotNil(t,observed.Has);require.False(t,observed.Has.InpData);require.True(t,observed.Has.Uri)`, 1)
	runtimeSource = strings.Replace(runtimeSource, "for _,eager:=range []bool{false,true}", `for _,path:=range []string{"/archive","/restore"}{for _,eager:=range []bool{false,true}`, 1)
	runtimeSource = strings.ReplaceAll(runtimeSource, `Path:"/archive"`, `Path:path`)
	runtimeSource = strings.ReplaceAll(runtimeSource, `"PATCH","/archive"`, `"PATCH",path`)
	runtimeSource = strings.Replace(runtimeSource, `require.Same(t,observedOutput,value.(*ArchiveOutput));require.Equal`, `require.Same(t,observedOutput,value.(*ArchiveOutput));require.Equal(t,path,value.(*ArchiveOutput).Status);require.Equal`, 1)
	runtimeSource = strings.Replace(runtimeSource, "\n }\n}\n", "\n }}\n}\n", 1)
	writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeSource)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestFactoryShapeRuntime")
	t.Logf("%s", output)
}
