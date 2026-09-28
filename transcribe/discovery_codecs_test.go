package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/testdata/linkedcodec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const codecTestPackage = "github.com/viant/datly/transcribe/testdata/linkedcodec"

func codecDiscoveryWorkspace(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", "module github.com/viant/datly/transcribe/testdata\n\ngo 1.25.0\n")
	data, err := os.ReadFile("testdata/linkedcodec/codec.go")
	require.NoError(t, err)
	writeSourceFile(t, base, "linkedcodec/codec.go", string(data))
	return base
}

func TestDiscoveryLoadsOnlyReferencedCodecTypes(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(map[bool]string{false: "linked", true: "explicit"}[registered], func(t *testing.T) {
			base := codecDiscoveryWorkspace(t)
			writeSourceFile(t, base, "linkedcodec/component.go", "package linkedcodec\nimport xdatly \"github.com/viant/xdatly\"\ntype Holder struct { Read xdatly.Component[struct{},struct{}] `component:\"Invalid\"` }\n")
			writeSourceFile(t, base, "linkedcodec/.datly-gen.json", `{"version":5,"owner":"Other","resources":{"namespace":"unused","files":["missing.sql"]}}`)
			registry := x.NewRegistry()
			if registered {
				registry.Register(x.NewType(reflect.TypeFor[linkedcodec.Upper](), x.WithPkgPath(codecTestPackage), x.WithName("QueryList")))
			}
			catalog := typecatalog.NewCatalog()
			writeSourceFile(t, base, "reader/Records.dql", `#setting($_ = $route('/records','GET'))
#set($_ = $Values<[]string,[]int>(query/value).WithCodec('`+codecTestPackage+`.QueryList','strict'))
SELECT id FROM records`)
			project, err := (&Discovery{BaseDir: base, Include: []string{predicateTestModule + "/reader"}, Registry: registry, Types: catalog}).Compile(context.Background())
			require.NoError(t, err)
			require.Len(t, project.Components, 1)
			types := project.Components[0].Source.Types
			descriptor, found, err := types.Resolve(typecatalog.PackageAuthority, codecTestPackage+".QueryList")
			require.NoError(t, err)
			require.True(t, found)
			require.NotNil(t, descriptor.SynteticType.TypeSpec)
			want := reflect.TypeFor[linkedcodec.QueryList]()
			if registered {
				want = reflect.TypeFor[linkedcodec.Upper]()
			}
			require.Equal(t, want, descriptor.Type)
			_, found, err = types.Resolve(typecatalog.PackageAuthority, codecTestPackage+".Unrelated")
			require.NoError(t, err)
			require.False(t, found)
			_, found, err = catalog.Resolve(typecatalog.PackageAuthority, codecTestPackage+".QueryList")
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestDQLCodecAliasesSurviveGeneration(t *testing.T) {
	base := t.TempDir()
	const module = "example.com/codecalias"
	(testharness.GeneratedModule{Path: module}).Write(t, base)
	source := &Source{Scope: module + "/reader", Name: "Records", Text: `#import('transform', '` + codecTestPackage + `')
#setting($_ = $route('/records','GET'))
#set($_ = $Values<[]string,[]int>(query/value).WithCodec('transform.QueryList','strict').WithPredicate(0,'in','r','id'))
SELECT r.label, CAST(r.label AS string), tag(r.label, 'codec:"transform.Upper" sqlx:"label,type=string"') FROM records r
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}`}
	result, err := (&Discovery{BaseDir: base}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, codecTestPackage+".QueryList", result.Component.Parameters[0].Codec.Body)
	require.Equal(t, codecTestPackage+".Upper", result.Component.RootView.Columns[0].Codec.Body)
	input, dir, err := generationInput(base, "reader", result)
	require.NoError(t, err)
	input.SQLResources = true
	generated, err := NewCompiler().generateInputAt(context.Background(), base, dir, result, input)
	require.NoError(t, err)
	for _, tc := range []struct{ file, name string }{
		{generated.Result.Plan.Input.Destination, "QueryList"}, {generated.Result.Plan.ViewDest, "Upper"},
	} {
		content, err := os.ReadFile(filepath.Join(base, dir, tc.file))
		require.NoError(t, err)
		require.Contains(t, string(content), `codec:"`+codecTestPackage+"."+tc.name)
		require.NotContains(t, string(content), `codec:"transform.`)
	}
	fixture := fmt.Sprintf(`package codecexecution_test
import (
 "context"
 "database/sql"
 "net/http/httptest"
 "path/filepath"
 "testing"
 "github.com/stretchr/testify/require"
 _ "github.com/mattn/go-sqlite3"
 _ %q
 _ %q
 "github.com/viant/datly/bootstrap/connector"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
)
func TestGeneratedCodecExecution(t *testing.T) {
 root,err:=filepath.Abs("."); require.NoError(t,err)
 for _,eager:=range []bool{false,true} {t.Run(map[bool]string{false:"indexed",true:"eager"}[eager],func(t *testing.T){
  dsn:=filepath.Join(t.TempDir(),"records.db")
  db,err:=sql.Open("sqlite3",dsn); require.NoError(t,err)
  _,err=db.Exec("CREATE TABLE records(id INTEGER,label TEXT); INSERT INTO records VALUES(1,'one'),(2,'two'),(3,'three'),(4,'four')"); require.NoError(t,err); require.NoError(t,db.Close())
  ctx:=context.Background()
  server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:root, GoBootstrap:&config.Packages{Packages:[]string{%q},EagerComponents:eager},Connectors:[]connector.Config{{Name:"main",Driver:"sqlite3",DSN:dsn}},Connector:"main"}})
  require.NoError(t,err); defer server.Shutdown(ctx)
  require.NoError(t,server.Reload(ctx,1))
  recorder:=httptest.NewRecorder()
  server.ServeHTTP(recorder,httptest.NewRequest("GET","/records?value=1,2&value=3",nil))
  require.Equal(t,200,recorder.Code,recorder.Body.String())
  require.JSONEq(t,%q,recorder.Body.String())
 })}
}
`, generated.Result.Plan.ComponentPackage, codecTestPackage, generated.Result.Plan.ComponentPackage, `{"Status":"ok","Data":[{"Label":"ONE"},{"Label":"TWO"},{"Label":"THREE"}]}`)
	writeSourceFile(t, base, "codec_execution_test.go", fixture)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=120s", ".")
	command.Dir, command.Env = base, append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestCodecSourceOnlyAndFailureIsolation(t *testing.T) {
	base := codecDiscoveryWorkspace(t)
	writeSourceFile(t, base, "linkedcodec/sourceonly.go", "package linkedcodec\ntype SourceOnly struct{}\n")
	catalog := typecatalog.NewCatalog()
	result, err := (&Discovery{BaseDir: base, Types: catalog}).CompileSource(context.Background(), &Source{Scope: predicateTestModule + "/reader", Name: "Records", Text: `#setting($_ = $route('/records','GET'))
#set($_ = $Value<string>(query/value).WithCodec('` + codecTestPackage + `.SourceOnly'))
SELECT id FROM records`})
	require.NoError(t, err)
	_, err = bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: result.Component, Types: result.Source.Types, InputType: reflect.TypeFor[struct{ Value string }]()})
	require.ErrorContains(t, err, "not linked into this binary")
	_, found, err := catalog.Resolve(typecatalog.PackageAuthority, codecTestPackage+".SourceOnly")
	require.NoError(t, err)
	require.False(t, found)
	// Bad aliases cannot accidentally resolve by a package's basename.
	_, err = bootstrap.CanonicalCodecName("transform.Upper", &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "transform", Package: "example.com/a"}, {Alias: "transform", Package: "example.com/b"}}})
	if err == nil {
		t.Fatal("conflicting codec aliases were accepted")
	}
	require.True(t, strings.Contains(err.Error(), "alias") || strings.Contains(err.Error(), "import"), err.Error())
}

func TestCodecDependencyConflictingAuthority(t *testing.T) {
	base := codecDiscoveryWorkspace(t)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[linkedcodec.QueryList]())))
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeFor[linkedcodec.Upper](), x.WithPkgPath(codecTestPackage), x.WithName("QueryList")))
	_, err := (&Discovery{BaseDir: base, Types: catalog, Registry: registry}).CompileSource(context.Background(), &Source{Scope: predicateTestModule + "/reader", Name: "Records", Text: `#setting($_ = $route('/records','GET'))
#set($_ = $Values<[]string,[]int>(query/value).WithCodec('` + codecTestPackage + `.QueryList'))
SELECT id FROM records`})
	require.ErrorContains(t, err, "different compiled identity")
	typ, _, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, codecTestPackage+".QueryList")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[linkedcodec.QueryList](), typ)
}
