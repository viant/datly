package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/outputcontract"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const declaredOutputContractDQL = `#package('generated')
#import('wire','github.com/viant/datly/transcribe/testdata/outputcontract')
#setting($_ = $output_type('wire.Output'))
#setting($_ = $route('/records','GET'))
#define($_ = $Id<int>(query/Id))
#define($_ = $Data<*wire.Row>(output/view))
SELECT r.*,type(r,'wire.Row') FROM (SELECT id FROM records WHERE id=$Id) r
`

func TestTranscribeLinksQualifiedOutputWithoutComponentHolder(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputcontract.Output]()), x.NewType(reflect.TypeFor[outputcontract.Row]()), x.NewType(reflect.TypeFor[outputcontract.MissingBinding]()), x.NewType(reflect.TypeFor[outputcontract.WrongBinding]())))
	generated, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: declaredOutputContractDQL}, Generation: GenerationOptions{Operation: "get"}})
	require.NoError(t, err)
	require.Equal(t, gen.ContractLinked, generated.Result.Plan.Output.Ownership)
	require.Equal(t, "wire.Output", generated.Result.Plan.Output.Type)
	for _, file := range generated.Result.Files {
		require.NotContains(t, file.Content, "type Output struct")
		require.NotContains(t, file.Content, "type RecordsOutput struct")
	}
}

func TestDeclaredOutputContractRejectsMissingAndMismatchedFields(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"unresolved output remains generated", strings.Replace(declaredOutputContractDQL, "wire.Output", "wire.Missing", 1), ""},
		{"missing field", strings.Replace(declaredOutputContractDQL, "$Data<", "$Missing<", 1), "field Missing"},
		{"missing binding", strings.Replace(declaredOutputContractDQL, "wire.Output", "wire.MissingBinding", 1), "incompatible binding metadata"},
		{"wrong binding", strings.Replace(declaredOutputContractDQL, "wire.Output", "wire.WrongBinding", 1), "incompatible binding metadata"},
		{"wrong cardinality", strings.Replace(declaredOutputContractDQL, "$Data<*wire.Row>", "$Data<[]*wire.Row>", 1), "differs from linked field"},
		{"bare output remains generated", strings.Replace(declaredOutputContractDQL, "wire.Output", "Output", 1), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputcontract.Output]()), x.NewType(reflect.TypeFor[outputcontract.Row]()), x.NewType(reflect.TypeFor[outputcontract.MissingBinding]()), x.NewType(reflect.TypeFor[outputcontract.WrongBinding]())))
			result, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/app/dql/records", Types: catalog, Text: tc.source})
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
			} else {
				require.NoError(t, err)
				require.Nil(t, result.Contracts.Output)
			}
		})
	}
}

func TestQualifiedOutputFinalizerNativeReader(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputcontract.Output]()), x.NewType(reflect.TypeFor[outputcontract.Row]()), x.NewType(reflect.TypeFor[outputcontract.MissingBinding]()), x.NewType(reflect.TypeFor[outputcontract.WrongBinding]())))
	_, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: declaredOutputContractDQL}, Generation: GenerationOptions{Operation: "get"}})
	require.NoError(t, err)
	const fixture = `package fixture_test
 import("context";"database/sql";"net/http/httptest";"path/filepath";"strings";"testing"
 _ "modernc.org/sqlite"
 _ "example.com/generated/generated"
 "github.com/viant/datly/bootstrap/connector"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config")
 func TestPreservedOutput(t *testing.T){for _,eager:=range []bool{false,true}{
 dsn:=filepath.Join(t.TempDir(),"data.db");db,err:=sql.Open("sqlite",dsn);if err!=nil{t.Fatal(err)}
 if _,err=db.Exec("CREATE TABLE records(id INTEGER); INSERT INTO records VALUES(7)");err!=nil{t.Fatal(err)};db.Close()
 root,err:=filepath.Abs(".");if err!=nil{t.Fatal(err)};ctx:=context.Background()
 server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:root,GoBootstrap:&config.Packages{Packages:[]string{"example.com/generated/generated"},EagerComponents:eager},Connector:"main",Connectors:[]connector.Config{{Name:"main",Driver:"sqlite",DSN:dsn}}}});if err!=nil{t.Fatal(err)};if err=server.Reload(ctx,1);err!=nil{t.Fatal(err)}
 for _,tc:=range []struct{id,body string}{{"7","{\"LEGACY_ID\":7}"},{"-1","null"}}{
 rec:=httptest.NewRecorder();server.ServeHTTP(rec,httptest.NewRequest("GET","/records?Id="+tc.id,nil));if rec.Code!=200||strings.TrimSpace(rec.Body.String())!=tc.body||rec.Header().Get("Content-Type")!="application/json"{t.Fatalf("eager=%v id=%s code=%d body=%s",eager,tc.id,rec.Code,rec.Body.String())}
 }
 if err=server.Shutdown(ctx);err!=nil{t.Fatal(err)}
 }}
 `
	require.NoError(t, os.WriteFile(filepath.Join(root, "runtime_test.go"), []byte(fixture), 0600))
	command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout=120s", ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native linked output finalizer: %v\n%s", err, output)
	}
}

func TestLinkedOutputSQLDestinationRetainsAuthoredURI(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[outputcontract.Output]()), x.NewType(reflect.TypeFor[outputcontract.Row]())))
	text := strings.Replace(declaredOutputContractDQL, "#setting($_ = $route", "#setting($_ = $sql_dest('r','renamed.sql'))\n#setting($_ = $route", 1)
	// The compiled root view is component-named; an exact root SQL override cannot move an authored URI.
	text = strings.Replace(text, "$sql_dest('r'", "$sql_dest('Records'", 1)
	_, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Scope: "example.com/generated/dql/records", Name: "Records", Types: catalog, Text: text}, Generation: GenerationOptions{Operation: "get"}})
	require.ErrorContains(t, err, "differs from uneditable URI")
	_, err = os.Stat(filepath.Join(root, "generated"))
	require.True(t, os.IsNotExist(err), "failed contract emitted files")
}
