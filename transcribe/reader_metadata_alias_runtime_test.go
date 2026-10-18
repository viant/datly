package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	gen "github.com/viant/datly/transcribe/generate"
)

// The first generation deliberately has no NoLimit policy. A later DQL edit
// must survive actual package compilation rather than only the in-memory plan.
func TestNativeReaderAliasDestinationSurvivesRebuiltRepeat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const pkg = "example.com/aliaspolicy/records"
	(testharness.GeneratedModule{Path: "example.com/aliaspolicy"}).Write(t, root)
	writeSourceFile(t, root, "records/record.go", "package records\n type Record struct { Id int `sqlx:\"ID\"` }\n type Config struct { Value string }; type ConfigAlias = Config\n")
	text := `#import('records','` + pkg + `')
#package('` + pkg + `')
#setting($_ = $route('/records','GET'))
#setting($_ = $file_prefix('records_'))
#setting($_ = $input_type('` + pkg + `.RecordsInput'))
#setting($_ = $output_type('` + pkg + `.RecordsOutput'))
#define($_ = $Config<*records.ConfigAlias>(body/config).Optional())
#define($_ = $ID<int>(query/id).Optional())
#define($_ = $Page<int>(query/page).QuerySelector('rows'))
#define($_ = $Limit<int>(query/limit).QuerySelector('rows'))
#define($_ = $Data<[]*` + pkg + `.Record>(output/view))
SELECT rows.*,type(rows,'` + pkg + `.Record'),CAST(rows.ID AS int) FROM (SELECT 1 AS ID) rows`
	writeSourceFile(t, root, "source/Records.dql", text)
	initial, err := (&Discovery{BaseDir: root, Include: []string{"example.com/aliaspolicy/source"}, SourceFile: "Records.dql"}).Compile(ctx)
	require.NoError(t, err)
	first, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: initial.Components[0], Destination: root})
	require.NoError(t, err)
	require.Equal(t, gen.ContractGenerated, first.Result.Plan.Input.Ownership)
	require.Equal(t, gen.ContractGenerated, first.Result.Plan.Output.Ownership)
	validator := `package records
import("encoding/json";"os";"path/filepath";"reflect";"testing";"github.com/viant/datly/bootstrap")
func TestCompiledOutputPolicy(t *testing.T){
 _,_=reflect.TypeFor[RecordsComponent]().MethodByName("DatlyQuerySelectors")
 pkg:=reflect.TypeFor[RecordsComponent]().PkgPath()
 found,err:=bootstrap.ReflectSelectedPackages([]string{pkg},nil);if err!=nil{t.Fatal(err)}
 if len(found.Components)!=1{t.Fatalf("components=%d",len(found.Components))}
 component,err:=found.Components[0].Resolve(reflect.TypeFor[RecordsInput](),reflect.TypeFor[RecordsOutput]());if err!=nil{t.Fatal(err)}
 got:=component.RootView!=nil&&component.RootView.Selector!=nil&&component.RootView.Selector.NoLimit
 want:=os.Getenv("EXPECT_NOLIMIT")=="1"
 if got!=want{t.Fatalf("compiled NoLimit=%v want=%v (root=%+v)",got,want,component.RootView)}
 shape:=map[string][]string{}
 for _,typ:=range []reflect.Type{reflect.TypeFor[RecordsInput](),reflect.TypeFor[RecordsOutput](),reflect.TypeFor[Record]()} {
  for i:=0;i<typ.NumField();i++{f:=typ.Field(i);shape[typ.Name()]=append(shape[typ.Name()],f.Name+":"+f.Type.String())}
  ptr:=reflect.PointerTo(typ);for i:=0;i<ptr.NumMethod();i++{m:=ptr.Method(i);shape[typ.Name()]=append(shape[typ.Name()],"method:"+m.Name+":"+m.Type.String())}
 }
 body,err:=json.Marshal(shape);if err!=nil{t.Fatal(err)}
 path:=filepath.Join(os.Getenv("FIXTURE_ROOT"),"public-shape.json")
 if !want{if err=os.WriteFile(path,body,0600);err!=nil{t.Fatal(err)}}else{before,err:=os.ReadFile(path);if err!=nil{t.Fatal(err)};if string(before)!=string(body){t.Fatalf("public Go shape changed: before=%s after=%s",before,body)}}
}
`
	writeSourceFile(t, root, "records/policy_reload_test.go", validator)
	reload := func(noLimit bool) {
		command := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-count=1", "./records")
		command.Dir = root
		expected := "0"
		if noLimit {
			expected = "1"
		}
		command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on", "EXPECT_NOLIMIT="+expected, "FIXTURE_ROOT="+root)
		out, e := command.CombinedOutput()
		require.NoError(t, e, "rebuilt native package: %s", out)
	}
	reload(false)
	text = strings.Replace(text, "SELECT rows.*", "SELECT rows.*,set_limit(rows,0)", 1)
	writeSourceFile(t, root, "source/Records.dql", text)
	generate := func() *GeneratedPackage {
		project, e := (&Discovery{BaseDir: root, Include: []string{"example.com/aliaspolicy/source"}, SourceFile: "Records.dql"}).Compile(ctx)
		require.NoError(t, e)
		require.Len(t, project.Components, 1)
		generated, e := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
		require.NoError(t, e)
		t.Logf("policy refresh ownership: Input=%s Output=%s", generated.Result.Plan.Input.Ownership, generated.Result.Plan.Output.Ownership)
		data, ok := generated.Result.Plan.Output.Field("Data")
		require.True(t, ok)
		require.Contains(t, data.Tag, "selectorNoLimit=true", "canonical planned policy must be correct before persistence")
		return generated
	}
	refreshed := generate()
	reload(true)
	require.Equal(t, gen.ContractLinked, refreshed.Result.Plan.Input.Ownership, "unchanged admitted business input must remain linked")
	require.NotEmpty(t, refreshed.Result.Plan.ReaderMetadata)
	snapshot := func() map[string]string {
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(filepath.Join(root, "records"), func(path string, entry os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if !entry.IsDir() {
				body, e := os.ReadFile(path)
				if e != nil {
					return e
				}
				files[path] = string(body)
			}
			return nil
		}))
		return files
	}
	before := snapshot()
	generate()
	reload(true)
	require.Equal(t, before, snapshot(), "rebuilt repeat must preserve native artifacts")
}
