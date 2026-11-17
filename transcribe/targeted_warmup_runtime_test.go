package transcribe

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestTargetedWarmupGeneratedDiscoveryAndRegeneration(t *testing.T) {
	const module = "github.com/viant/datly/inlinewarmupfixture"
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)"))
	text := `#package('` + module + `/records')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#setting($_ = $cache_warmup_cases('recent','Period=lifetime','ExcludeDefault=Period'))
#define($_ = $Period<string>(query/period))
#define($_ = $Data<?>(output/view))
SELECT r.*,c.*,use_cache(c,'children'),cache_warmup(c,'parent_id','CaseRefs=recent','Limit=0')
FROM records r LEFT JOIN children c ON c.parent_id=r.id`
	discovery := &Discovery{BaseDir: root, Include: []string{module + "/source"}, SourceFile: "Records.dql", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	generate := func() {
		writeSourceFile(t, root, "source/Records.dql", text)
		project, err := discovery.Compile(t.Context())
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		component := project.Components[0].Component
		require.Equal(t, strings.Contains(text, "cache_warmup(c,"), component.RootView.Relations[0].View.Source.Bindings.HasInlineWarmups())
		_, err = (Generator{Operation: "get"}).Generate(t.Context(), GenerationRequest{Compiled: project.Components[0], Destination: root})
		require.NoError(t, err)
	}
	generate()
	writeSourceFile(t, root, "records/warmup_runtime_test.go", targetedWarmupGeneratedRuntime)
	reload := func(wantRoot, wantChild bool) {
		command := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-count=1", "./records")
		command.Dir = root
		expected := "0"
		if wantRoot {
			expected = "1"
		}
		child := "0"
		if wantChild {
			child = "1"
		}
		command.Env = append(os.Environ(), "GOWORK=off", "EXPECT_ROOT="+expected, "EXPECT_CHILD="+child)
		out, err := command.CombinedOutput()
		require.NoError(t, err, "rebuilt generated reader: %s", out)
	}
	reload(false, true)
	text = strings.Replace(text, "SELECT r.*", "SELECT r.*,use_cache(r,'shared'),cache_warmup(r,'','Limit=0')", 1)
	generate()
	reload(true, true)
	generate()
	reload(true, true)
	text = strings.Replace(text, ",cache_warmup(c,'parent_id','CaseRefs=recent','Limit=0')", "", 1)
	generate()
	reload(true, false)
}

const targetedWarmupGeneratedRuntime = `package records
import (
 "os"
 "path/filepath"
 "reflect"
 "testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 index "github.com/viant/datly/bootstrap/index"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
)
func TestGeneratedTargetedWarmups(t *testing.T){
 pkg:=reflect.TypeFor[RecordsComponent]().PkgPath()
 cwd,err:=os.Getwd();require.NoError(t,err)
 // Catalog/shape discovery runs before runtime materialization.
 snapshot,err:=(index.Builder{Config:index.Config{BaseDir:filepath.Dir(cwd),Include:[]string{pkg}}}).Build(t.Context());require.NoError(t,err)
 entry,_,_,ok:=snapshot.Route("GET","/records");require.True(t,ok);require.True(t,entry.Warmup)
 found,err:=bootstrap.ReflectSelectedPackages([]string{pkg},nil);require.NoError(t,err);require.Len(t,found.Components,1)
 component,err:=found.Components[0].Resolve(reflect.TypeFor[RecordsInput](),reflect.TypeFor[RecordsOutput]());require.NoError(t,err)
 resources:=resource.New();for ns,fs:=range bootstrap.LinkedResources(nil,pkg){require.NoError(t,resources.Register(ns,fs))}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[RecordsInput](),OutputType:reflect.TypeFor[RecordsOutput](),Resources:resources});require.NoError(t,err)
 execution,err:=artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL:&dsql.SQLComponent{},CacheSettings:map[string]*spec.CacheSettings{
  "shared":{Enabled:true,Location:t.TempDir(),TTL:"1m"},"children":{Enabled:true,Location:t.TempDir(),TTL:"1m"},
 }});require.NoError(t,err)
 targets:=execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
 want:=0;if os.Getenv("EXPECT_ROOT")=="1"{want++};if os.Getenv("EXPECT_CHILD")=="1"{want++};require.Len(t,targets,want)
 for _,target:=range targets{require.Equal(t,"inline",target.Origin);require.Empty(t,target.PolicyRef);require.NotNil(t,target.Settings.Limit);require.Zero(t,*target.Settings.Limit);require.Nil(t,target.Settings.MaxCases)}
 if os.Getenv("EXPECT_CHILD")=="1" {
 child:=targets[len(targets)-1];require.Equal(t,"c",child.View);require.Equal(t,"children",child.CacheName)
 require.Equal(t,[]string{"lifetime"},child.Settings.Cases[0].Set[0].Values);require.True(t,child.Settings.Cases[0].Set[0].ExcludeDefault)
 }
}
`
