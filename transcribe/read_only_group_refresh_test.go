package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
)

func TestReadOnlyGroupRegeneration(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	const module = "github.com/viant/datly/grouprefresh"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	children, err := os.ReadFile("../standalone/testdata/app/readgroups/components.go")
	require.NoError(t, err)
	// Reuse the real nested-reader fixture, but generate the public parent from DQL.
	lines := strings.Split(string(children), "\n")
	for i, line := range lines {
		if strings.Contains(line, "Subject   xdatly.Component") {
			lines[i] = ""
		}
	}
	writeSourceFile(t, root, "children/components.go", strings.Join(lines, "\n"))
	text := `#import('children','` + module + `/children')
#package('` + module + `/subject')
#setting($_ = $route('/subject','GET'))
#setting($_ = $input_type('` + module + `/subject.SubjectInput'))
#setting($_ = $output_type('` + module + `/subject.SubjectOutput'))
#define($_ = $Case<string>(query/case).Optional())
#define($_ = $Left<*children.LeftOutput>(component/GET:/read-group/left).Optional()GROUP)
#define($_ = $Right<*children.RightOutput>(component/POST:/read-group/right).Optional()RIGHTGROUP)
#define($_ = $Data<[]*children.Row>(output/view))
SELECT rows.*,type(rows,'children.Row'),CAST(rows.id AS int),CAST(rows.name AS string),use_connector(rows,'main') FROM (SELECT id,name FROM records) rows`
	generate := func(group string) *GeneratedPackage {
		rightGroup := group
		if strings.Contains(group, "'Case'") {
			rightGroup += ".DependsOn('Left')"
		}
		current := strings.ReplaceAll(text, "RIGHTGROUP", rightGroup)
		writeSourceFile(t, root, "source/subject.dql", strings.ReplaceAll(current, "GROUP", group))
		project, e := (&Discovery{BaseDir: root, Include: []string{module + "/source"}, SourceFile: "subject.dql"}).Compile(ctx)
		require.NoError(t, e)
		require.Len(t, project.Components, 1)
		generated, e := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
		require.NoError(t, e)
		for _, name := range []string{"Left", "Right"} {
			field, ok := generated.Result.Plan.Input.Field(name)
			require.True(t, ok)
			if group == "" {
				for _, key := range []string{"resolutionGroup=", "resolutionMode=", "resolutionAfter=", "dependsOn="} {
					require.NotContains(t, field.Tag, key)
				}
			} else {
				require.Contains(t, field.Tag, "resolutionGroup=subject")
				require.Contains(t, field.Tag, "resolutionMode=dependencies")
				if strings.Contains(group, "'Case'") {
					require.Contains(t, field.Tag, "resolutionAfter=Case")
					if name == "Right" {
						require.Contains(t, field.Tag, "dependsOn=Left")
					}
				} else {
					require.NotContains(t, field.Tag, "resolutionAfter=")
					require.NotContains(t, field.Tag, "dependsOn=")
				}
			}
		}
		return generated
	}
	generate("")
	snapshot := func() map[string]string {
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(filepath.Join(root, "subject"), func(path string, entry os.DirEntry, e error) error {
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
	for _, group := range []string{".WithReadOnlyGroup('subject','Case')", ".WithReadOnlyGroup('subject')", "", ".WithReadOnlyGroup('subject')"} {
		generate(group)
		before := snapshot()
		generate(group)
		require.Equal(t, before, snapshot(), "repeat regeneration must be byte-identical")
	}
	writeSourceFile(t, root, "subject/refresh_test.go", readOnlyGroupRefreshRuntime)
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-race", "-count=1", "-timeout=90s", "./subject")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

const readOnlyGroupRefreshRuntime = `package subject
import (
 "context"
 "encoding/json"
 "errors"
 "net/http/httptest"
 "path/filepath"
 "reflect"
 "sync"
 "testing"
 "time"
 "github.com/stretchr/testify/require"
 children "github.com/viant/datly/grouprefresh/children"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
 "os"
)

func (o *SubjectOutput) Finalize(ctx context.Context, cause error) error {
 if cause != nil { return cause }
 i:=ctx.Value(reflect.TypeFor[*SubjectInput]()).(*SubjectInput)
 if i.Left==nil || i.Right==nil || !i.Left.Complete || !i.Right.Complete { return errors.New("parent observed incomplete children") }
 return nil
}

func TestRegeneratedGroupOverlaps(t *testing.T) {
 for _,mode:=range []string{"eager","indexed","linked"} { t.Run(mode,func(t *testing.T){
  ctx,cancel:=context.WithTimeout(t.Context(),20*time.Second);defer cancel()
  root,err:=filepath.Abs("..");require.NoError(t,err)
  dsn:=filepath.Join(t.TempDir(),"records.db")
  db:=sqlite.New(t,sqlite.WithDSN(dsn))
  require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE records(id INTEGER,name TEXT)","INSERT INTO records VALUES(1,'first')"))
  cfgFile:=filepath.Join(t.TempDir(),"config.json")
  body,err:=json.Marshal(map[string]any{
   "Endpoint":map[string]any{"Address":"127.0.0.1:0"},
   "GoBootstrap":map[string]any{"Packages":[]string{"github.com/viant/datly/grouprefresh/subject","github.com/viant/datly/grouprefresh/children"},"EagerComponents":mode=="eager","LinkedOnly":mode=="linked"},
   "Connector":"main","Connectors":[]any{map[string]any{"Name":"main","Driver":"sqlite3","DSN":dsn}},
  });require.NoError(t,err);require.NoError(t,os.WriteFile(cfgFile,body,0600))
  cfg,err:=(config.Loader{}).Load(ctx,cfgFile);require.NoError(t,err);cfg.BaseDir=root
  if mode=="linked" {cfg.BaseDir=filepath.Join(t.TempDir(),"no-source")}
  server,err:=standalone.New(ctx,standalone.Options{Config:cfg});require.NoError(t,err)
  defer server.Shutdown(context.Background())
  require.NoError(t,server.Reload(ctx,1))
  entered:=make(chan string,2);release:=make(chan struct{})
  var once sync.Once
  unblock:=func(){once.Do(func(){close(release)})};defer unblock()
  remove:=children.Install(mode,&children.Probe{Entered:entered,Release:release});defer remove()
  done:=make(chan *httptest.ResponseRecorder,1)
  go func(){r:=httptest.NewRecorder();server.ServeHTTP(r,httptest.NewRequest("GET","/subject?case="+mode,nil).WithContext(ctx));done<-r}()
  branches:=[]string{}
  for range 2 {select {
   case branch:=<-entered:branches=append(branches,branch)
   case r:=<-done:t.Fatalf("returned before overlap: %d %s",r.Code,r.Body.String())
   case <-ctx.Done():t.Fatal(ctx.Err())
  }}
  require.ElementsMatch(t,[]string{"left","right"},branches)
  unblock()
  select {case r:=<-done:
   require.Equal(t,200,r.Code,r.Body.String())
   var out struct{Data []children.Row}
   require.NoError(t,json.Unmarshal(r.Body.Bytes(),&out))
   require.Equal(t,[]children.Row{{ID:1,Name:"first"}},out.Data)
  case <-ctx.Done():t.Fatal(ctx.Err())}
 })}
}
`
