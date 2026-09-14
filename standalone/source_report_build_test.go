package standalone

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/project/build"
	"github.com/viant/datly/standalone/config"
)

func TestStandaloneReportsAutomaticCustomBuildSQLite(t *testing.T) {
	s, f, _ := newStandaloneReport(t)
	cfg := *s.source.config
	cfg.Info = nil
	// A compiled fixture needs its own module path, rather than a nested path
	// also present in the required Datly module.
	const module = "example.com/standalone/reporting"
	(testharness.GeneratedModule{Path: module}).Write(t, f.Root)
	cfg.GoBootstrap = &config.Packages{Packages: []string{module + "/spend"}}
	configuration, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.Config, configuration, 0600))
	(testharness.GeneratedModule{}).WriteSums(t, f.Root)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// Keep this automatic-link proof focused on the source reader. The linked
	// auth fixture above separately proves required JWT inputs. Automatic
	// traversal of jwt.RegisteredClaims is a pre-existing linker limit.
	sourcePath := filepath.Join(f.Root, "spend/spend.go")
	authored, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	lines := strings.Split(string(authored), "\n")
	for i, line := range lines {
		if strings.Contains(line, `component:"Protected,`) {
			lines[i] = ""
		}
	}
	require.NoError(t, os.WriteFile(sourcePath, []byte(strings.Join(lines, "\n")), 0600))
	// Dependency normalization is confined to this generated temporary module.
	env := append(os.Environ(), "GOFLAGS=-mod=mod")
	service := build.Service{}
	require.NoError(t, service.Init(ctx, build.InitRequest{Dir: f.Root}))
	result, err := service.Build(ctx, build.Request{Dir: f.Root, Packages: []string{"./spend"}, Env: env})
	require.NoError(t, err)
	require.Equal(t, 1, result.Components)
	// Exercise the generated Registry/Workspace, with no manual type export list.
	// The binary is built above; HTTP handlers can be tested without a TCP socket.
	code := `package datlylink
import (
 _ "github.com/mattn/go-sqlite3"
 "context"
 "encoding/json"
 "net/http/httptest"
 "strings"
 "testing"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
)
func TestDiscoveredReports(t *testing.T){
 ctx:=context.Background()
 cfg,err:=(config.Loader{}).Load(ctx,` + strconv.Quote(f.Config) + `);if err!=nil{t.Fatal(err)}
 registry,err:=Registry();if err!=nil{t.Fatal(err)}
 server,err:=standalone.New(ctx,standalone.Options{Config:cfg,Registry:registry,Workspace:Workspace()});if err!=nil{t.Fatal(err)};defer server.Shutdown(ctx)
 if err=server.Reload(ctx,1);err!=nil{t.Fatal(err)}
 for _,test:=range []struct{path,body string;count int}{{"/spend/cube",` + strconv.Quote(spendCube) + `,3},{"/spend/cube/compose",` + strconv.Quote(spendCompose) + `,2}}{
  req:=httptest.NewRequest("POST",test.path,strings.NewReader(test.body));req.Header.Set("Content-Type","application/json")
  res:=httptest.NewRecorder();server.ServeHTTP(res,req);if res.Code!=200{t.Fatalf("%s: %d %s",test.path,res.Code,res.Body.String())}
  var result struct{Rows []struct{TotalSpend float64};Data []struct{Web float64}}
  if err=json.Unmarshal(res.Body.Bytes(),&result);err!=nil{t.Fatal(err)}
  if len(result.Rows)+len(result.Data)!=test.count{t.Fatal(res.Body.String())}
  if len(result.Data)>0 && result.Data[0].Web!=1150{t.Fatal(res.Body.String())}
 }
}
`
	require.NoError(t, os.WriteFile(filepath.Join(f.Root, "internal/datlylink/report_test.go"), []byte(code), 0600))
	command := exec.CommandContext(ctx, "go", "test", "./internal/datlylink", "-run", "TestDiscoveredReports", "-count=1")
	command.Dir = f.Root
	command.Env = env
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	t.Logf("Built custom executable and verified generated Registry/Workspace: %s", output)
}
