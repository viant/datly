package standalone

import (
	"context"
	"encoding/json"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/project/build"
	"github.com/viant/datly/standalone/config"
)

func TestStandaloneReportsAutomaticCustomBuildSQLite(t *testing.T) {
	f, cfg, jwt := newStandaloneReportFixture(t)
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
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	// Dependency normalization is confined to this generated temporary module.
	env := append(os.Environ(), "GOFLAGS=-mod=mod")
	service := build.Service{}
	require.NoError(t, service.Init(ctx, build.InitRequest{Dir: f.Root}))
	link := `package datlylink
import _ "example.com/standalone/reporting/spend"
func init(){}
`
	formattedLink, err := format.Source([]byte(link))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.Root, "internal/datlylink/link.go"), formattedLink, 0600))
	result, err := service.Build(ctx, build.Request{Dir: f.Root, Packages: []string{"./spend"}, Env: env})
	require.NoError(t, err)
	require.Equal(t, 2, result.Components)
	// Exercise the user-selected default import with no generated registry.
	// The binary is built above; HTTP handlers can be tested without a TCP socket.
	code := `package datlylink
import (
 _ "github.com/mattn/go-sqlite3"
 "context"
 "encoding/json"
 "io"
 "net/http"
 "net/http/httptest"
 "os"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/scy/auth/jwt"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
 spend "example.com/standalone/reporting/spend"
)
func TestDiscoveredReports(t *testing.T) {
 ctx:=context.Background()
 cfg,err:=(config.Loader{}).Load(ctx,` + strconv.Quote(f.Config) + `);if err!=nil{t.Fatal(err)}
 input:=reflect.TypeFor[spend.AuthInput]()
 field,ok:=input.FieldByName("JWT")
 if !ok || field.Type!=reflect.TypeFor[*jwt.Claims]() || !strings.Contains(field.Tag.Get("parameter"),"required") || field.Tag.Get("codec")!="JwtClaim" {t.Fatalf("declared JWT contract changed: %+v",field)}
 server,err:=standalone.New(ctx,standalone.Options{Config:cfg,Holders:[]any{spend.Component{}}});if err!=nil{t.Fatal(err)}
 defer func(){if err:=server.Shutdown(ctx);err!=nil{t.Error(err)}}()
 if err=server.Reload(ctx,1);err!=nil{t.Fatal(err)}
 metadata,err:=server.Metadata(ctx);if err!=nil{t.Fatal(err)}
 if len(metadata.Components)!=6 {t.Fatalf("components: %d",len(metadata.Components))}
 baseURL:=""
 if os.Getenv("DATLY_JWT_TCP")=="1" {
  listener:=httptest.NewServer(server);defer listener.Close();baseURL=listener.URL
  t.Log("actual TCP HTTP requests enabled")
 }
 for _,route:=range []struct{name,method,suffix,body string;count int}{
  {"read","GET","","",3},
  {"cube","POST","/cube",` + strconv.Quote(spendCube) + `,3},
  {"compose","POST","/cube/compose",` + strconv.Quote(spendCompose) + `,2},
 } {
  credentials:=[]struct{name,key,token string;status int}{
   {"missing key","",` + strconv.Quote(jwt.Sign(t, "reader", time.Now().Add(time.Hour))) + `,403},
   {"wrong key","wrong",` + strconv.Quote(jwt.Sign(t, "reader", time.Now().Add(time.Hour))) + `,403},
   {"missing JWT","report-secret","",401},
   {"malformed JWT","report-secret","Bearer invalid",401},
   {"wrong signature","report-secret",` + strconv.Quote(testharness.NewJWT(t).Sign(t, "reader", time.Now().Add(time.Hour))) + `,401},
   {"expired JWT","report-secret",` + strconv.Quote(jwt.Sign(t, "reader", time.Now().Add(-time.Hour))) + `,401},
   {"valid JWT","report-secret",` + strconv.Quote(jwt.Sign(t, "reader", time.Now().Add(time.Hour))) + `,200},
   {"public","","",200},
  }
  for _,auth:=range credentials {
   t.Run(route.name+"/"+auth.name,func(t *testing.T){
    path:="/secure"+route.suffix;if auth.name=="public"{path="/spend"+route.suffix}
    req:=httptest.NewRequest(route.method,path,strings.NewReader(route.body))
    req.Header.Set("Content-Type","application/json")
    req.Header.Set("X-Tenant","acme");req.Header.Set("Cookie","channel=web")
    req.Header.Set("X-Report-Key",auth.key);req.Header.Set("Authorization",auth.token)
    var status int;var data []byte
    if baseURL=="" {
     response:=httptest.NewRecorder();server.ServeHTTP(response,req);status=response.Code;data=response.Body.Bytes()
    } else {
     request,err:=http.NewRequestWithContext(ctx,route.method,baseURL+path,strings.NewReader(route.body));if err!=nil{t.Fatal(err)}
     request.Header=req.Header
     response,err:=http.DefaultClient.Do(request);if err!=nil{t.Fatal(err)}
     defer response.Body.Close();status=response.StatusCode
     data,err=io.ReadAll(response.Body);if err!=nil{t.Fatal(err)}
    }
    if status!=auth.status{t.Fatalf("status %d, want %d: %s",status,auth.status,data)}
    if status!=200{return}
    var result struct{Rows []struct{AccountID int;Region string;TotalSpend float64};Data []struct{Customer int;Web,Store float64}}
    if err=json.Unmarshal(data,&result);err!=nil{t.Fatal(err)}
    if len(result.Rows)+len(result.Data)!=route.count{t.Fatal(string(data))}
    if route.name=="compose" {
     if result.Data[0].Customer!=1 || result.Data[0].Web!=1150 || result.Data[0].Store!=2000 || result.Data[1].Web!=70{t.Fatal(string(data))}
    } else {
     totals:=map[string]float64{}
     for _,row:=range result.Rows{if row.AccountID==1{totals[row.Region]=row.TotalSpend}}
     if totals["EU"]!=150 || totals["US"]!=1000{t.Fatal(string(data))}
    }
   })
  }
 }
}
`
	formatted, err := format.Source([]byte(code))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.Root, "internal/datlylink/report_test.go"), formatted, 0600))
	command := exec.CommandContext(ctx, "go", "test", "./internal/datlylink", "-run", "TestDiscoveredReports", "-count=1", "-v")
	command.Dir = f.Root
	command.Env = env
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	t.Logf("Built custom executable and verified user-linked package authority: %s", output)
}
