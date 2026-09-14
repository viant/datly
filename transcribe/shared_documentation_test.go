package transcribe

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratedDocumentationSourceRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(source, "docs/schemas"), 0700))
	global := `Columns:
  id: Embedded identifier
Paths:
  /documents: Global operation
Responses:
  /download:
    '201':
      description: Authored generic response
      content:
        application/json:
          schema: {$ref: 'schemas/node.json'}
          example: {id: 7}
`
	rule := `Paths:
  /documents: Embedded rule operation
  /download: Download authored data
`
	require.NoError(t, os.WriteFile(filepath.Join(source, "docs/global.yaml"), []byte(global), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "docs/rule.yaml"), []byte(rule), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "docs/schemas/node.json"), []byte(`{"type":"object","properties":{"id":{"type":"integer"},"next":{"$ref":"#"}}}`), 0600))
	generated, err := transcribeSource(context.Background(), root, &Source{Scope: "example.com/generated/documents", Name: "Documents", Path: filepath.Join(source, "query.sql"), Text: `#setting($_ = $route('/documents','GET'))
#setting($_ = $mcp('documents'))
#setting($_ = $DocURL('ignored-missing.yaml'))
#setting($_ = $DocGlobalURLs('docs/global.yaml'))
#setting($_ = $DocURLs('docs/rule.yaml'))
SELECT 1 AS id`})
	require.NoError(t, err)
	require.Len(t, generated.Result.Plan.Documentation.GlobalURLs, 1)
	require.Len(t, generated.Result.Plan.Documentation.DocURLs, 1)
	require.True(t, strings.Contains(generated.Result.Plan.Documentation.DocURLs[0], "datly_docs/default/docs/rule.yaml"))
	code := fmt.Sprintf(`package documentedtest
import(
 "context";"encoding/json";"reflect";"testing";"io/fs";"strings"
 afsembed "github.com/viant/afs/embed"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/application"
 "github.com/viant/datly/bootstrap"
 gateway "github.com/viant/datly/gateway/http"
 "github.com/viant/datly/gateway/openapi"
 "github.com/viant/datly/gateway/openapi/openapi3"
 "github.com/viant/datly/mcp"
 "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/tag"
 "github.com/viant/datly/typecatalog"
 "github.com/viant/xdatly/response"
 generated "example.com/generated/generated"
)
func TestEmbeddedPublication(t *testing.T){
 ctx:=context.Background();manager,err:=application.New(nil);if err!=nil{t.Fatal(err)};defer manager.Shutdown(ctx)
 var assets fs.FS=generated.DatlyResources
 compile:=func(ctx context.Context,types *typecatalog.Catalog)(*application.Build,error){
  store:=resource.New();if err:=store.Register(generated.DatlyResourceNamespace,assets);err!=nil{return nil,err}
  metadata,_,err:=tag.ParseComponent(reflect.TypeOf(generated.Component{}).Field(0).Tag);if err!=nil{return nil,err}
  component,err:=(&bootstrap.RouteSource{Tag:metadata,PackagePath:"example.com/generated/generated"}).Resolve(reflect.TypeFor[generated.%s](),reflect.TypeFor[generated.%s]());if err!=nil{return nil,err}
  artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[generated.%s](),OutputType:reflect.TypeFor[generated.%s](),Resources:store});if err!=nil{return nil,err}
  entry,err:=artifact.Registration(registry.RegisteredComponent{Handler:handler.HandlerFunc(func(context.Context,handler.Invocation)(any,error){panic("documentation executed handler")})});if err!=nil{return nil,err}
  generic:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Name:"Download"},Documentation:metadata.Documentation,Routes:[]*spec.Route{{Method:"GET",Path:"/download",MCP:[]*spec.MCPExposure{{Kind:spec.MCPExposureTool,Name:"download"}}}}}
  download,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:generic,InputType:reflect.TypeFor[struct{}](),OutputType:reflect.TypeFor[response.Response](),Resources:store});if err!=nil{return nil,err}
  downloadEntry,err:=download.Registration(registry.RegisteredComponent{Handler:handler.HandlerFunc(func(context.Context,handler.Invocation)(any,error){panic("documentation executed generic handler")})});if err!=nil{return nil,err}
  return &application.Build{Components:[]*registry.RegisteredComponent{entry,downloadEntry},Resources:store,HTTP:gateway.Config{OpenAPI:&gateway.OpenAPIConfig{Info:openapi3.Info{Title:"Embedded",Version:"1"}}}},nil
 }
 err=manager.Reload(ctx,application.Request{Revision:1,Compile:compile});if err!=nil{t.Fatal(err)}
 raw,err:=manager.ExportOpenAPI(ctx,openapi.ExportRequest{});if err!=nil{t.Fatal(err)};var doc openapi3.OpenAPI;if err=json.Unmarshal(raw,&doc);err!=nil{t.Fatal(err)}
 if doc.Paths["/documents"].Get.Description!="Embedded rule operation"{t.Fatal(string(raw))}
 if doc.Paths["/download"].Get.Responses["201"].Content["application/json"].Schema.Ref==""{t.Fatal("authored schema not published")}
 _,service,err:=manager.Pin(ctx);if err!=nil{t.Fatal(err)};tool,ok:=service.(*mcp.Service).Catalog().Tool("documents");if !ok||*tool.Metadata().Description!="Embedded rule operation"{t.Fatal("MCP rule metadata missing")};download,_:=service.(*mcp.Service).Catalog().Tool("download");if download.Metadata().Meta["datly/httpSchemas"]==nil{t.Fatal("MCP authored HTTP schema missing")}
 holder:=afsembed.NewHolder()
 err=fs.WalkDir(assets,".",func(name string,entry fs.DirEntry,err error)error{if err!=nil{return err};if entry.IsDir(){return nil};data,err:=fs.ReadFile(assets,name);if err!=nil{return err};holder.Add(name,strings.ReplaceAll(string(data),"Embedded rule operation","Binary reload operation"));return nil});if err!=nil{t.Fatal(err)}
 assets=holder.EmbedFs();if err=manager.Reload(ctx,application.Request{Revision:2,Compile:compile});err!=nil{t.Fatal(err)}
 raw,err=manager.ExportOpenAPI(ctx,openapi.ExportRequest{});if err!=nil{t.Fatal(err)};if err=json.Unmarshal(raw,&doc);err!=nil{t.Fatal(err)};if doc.Paths["/documents"].Get.Description!="Binary reload operation"{t.Fatal("binary documentation reload failed")}
 _,service,err=manager.Pin(ctx);if err!=nil{t.Fatal(err)};tool,_=service.(*mcp.Service).Catalog().Tool("documents");if *tool.Metadata().Description!="Binary reload operation"{t.Fatal("MCP binary documentation reload failed")}
}
`, generated.Result.Plan.Input.Type, generated.Result.Plan.Output.Type, generated.Result.Plan.Input.Type, generated.Result.Plan.Output.Type)
	require.NoError(t, os.WriteFile(filepath.Join(root, "documentation_test.go"), []byte(code), 0600))
	binary := filepath.Join(root, "embedded-documentation.test")
	build := exec.Command("go", "test", "-mod=mod", "-c", "-o", binary, ".")
	build.Dir = root
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	// Runtime metadata now has only binary resources: remove both authored input
	// and generated filesystem copies before starting the compiled test binary.
	require.NoError(t, os.RemoveAll(source))
	require.NoError(t, os.RemoveAll(filepath.Join(root, "generated/datly_docs")))
	require.NoError(t, os.RemoveAll(filepath.Join(root, "generated/datly_sql")))
	execute := exec.Command(binary, "-test.run", "TestEmbeddedPublication", "-test.v")
	execute.Dir = t.TempDir()
	output, err = execute.CombinedOutput()
	require.NoError(t, err, string(output))
}
func TestGeneratedDocumentationRejectsInvalidResource(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"missing", nil}, {"malformed", []byte("Columns: [")}, {"case-conflict", []byte("Columns: {ID: first, id: second}")},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			source := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			if test.data != nil {
				require.NoError(t, os.WriteFile(filepath.Join(source, "docs.yaml"), test.data, 0600))
			}
			_, err := transcribeSource(context.Background(), root, &Source{Scope: "example.com/generated", Name: "Documents", Path: filepath.Join(source, "query.sql"), Text: "#setting($_ = $route('/documents','GET'))\n#setting($_ = $DocURL('docs.yaml'))\nSELECT 1 AS id"})
			require.Error(t, err)
		})
	}
}
