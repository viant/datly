package transcribe

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	bs "github.com/viant/bindly/state"
	mi "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/mcp/tool"
	hc "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type formStringInput struct {
	Action   string `parameter:",kind=form,in=uploadAction,dataType=*string"`
	UploadId string `parameter:",kind=form,in=uploadId,dataType=*string"`
	Plain    string `parameter:",kind=form,in=plain"`
}

func TestExplicitFormStringWireRuntime(t *testing.T) {
	for _, withDefault := range []bool{false, true} {
		t.Run(fmt.Sprint(withDefault), func(t *testing.T) {
			p := &spec.Parameter{Name: "Action", TypeExpr: "*string", OutputTypeExpr: "string", Source: spec.BindSource{Kind: "form", Name: "uploadAction"}}
			if withDefault {
				v := "ui"
				p.Value = &v
			}
			component := &spec.Component{Parameters: []*spec.Parameter{p}, Routes: []*spec.Route{{Method: "POST", Path: "/upload"}}}
			compiled, err := hc.New(hc.Input{Component: component, InputType: reflect.TypeFor[formStringInput]()}).Compile()
			require.NoError(t, err)
			route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/upload"})
			require.True(t, ok)
			control := *component
			control.Parameters = []*spec.Parameter{p.Clone()}
			control.Parameters[0].TypeExpr = "string"
			control.Parameters[0].OutputTypeExpr = ""
			baseline, err := hc.New(hc.Input{Component: &control, InputType: reflect.TypeFor[formStringInput]()}).Compile()
			require.NoError(t, err)
			baselineRoute, ok := baseline.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/upload"})
			require.True(t, ok)
			plan, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "upload"}})
			require.NoError(t, err)
			for _, name := range []string{"Action", "UploadId"} {
				require.Equal(t, []string{"string", "null"}, plan.Metadata().InputSchema.Properties[name]["type"])
			}
			require.Equal(t, "string", plan.Metadata().InputSchema.Properties["Plain"]["type"])
			for _, field := range route.Fields() {
				require.Equal(t, reflect.TypeFor[string](), field.SourceType())
				require.Equal(t, reflect.TypeFor[string](), field.DestinationType())
				require.Nil(t, field.Binding().Transformer)
				require.True(t, field.Binding().Required == nil || !*field.Binding().Required)
			}
			for _, test := range []struct {
				name, value string
				present     bool
			}{{"missing", "", false}, {"empty", "", true}, {"value", "pws", true}} {
				t.Run("multipart/"+test.name, func(t *testing.T) {
					var body bytes.Buffer
					writer := multipart.NewWriter(&body)
					if test.present {
						require.NoError(t, writer.WriteField("uploadAction", test.value))
						require.NoError(t, writer.WriteField("uploadId", test.value))
					}
					require.NoError(t, writer.Close())
					req := httptest.NewRequest("POST", "/upload", &body)
					req.Header.Set("Content-Type", writer.FormDataContentType())
					scope, err := requestprovider.New(req)
					require.NoError(t, err)
					defer scope.Close()
					injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
					require.NoError(t, err)
					var got formStringInput
					require.NoError(t, injector.Bind(t.Context(), &got, bindly.WithPlan(route.Plan())))
					var original formStringInput
					require.NoError(t, injector.Bind(t.Context(), &original, bindly.WithPlan(baselineRoute.Plan())))
					require.Equal(t, original, got)
					want := test.value
					if !test.present && withDefault {
						want = "ui"
					}
					require.Equal(t, want, got.Action)
					require.Equal(t, test.value, got.UploadId)
				})
			}
			for _, test := range []struct {
				name string
				args mi.Arguments
				want string
			}{{"omitted", mi.Arguments{}, ""}, {"null", mi.Arguments{"Action": nil, "UploadId": nil}, ""}, {"empty", mi.Arguments{"Action": "", "UploadId": ""}, ""}, {"value", mi.Arguments{"Action": "pws", "UploadId": "pws"}, "pws"}} {
				t.Run("mcp/"+test.name, func(t *testing.T) {
					adapter, err := mi.NewCompiler().Compile([]mi.Argument{{PublicName: "Action", Source: bs.Location{Kind: "form", In: "uploadAction"}, SourceType: reflect.TypeFor[string]()}, {PublicName: "UploadId", Source: bs.Location{Kind: "form", In: "uploadId"}, SourceType: reflect.TypeFor[string]()}})
					require.NoError(t, err)
					scope, err := adapter.Scope(test.args)
					require.NoError(t, err)
					injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
					require.NoError(t, err)
					var got formStringInput
					require.NoError(t, injector.Bind(t.Context(), &got, bindly.WithPlan(route.Plan())))
					var original formStringInput
					require.NoError(t, injector.Bind(t.Context(), &original, bindly.WithPlan(baselineRoute.Plan())))
					require.Equal(t, original, got)
					want := test.want
					if test.name == "omitted" && withDefault {
						want = "ui"
					}
					require.Equal(t, want, got.Action)
					require.Equal(t, test.want, got.UploadId)
				})
			}
		})
	}
}

func TestLinkedExplicitFormString(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	for _, persisted := range []string{"string", "*string"} {
		p := &spec.Parameter{Name: "Action", TypeExpr: "*string", OutputTypeExpr: "string", Source: spec.BindSource{Kind: "form", Name: "uploadAction"}}
		actual := &spec.Parameter{Name: "Action", TypeExpr: persisted, Source: p.Source}
		err := validateHandlerParameters(&HandlerBinding{input: reflect.TypeFor[formStringInput]()}, []*spec.Parameter{p}, []*spec.Parameter{actual}, resolver)
		if persisted == "*string" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "type conflicts")
		}
	}
}

func TestSourceExplicitFormStringFreshRuntime(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	code := strings.Replace(string(data), "Tenant    int      `parameter:\"TenantID,kind=query,in=tenant_id\"`", "Action string `parameter:\",kind=form,in=uploadAction,dataType=*string\"`\n UploadId string `parameter:\",kind=form,in=uploadId,dataType=*string\"`", 1)
	require.NotEqual(t, string(data), code)
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text = strings.Replace(source.Text, "$TenantID<int>(query/tenant_id)", "$Action<*string,string>(form/uploadAction)", 1) + "\n#define($_ = $UploadId<*string,string>(form/uploadId).Optional())"
	var previous map[string]string
	for i := 0; i < 2; i++ {
		compiled, err := NewCompiler().Compile(t.Context(), source)
		require.NoError(t, err)
		generated, err := (Generator{Operation: "handler"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		snapshot := map[string]string{}
		for _, file := range generated.Result.Files {
			snapshot[file.Path] = file.Content
		}
		if i != 0 {
			require.Equal(t, previous, snapshot)
		}
		previous = snapshot
	}
	main := fmt.Sprintf(`package main
import("fmt";"reflect";_ %q;"github.com/viant/datly/bootstrap";compiler "github.com/viant/datly/runtime/handler/compiler";"github.com/viant/datly/mcp/tool";"github.com/viant/datly/spec";"github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{%q,%q});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile();if err!=nil{panic(err)}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/convert"})
 plan,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:component.Routes[0].MCP[0]});if err!=nil{panic(err)}
 for _,name:=range []string{"Action","UploadId"}{
 found:=false;for _,p:=range component.Parameters{if p.Name==name{found=true;if p.TypeExpr!="*string" || p.OutputTypeExpr!="string"{panic("persisted split lost")}}};if !found{panic("parameter lost")}
 if !reflect.DeepEqual(plan.Metadata().InputSchema.Properties[name]["type"],[]string{"string","null"}){panic("wire schema lost")}
 found=false;for _,f:=range route.Fields(){if f.Binding().Path==name{found=true;if f.SourceType()!=reflect.TypeFor[string]() || f.DestinationType()!=reflect.TypeFor[string](){panic("business type changed")}}};if !found{panic("binding lost")}
 }
 fmt.Print("retained")
}`, handlerFixtureModule+"/registration", handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "main.go", main)
	cmd := exec.CommandContext(context.Background(), "go", "run", "-mod=readonly", "-ldflags=-w", ".")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
}

func TestExplicitEmbeddedFormStringWire(t *testing.T) {
	type UploadForm struct {
		Action string `parameter:",kind=form,in=uploadAction,dataType=*string"`
	}
	type Input struct{ UploadForm }
	component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/upload"}}}
	compiled, err := hc.New(hc.Input{Component: component, InputType: reflect.TypeFor[Input]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/upload"})
	require.True(t, ok)
	plan, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "upload"}})
	require.NoError(t, err)
	require.Equal(t, []string{"string", "null"}, plan.Metadata().InputSchema.Properties["Action"]["type"])
	require.Len(t, route.Fields(), 1)
	require.Equal(t, reflect.TypeFor[string](), route.Fields()[0].SourceType())
	require.Equal(t, reflect.TypeFor[string](), route.Fields()[0].DestinationType())
}
