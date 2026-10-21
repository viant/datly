package transcribe

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	bs "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	mi "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/mcp/tool"
	hc "github.com/viant/datly/runtime/handler/compiler"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type queryWireInput struct {
	Source string `parameter:",kind=query,in=source,dataType=*string" value:"ui"`
}

func TestExplicitQueryWireRuntime(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	actual := &spec.Parameter{Name: "Source", TypeExpr: "*string", Source: spec.BindSource{Kind: "query", Name: "source"}}
	authored := &spec.Parameter{Name: "Source", Source: actual.Source}
	authored.TypeExpr = "*string"
	require.NoError(t, validateHandlerParameters(&HandlerBinding{input: reflect.TypeFor[queryWireInput]()}, []*spec.Parameter{authored}, []*spec.Parameter{actual}, resolver))
	defaultValue := "ui"
	actual.Value = &defaultValue
	component := &spec.Component{Parameters: []*spec.Parameter{actual}, Routes: []*spec.Route{{Method: "GET", Path: "/source"}}}
	compiled, err := hc.New(hc.Input{Component: component, InputType: reflect.TypeFor[queryWireInput]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/source"})
	require.True(t, ok)
	for _, field := range route.Fields() {
		require.Equal(t, reflect.TypeFor[string](), field.SourceType())
		require.Equal(t, reflect.TypeFor[string](), field.DestinationType())
	}
	plan, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "source"}})
	require.NoError(t, err)
	require.Equal(t, []string{"string", "null"}, plan.Metadata().InputSchema.Properties["Source"]["type"])
	for _, tc := range []struct {
		name string
		args mi.Arguments
		want string
	}{{"omitted", mi.Arguments{}, "ui"}, {"null", mi.Arguments{"Source": nil}, "ui"}, {"empty", mi.Arguments{"Source": ""}, "ui"}, {"value", mi.Arguments{"Source": "pws"}, "pws"}} {
		t.Run(tc.name, func(t *testing.T) {
			adapter, err := mi.NewCompiler().Compile([]mi.Argument{{PublicName: "Source", Source: bs.Location{Kind: "query", In: "source"}, SourceType: reflect.TypeFor[string]()}})
			require.NoError(t, err)
			scope, err := adapter.Scope(tc.args)
			require.NoError(t, err)
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			require.NoError(t, err)
			var got queryWireInput
			require.NoError(t, injector.Bind(context.Background(), &got, bindly.WithPlan(route.Plan())))
			require.Equal(t, tc.want, got.Source)
		})
	}
	for _, ignore := range []bool{false, true} {
		for _, tc := range []struct {
			values url.Values
			want   string
		}{{url.Values{}, "ui"}, {url.Values{"source": {""}}, "ui"}, {url.Values{"source": {"pws"}}, "pws"}} {
			providers := requestprovider.NewValues(requestprovider.WithQuery(tc.values))
			injector, err := bindly.NewInjector(bindly.WithProviders(providers.Providers()...))
			require.NoError(t, err)
			var got queryWireInput
			ctx := requestprovider.WithQueryPolicy(context.Background(), requestprovider.QueryPolicy{IgnoreEmptyParameters: ignore})
			require.NoError(t, injector.Bind(ctx, &got, bindly.WithPlan(route.Plan())))
			want := tc.want
			if !ignore && len(tc.values["source"]) != 0 && tc.values.Get("source") == "" {
				want = ""
			}
			require.Equal(t, want, got.Source)
		}
	}
}

func TestSourceExplicitQueryWireFreshRuntime(t *testing.T) {
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	code := strings.Replace(string(data), "Tenant    int      `parameter:\"TenantID,kind=query,in=tenant_id\"`", "Source string `parameter:\",kind=query,in=source,dataType=*string\" value:\"ui\"`", 1)
	require.NotEqual(t, string(data), code)
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text = strings.Replace(source.Text, "$TenantID<int>(query/tenant_id)", "$Source<*string,string>(query/source)", 1)
	compiled, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		compiled, err = NewCompiler().Compile(context.Background(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
	}
	main := fmt.Sprintf(`package main
import ("fmt"; "reflect"; _ %q; "github.com/viant/datly/bootstrap"; compiler "github.com/viant/datly/runtime/handler/compiler"; "github.com/viant/datly/mcp/tool"; "github.com/viant/datly/spec"; "github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{%q,%q});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile();if err!=nil{panic(err)}
 for _,p:=range component.Parameters {if p.Name=="Source" && p.OutputTypeExpr!="string" {panic("destination lost on reload")}}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/convert"});found:=false
 for _,field:=range route.Fields(){if field.Binding().Path!="Source"{continue};found=true;if field.SourceType()!=reflect.TypeFor[string]() || field.DestinationType()!=reflect.TypeFor[string](){panic("runtime source changed")}}
 if !found{panic("Source lost")}
 plan,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:component.Routes[0].MCP[0]});if err!=nil{panic(err)}
 if !reflect.DeepEqual(plan.Metadata().InputSchema.Properties["Source"]["type"],[]string{"string","null"}){panic(fmt.Sprintf("wire metadata lost: %%#v",plan.Metadata().InputSchema))}
 fmt.Print("retained")
}`, handlerFixtureModule+"/registration", handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
	writeSourceHandlerFile(t, root, "main.go", main)
	cmd := exec.Command("go", "run", "-mod=readonly", ".")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
}

func TestBackedQueryStringSplit(t *testing.T) {
	for _, tc := range []struct {
		name, expr, output, persisted, kind string
		required, destination, want         bool
	}{
		{"backed", "*string", "string", "*string", "query", false, true, true},
		{"single", "*string", "", "*string", "query", false, true, true},
		{"unbacked", "*string", "string", "string", "query", false, true, false},
		{"scalar", "string", "", "string", "query", false, true, false},
		{"required", "*string", "string", "*string", "query", true, true, true},
		{"body", "*string", "string", "*string", "body", false, true, false},
		{"int", "*int", "int", "*int", "query", false, false, false},
		{"bad-output", "*string", "int", "*string", "query", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &spec.Parameter{TypeExpr: tc.expr, OutputTypeExpr: tc.output, Source: spec.BindSource{Kind: tc.kind}, Required: &tc.required}
			b := &spec.Parameter{TypeExpr: tc.persisted, Source: a.Source, Required: &tc.required}
			require.Equal(t, tc.want, backedScalarWireSplit(a, b, tc.destination && tc.expr == tc.persisted && tc.expr != "string" && tc.output != "int"))
		})
	}
}

func TestNativeExplicitQueryWireFreshRuntime(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/querywire"}).Write(t, root)
	source := &Source{Name: "Records", Scope: "example.com/querywire", Path: root, Text: `#package('generated')
#setting($_ = $route('/records','POST'))
#define($_ = $Body<*RecordsView>(body/).Optional())
#define($_ = $Source<*string,string>(query/source).Optional().Value('ui'))
SELECT r.id,CAST(r.id AS 'int'),tag(r.id,'sqlx:"id,primaryKey"') FROM records r`}
	generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	var found bool
	for _, f := range generated.Result.Plan.Input.Fields {
		if f.Name == "Source" {
			found = true
			require.Equal(t, "string", f.Type)
			require.Contains(t, f.Tag, "dataType=*string")
		}
	}
	require.True(t, found)
	main := `package main
import("fmt";"reflect";"strings";_ "example.com/querywire/generated";"github.com/viant/datly/bootstrap";compiler "github.com/viant/datly/runtime/handler/compiler";"github.com/viant/datly/transcribe/generate";"github.com/viant/datly/mcp/tool";"github.com/viant/datly/spec";"github.com/viant/datly/typecatalog")
func main(){
 packages,err:=bootstrap.ReflectPackages([]string{"example.com/querywire/generated"});if err!=nil{panic(err)}
 holder:=packages.Components[0];component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType);if err!=nil{panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil);if err!=nil{panic(err)}
 for _,p:=range component.Parameters{if p.Name=="Source" && (p.TypeExpr!="*string" || p.OutputTypeExpr!="string"){panic(fmt.Sprintf("split lost: %+v",p))}}
 component.Settings.InputType="RecordsInput";component.Settings.OutputType="RecordsOutput"
 plan,err:=generate.New(generate.Input{Component:component,TargetPackage:"example.com/querywire/generated",TypeResolver:resolver}).Plan();if err!=nil{panic(err)}
 found:=false;for _,f:=range plan.Input.Fields{if f.Name=="Source"{found=true;if f.Type!="string"{panic("regeneration changed destination")};if !strings.Contains(f.Tag,"dataType=*string"){panic("regeneration lost dataType")}}};if !found{panic("source missing")}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile();if err!=nil{panic(err)}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/records"})
 for _,f:=range route.Fields(){if f.Binding().Path=="Source" && (f.SourceType()!=reflect.TypeFor[string]() || f.DestinationType()!=reflect.TypeFor[string]()){panic("binding changed")}}
 discovery,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:&spec.MCPExposure{Kind:spec.MCPExposureTool,Name:"records"}});if err!=nil{panic(err)}
 if !reflect.DeepEqual(discovery.Metadata().InputSchema.Properties["Source"]["type"],[]string{"string","null"}){panic(fmt.Sprintf("schema lost: %+v",discovery.Metadata().InputSchema))}
 fmt.Print("retained")
}`
	writeSourceHandlerFile(t, root, "main.go", main)
	cmd := exec.Command("go", "run", "-mod=readonly", ".")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "retained", strings.TrimSpace(string(out)))
	second, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	for _, f := range second.Result.Plan.Input.Fields {
		if f.Name == "Source" {
			require.Equal(t, "string", f.Type)
			require.Contains(t, f.Tag, "dataType=*string")
		}
	}

}

func TestExplicitQueryWireNoDefault(t *testing.T) {
	type input struct {
		Source string `parameter:",kind=query,in=source,dataType=*string"`
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/source"}}}
	compiled, err := hc.New(hc.Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/source"})
	require.True(t, ok)
	discovery, err := tool.NewCompiler().Compile(tool.Input{Contract: route, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "source"}})
	require.NoError(t, err)
	require.Equal(t, []string{"string", "null"}, discovery.Metadata().InputSchema.Properties["Source"]["type"])
	for _, tc := range []struct {
		args mi.Arguments
		want string
	}{{mi.Arguments{}, ""}, {mi.Arguments{"Source": nil}, ""}, {mi.Arguments{"Source": ""}, ""}, {mi.Arguments{"Source": "pws"}, "pws"}} {
		adapter, err := mi.NewCompiler().Compile([]mi.Argument{{PublicName: "Source", Source: bs.Location{Kind: "query", In: "source"}, SourceType: reflect.TypeFor[string]()}})
		require.NoError(t, err)
		scope, err := adapter.Scope(tc.args)
		require.NoError(t, err)
		injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
		require.NoError(t, err)
		var got input
		require.NoError(t, injector.Bind(t.Context(), &got, bindly.WithPlan(route.Plan())))
		require.Equal(t, tc.want, got.Source)
	}
}

func TestLinkedBackedQueryString(t *testing.T) {
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	for _, backed := range []bool{false, true} {
		p := &spec.Parameter{Name: "Source", TypeExpr: "*string", OutputTypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "source"}}
		actual := &spec.Parameter{Name: "Source", Source: p.Source}
		if backed {
			actual.TypeExpr = "*string"
		}
		err := validateHandlerParameters(&HandlerBinding{input: reflect.TypeFor[queryWireInput]()}, []*spec.Parameter{p}, []*spec.Parameter{actual}, resolver)
		if backed {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "type conflicts")
		}
	}
}
