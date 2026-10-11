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
	"github.com/viant/datly/mcp/tool/testdata/customjson"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func wireSourceHandlerFixture(t *testing.T) (string, *Source) {
	t.Helper()
	root, source := sourceHandlerFixture(t)
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	code := strings.Replace(string(data), `parameter:",kind=body,in=request"`, `parameter:",kind=body,in=request,required=true,errorCode=418" json:"request" wireType:"`+handlerFixtureModule+`/business.RequestWire"`, 1)
	code = strings.Replace(code, "type Request struct {", "type Request struct {\n\tLookup json.RawMessage `json:\"lookup\"`", 1)
	code += "\ntype RequestWire Request\ntype WireAlias = RequestWire\ntype OtherWire struct { Value string }\nfunc (*Request) UnmarshalJSON([]byte) error { panic(\"discovery invoked custom decoder\") }\n"
	writeSourceHandlerFile(t, root, "business/contracts.go", code)
	source.Text += "\n#import('business', '" + handlerFixtureModule + "/business')"
	return root, source
}

func TestSourceHandlerWireTypeCompilerGenerationFreshRuntime(t *testing.T) {
	for _, declaration := range []string{"", `.Required()`, `.Required().WithTag('wireType:"business.RequestWire"')`, `.Required().WithTag('wireType:"business.WireAlias"')`} {
		t.Run(declaration, func(t *testing.T) {
			root, source := wireSourceHandlerFixture(t)
			if declaration != "" {
				source.Text += "\n#set($_ = $Request<*business.Request>(body/request)" + declaration + ")"
			}
			compiled, err := NewCompiler().Compile(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, []string{handlerFixtureModule + "/business.RequestWire"}, compiled.ExternalHandler.WireTypes)
			// The owner-only carrier must not create declarations or override binding metadata.
			expected := 2
			if declaration != "" {
				expected++
			}
			require.Len(t, compiled.Component.Parameters, expected)
			clone := compiled.ExternalHandler.Clone()
			clone.WireTypes[0] = "changed"
			require.Equal(t, handlerFixtureModule+"/business.RequestWire", compiled.ExternalHandler.WireTypes[0])
			generated, err := (Generator{Operation: "handler"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, err)
			require.Equal(t, generate.ContractLinked, generated.Result.Plan.Input.Ownership)
			require.Len(t, generated.Result.Plan.WireTypes, 1)
			main := fmt.Sprintf(`package main
import ("fmt"; "reflect"; _ %q; "github.com/viant/datly/bootstrap"; compiler "github.com/viant/datly/runtime/handler/compiler"; "github.com/viant/datly/mcp/tool"; "github.com/viant/datly/spec"; "github.com/viant/datly/typecatalog")
func main() {
 packages,err:=bootstrap.ReflectPackages([]string{%q,%q}); if err!=nil {panic(err)}
 holder:=packages.Components[0]
 component,err:=holder.Resolve(holder.LinkedInputType,holder.LinkedOutputType); if err!=nil {panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil); if err!=nil {panic(err)}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:holder.LinkedInputType,TypeLookup:resolver.Type}).Compile(); if err!=nil {panic(err)}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/convert"})
 found:=false
 for _, field:=range route.Fields() { if field.WireType()==nil {continue}; found=true
  if field.WireType().Name()!="RequestWire" || field.SourceType()!=field.DestinationType() || field.SourceType().Kind()!=reflect.Pointer || field.SourceType().Elem().Name()!="Request" {panic("source identity changed")}
  if field.Binding().Required==nil || !*field.Binding().Required || field.Binding().ErrorCode!=418 {panic("owner metadata changed")}
 }
 if !found {panic("representation lost")}
 plan,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:component.Routes[0].MCP[0]}); if err!=nil {panic(err)}
 properties:=plan.Metadata().InputSchema.Properties["request"]["properties"].(map[string]interface{})
 if len(properties["lookup"].(map[string]interface{}))!=0 {panic("RawMessage changed")}
 fmt.Println("retained")
}`, handlerFixtureModule+"/registration", handlerFixtureModule+"/registration", handlerFixtureModule+"/business")
			writeSourceHandlerFile(t, root, "main.go", main)
			cmd := exec.Command("go", "run", "-mod=readonly", ".")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			require.Equal(t, "retained", strings.TrimSpace(string(out)))
		})
	}
}

func TestSourceHandlerWireTypeRetainsOverrideRejections(t *testing.T) {
	root, source := wireSourceHandlerFixture(t)
	for _, tc := range []struct{ declaration, message string }{
		{`.Required().WithTag('wireType:"business.OtherWire"')`, "wireType conflicts"},
		{`.Required().WithTag('wireType:"business.Absent"')`, "wireType conflicts"},
		{`.Required().WithTag('wireType:"*business.RequestWire"')`, "wireType conflicts"},
		{`.Optional().WithTag('wireType:"business.RequestWire"')`, "requiredness conflicts"},
		{`.Required().WithTag('wireType:"business.RequestWire" json:"different"')`, "tags conflict"},
		{`.Required().Value('changed').WithTag('wireType:"business.RequestWire"')`, "unsupported contract overrides"},
	} {
		t.Run(tc.declaration, func(t *testing.T) {
			copy := *source
			copy.Text += "\n#set($_ = $Request<*business.Request>(body/request)" + tc.declaration + ")"
			before := sourceHandlerSnapshot(t, root)
			_, err := NewCompiler().Compile(context.Background(), &copy)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
	data, err := os.ReadFile(filepath.Join(root, "business/contracts.go"))
	require.NoError(t, err)
	for _, value := range []string{"", handlerFixtureModule + "/business.Absent"} {
		code := strings.Replace(string(data), `wireType:"`+handlerFixtureModule+`/business.RequestWire"`, `wireType:"`+value+`"`, 1)
		writeSourceHandlerFile(t, root, "business/contracts.go", code)
		copy := *source
		copy.Text += "\n#set($_ = $Request<*business.Request>(body/request).Required().WithTag('wireType:\"business.RequestWire\"'))"
		_, err := NewCompiler().Compile(context.Background(), &copy)
		require.ErrorContains(t, err, "wireType")
	}
}

func TestLinkedHandlerWireTypePreservesOverrideValidation(t *testing.T) {
	const pkg = "github.com/viant/datly/mcp/tool/testdata/customjson"
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[customjson.RequestWire]())))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[customjson.OtherWire]())))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "body", Package: pkg}}})
	require.NoError(t, err)
	binding := &HandlerBinding{input: reflect.TypeFor[customjson.Input]()}
	actual := &spec.Parameter{Name: "Body", Source: spec.BindSource{Kind: "body"}, WireTypeExpr: pkg + ".RequestWire"}
	value := "changed"
	for _, tc := range []struct {
		name    string
		change  func(*spec.Parameter)
		message string
	}{
		{"matching", func(p *spec.Parameter) {}, ""},
		{"conflict", func(p *spec.Parameter) { p.WireTypeExpr = "body.OtherWire" }, "wireType conflicts"},
		{"pointer", func(p *spec.Parameter) { p.WireTypeExpr = "*body.RequestWire" }, "wireType conflicts"},
		{"ordinary tag", func(p *spec.Parameter) { p.Tag += ` json:"request"` }, "tags conflict"},
		{"codec", func(p *spec.Parameter) { p.Codec = &spec.Codec{Body: "identity"} }, "unsupported contract overrides"},
		{"default", func(p *spec.Parameter) { p.Value = &value }, "unsupported contract overrides"},
		{"predicate", func(p *spec.Parameter) { p.Predicates = []*spec.Predicate{{Name: "eq"}} }, "unsupported contract overrides"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := &spec.Parameter{Name: "Body", Source: spec.BindSource{Kind: "body"}, WireTypeExpr: "body.RequestWire", Tag: `wireType:"body.RequestWire"`}
			tc.change(param)
			err := validateHandlerParameters(binding, []*spec.Parameter{param}, []*spec.Parameter{actual}, resolver)
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
	// Source-only catalog descriptors do not prove compiled runtime identity.
	synthetic := typecatalog.NewCatalog()
	require.NoError(t, synthetic.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: pkg, Name: "RequestWire"}))
	resolver, err = typecatalog.NewResolver(synthetic, typecatalog.TranscribeAuthority, nil)
	require.NoError(t, err)
	param := actual.Clone()
	require.ErrorContains(t, validateHandlerParameters(binding, []*spec.Parameter{param}, []*spec.Parameter{actual}, resolver), "wireType conflicts")
	ownerMissing := actual.Clone()
	ownerMissing.WireTypeExpr = ""
	require.ErrorContains(t, validateHandlerParameters(binding, []*spec.Parameter{param}, []*spec.Parameter{ownerMissing}, resolver), "wireType conflicts")
}
