package generate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp/tool/testdata/customjson"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	"reflect"
)

func TestWireTypeExternalInputFreshRuntimeRetention(t *testing.T) {
	root := t.TempDir()
	const module = "example.com/wireowner"
	const native = module + "/native"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	inputExpr := "struct { Body *Request `parameter:\",kind=body\" wireType:\"" + native + ".RequestWire\" json:\"request\"` }"
	if err := os.MkdirAll(filepath.Join(root, "native"), 0755); err != nil {
		t.Fatal(err)
	}
	nativeSource := `package native
import ("context"; "encoding/json"; "github.com/viant/xdatly/handler")
type Request struct { Lookup json.RawMessage ` + "`json:\"lookup\"`" + ` }
func (*Request) UnmarshalJSON([]byte) error { panic("discovery called decoder") }
type RequestWire Request
type Input ` + inputExpr + `
type Output struct { Value string }
type Handler struct{}
func NewHandler() handler.Contract[Input,Output] { return &Handler{} }
func (*Handler) Exec(context.Context,handler.Session,*Input,*Output) error { return nil }
`
	if err := os.WriteFile(filepath.Join(root, "native/types.go"), []byte(nativeSource), 0644); err != nil {
		t.Fatal(err)
	}
	loaded, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "native")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, loaded); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{DefaultPackage: native})
	if err != nil {
		t.Fatal(err)
	}
	input, err := resolver.Descriptor(native + ".Input")
	if err != nil {
		t.Fatal(err)
	}
	output, err := resolver.Descriptor(native + ".Output")
	if err != nil {
		t.Fatal(err)
	}
	component, err := (bootstrap.ContractResolver{Component: &spec.Component{Name: "Requests", Routes: []*spec.Route{{Method: "POST", Path: "/requests", Handler: native + ".NewHandler", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "requests"}}}}, Settings: &spec.Settings{InputType: native + ".Input", OutputType: native + ".Output"}}, InputType: input, OutputType: output, Types: resolver}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if component.Parameters[0].WireTypeExpr != native+".RequestWire" {
		t.Fatalf("source annotation lost: %+v", component.Parameters)
	}
	generator := New(Input{Component: component, TypeResolver: resolver, TargetPackage: module + "/registration", PackageName: "registration", Contracts: ContractReferences{Input: &ContractReference{Expression: "native.Input", DescriptorKey: native + ".Input"}, Output: &ContractReference{Expression: "native.Output", DescriptorKey: native + ".Output"}}, ExternalHandler: &ExternalHandler{Package: native, Name: "NewHandler"}})
	if _, err = generator.Generate(filepath.Join(root, "registration")); err != nil {
		t.Fatal(err)
	}
	mainSource := `package main
import ("fmt"; "reflect"; _ "example.com/wireowner/registration"; "github.com/viant/datly/bootstrap"; compiler "github.com/viant/datly/runtime/handler/compiler"; "github.com/viant/datly/mcp/tool"; "github.com/viant/datly/spec"; "github.com/viant/datly/typecatalog")
func main() {
 packages,err:=bootstrap.ReflectPackages([]string{"example.com/wireowner/registration","example.com/wireowner/native"}); if err!=nil {panic(err)}
 if len(packages.Components)!=1 {panic("missing generated holder")}
 source:=packages.Components[0]
 component,err:=source.Resolve(source.LinkedInputType,source.LinkedOutputType); if err!=nil {panic(err)}
 resolver,err:=typecatalog.NewResolver(packages.Types,typecatalog.PackageAuthority,nil); if err!=nil {panic(err)}
 compiled,err:=compiler.New(compiler.Input{Component:component,InputType:source.LinkedInputType,TypeLookup:resolver.Type}).Compile(); if err!=nil {panic(err)}
 route,_:=compiled.Input.ForRoute(spec.RouteRef{Method:"POST",Path:"/requests"})
 field:=route.Fields()[0]
 if field.WireType().PkgPath()!="example.com/wireowner/native" || field.WireType().Name()!="RequestWire" || field.SourceType()!=field.DestinationType() || field.SourceType().Kind()!=reflect.Pointer || field.SourceType().Elem().Name()!="Request" {panic("identity changed")}
 plan,err:=tool.NewCompiler().Compile(tool.Input{Contract:route,Exposure:component.Routes[0].MCP[0]}); if err!=nil {panic(err)}
 properties:=plan.Metadata().InputSchema.Properties["request"]["properties"].(map[string]interface{})
 if len(properties["lookup"].(map[string]interface{}))!=0 {panic("RawMessage changed")}
 fmt.Println("retained")
}

`
	for path, body := range map[string]string{"main.go": mainSource} {
		if err = os.WriteFile(filepath.Join(root, path), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "run", "-mod=readonly", ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	result, err := command.CombinedOutput()
	if err != nil || strings.TrimSpace(string(result)) != "retained" {
		t.Fatalf("fresh binary: %v\n%s", err, result)
	}
}

func TestWireTypeGenerationCanonicalizesAndRejectsConflict(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeFor[customjson.RequestWire](), reflect.TypeFor[customjson.OtherWire]()} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	const pkg = "github.com/viant/datly/mcp/tool/testdata/customjson"
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "body", Package: pkg}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, expression := range []string{"body.RequestWire", "body.OtherWire"} {
		component := &spec.Component{Name: "Requests", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "body", Package: pkg}}}, Parameters: []*spec.Parameter{{Name: "Body", Source: spec.BindSource{Kind: "body"}, TypeExpr: "string", WireTypeExpr: expression, Tag: `wireType:"` + pkg + `.RequestWire"`}}}
		plan, err := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"}).Plan()
		if expression == "body.OtherWire" {
			if err == nil || !strings.Contains(err.Error(), "conflicting wireType") {
				t.Fatalf("conflict: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.WireTypes) != 1 || reflect.StructTag(plan.Input.Fields[0].Tag).Get("wireType") != pkg+".RequestWire" || component.Parameters[0].WireTypeExpr != expression {
			t.Fatalf("canonical identity/immutability: %+v", plan)
		}
	}
}
