package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	fixture "github.com/viant/datly/transcribe/testdata/linkedcontract"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestGeneratedFactoryExportsLinkAutomatically(t *testing.T) {
	for _, kind := range []string{"contract", "mutation"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/linkfixture"}).Write(t, root)
			input := Input{Component: customHandlerComponent(), TargetPackage: "github.com/viant/datly/linkfixture/orders"}
			if kind == "contract" {
				input.ContractHandler = parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
			} else {
				input.MutationHandler = mutationAsset(t, mutationDefinitionFixture)
			}
			dir := filepath.Join(root, "orders")
			generated, err := New(input).Generate(dir)
			if err != nil {
				t.Fatal(err)
			}
			if generated.Plan.FactoryLink == nil || generated.Plan.FactoryLink.Name != "RegisterOrdersFactories" || generated.Plan.FactoryLink.Destination != "links.go" {
				t.Fatalf("link=%+v", generated.Plan.FactoryLink)
			}
			source := strings.ReplaceAll(factoryLinkSmoke, "{{FAIL}}", map[string]string{"contract": "false", "mutation": "true"}[kind])
			if err = os.WriteFile(filepath.Join(dir, "link_test.go"), []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-race", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("compiled linked %s factory: %v\n%s", kind, err, output)
			}
			path := filepath.Join(dir, generated.Plan.FactoryLink.Destination)
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			edited := append(content, []byte("\n// user edited linking code\n")...)
			if err = os.WriteFile(path, edited, 0644); err != nil {
				t.Fatal(err)
			}
			if _, err = New(input).Generate(dir); err == nil {
				t.Fatal("edited linking artifact overwritten")
			}
			actual, err := os.ReadFile(path)
			if err != nil || string(actual) != string(edited) {
				t.Fatal("failed generation changed edited artifact")
			}
		})
	}
}

func TestFactoryLinkCompilesOnlyUsedContractImports(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(map[bool]string{false: "generated fields", true: "linked aliases"}[linked], func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/linkimports"}).Write(t, root)
			pkg := reflect.TypeOf(fixture.Input{}).PkgPath()
			catalog := typecatalog.NewCatalog()
			for _, typ := range []reflect.Type{reflect.TypeOf(fixture.Input{}), reflect.TypeOf(fixture.Output{}), reflect.TypeOf(fixture.Event{}), reflect.TypeOf(time.Time{})} {
				if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
					t.Fatal(err)
				}
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "github.com/viant/datly/linkimports/orders", Imports: []typecatalog.PackageImport{{Alias: "registry", Package: pkg}, {Alias: "named", Package: pkg}, {Alias: "time", Package: "time"}}})
			if err != nil {
				t.Fatal(err)
			}
			component := customHandlerComponent()
			component.TypeContext = &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "registry", Package: pkg}, {Alias: "named", Package: pkg}, {Alias: "time", Package: "time"}}}
			input := Input{Component: component, TargetPackage: "github.com/viant/datly/linkimports/orders", TypeResolver: resolver}
			source := validContractHandlerSource()
			if linked {
				input.Contracts = ContractReferences{Input: &ContractReference{Expression: "registry.Input", DescriptorKey: pkg + ".Input"}, Output: &ContractReference{Expression: "registry.Output", DescriptorKey: pkg + ".Output"}}
				source = strings.NewReplacer("OrdersInput", "models.Input", "OrdersOutput", "models.Output", `"context"`, `"context"`+"\n models "+strconv.Quote(pkg)).Replace(source)
			} else {
				component.Parameters = []*spec.Parameter{{Name: "Record", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "*named.Event"}, {Name: "When", Source: spec.BindSource{Kind: "query", Name: "when"}, TypeExpr: "time.Time"}}
			}
			input.ContractHandler = parseContractHandlerAsset(t, "NewOrdersHandler", source)
			result, err := New(input).Generate(filepath.Join(root, "orders"))
			if err != nil {
				t.Fatal(err)
			}
			content, err := os.ReadFile(filepath.Join(root, "orders", result.Plan.FactoryLink.Destination))
			if err != nil {
				t.Fatal(err)
			}
			if !linked && (strings.Contains(string(content), `"time"`) || strings.Contains(string(content), strconv.Quote(pkg))) {
				t.Fatalf("unused field imports in companion: %s", content)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("compiled companion imports: %v\n%s", err, output)
			}
		})
	}
}

const factoryLinkSmoke = `package orders
import(
 "context"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/x"
)
func TestGeneratedNamedSelection(t *testing.T){
 exports:=x.NewRegistry();if err:=RegisterOrdersFactories(exports);err!=nil{t.Fatal(err)}
 if err:=RegisterOrdersFactories(exports);err==nil{t.Fatal("duplicate package registration accepted")}
 if err:=RegisterOrdersFactories(nil);err==nil{t.Fatal("nil registry accepted")}
 builder,err:=bootstrap.NewArtifactBuilder(exports);if err!=nil{t.Fatal(err)}
 holder:=reflect.TypeOf(Component{});field,_:=holder.FieldByName("Contract");tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 source:=&bootstrap.RouteSource{PackagePath:holder.PkgPath(),PackageName:"orders",HolderType:"Component",FieldName:"Contract",Tag:tag,InputType:"OrdersInput",OutputType:"OrdersOutput"}
 component,err:=source.Resolve(reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}));if err!=nil{t.Fatal(err)}
 artifact,err:=builder.Build(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{})});if err!=nil{t.Fatal(err)}
 registered,err:=artifact.Registration(druntime.RegisteredComponent{});if err!=nil{t.Fatal(err)}
 runtime,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{registered});if err!=nil{t.Fatal(err)}
 _,err=runtime.ExecuteRoute(context.Background(),"POST","/orders",nil)
 if {{FAIL}} {if err==nil||!strings.Contains(err.Error(),"artifact failure finalized"){t.Fatalf("mutation lifecycle=%v",err)}}else if err!=nil{t.Fatal(err)}
}
`

func TestFactoryLinkNamesAndAliases(t *testing.T) {
	for _, kind := range []string{"contract", "mutation"} {
		input := Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders"}
		if kind == "contract" {
			input.ContractHandler = parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource()+"\nfunc RegisterOrdersFactories(){}\n")
		} else {
			input.MutationHandler = mutationAsset(t, mutationDefinitionFixture+"\nfunc RegisterOrdersFactories(){}\n")
		}
		if _, err := New(input).Plan(); err == nil || !strings.Contains(err.Error(), "shared by factory linking export") {
			t.Fatalf("%s collision=%v", kind, err)
		}
	}
	asset := mutationAsset(t, mutationDefinitionFixture)
	asset.Destination = "links.go"
	if _, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset}).Plan(); err == nil {
		t.Fatal("link destination collision accepted")
	}
	plan := &Plan{Input: ContractPlan{Type: "x.Input", Ownership: ContractLinked}, Output: ContractPlan{Type: "registry.Output", Ownership: ContractLinked}, Imports: []spec.ImportSpec{{Alias: "x", Package: "example.com/input"}, {Alias: "registry", Package: "example.com/output"}}}
	link := &FactoryLinkPlan{Name: "RegisterFactories", Factory: "NewDefinition", PackagePath: "example.com/generated", Adapter: "mutation"}
	source, err := link.source("generated", plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "registry2 *x2.Registry") || !strings.Contains(source, "Factory[x.Input, registry.Output]") {
		t.Fatalf("unsafe linking aliases: %s", source)
	}
}
