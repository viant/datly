package generate

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

const mutationDefinitionFixture = `package authored
import (
 "context"
 "errors"
 h "github.com/viant/xdatly/handler"
 policy "github.com/viant/xdatly/handler/mutation"
)
type ordersDefinition struct{}
func NewOrdersDefinition() policy.Definition[OrdersInput,OrdersOutput] { return &ordersDefinition{} }
// This artifact fixture deliberately has no mutation Program implementation.
func(*ordersDefinition)Capture(context.Context,*OrdersInput)(policy.Program[OrdersOutput],error){return nil,errors.New("artifact capture boundary")}
func(*ordersDefinition)FinalizeFailure(context.Context,*OrdersInput,*OrdersOutput,h.Outcome)error{return errors.New("artifact failure finalized")}
`

func mutationAsset(t *testing.T, source string) *MutationHandlerAsset {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "definition.go", source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return &MutationHandlerAsset{Factory: "NewOrdersDefinition", File: file}
}

func TestMutationDefinitionArtifactExplicitComposition(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/mutationfixture"}).Write(t, root)
	input := Input{Component: customHandlerComponent(), TargetPackage: "github.com/viant/datly/mutationfixture/orders", MutationHandler: mutationAsset(t, mutationDefinitionFixture)}
	input.MutationHandler = mutationAsset(t, strings.ReplaceAll(mutationDefinitionFixture, `errors.New("artifact capture boundary")`, `errors.New(captureMessage)`))
	support, err := parser.ParseFile(token.NewFileSet(), "support.go", "package authored\nconst captureMessage = \"artifact capture boundary\"", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	input.MutationHandler.Support = []MutationSource{{Destination: "orders_frames_gen.go", File: support}}
	dir := filepath.Join(root, "orders")
	generated, err := New(input).Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Plan.MutationHandler == nil || generated.Plan.MutationHandler.Destination != "mutation.go" || generated.Plan.ContractHandler != nil || generated.Plan.Handler != "NewOrdersDefinition" {
		t.Fatalf("plan=%+v", generated.Plan)
	}
	const smoke = `package orders
import(
 "context"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 mutation "github.com/viant/datly/runtime/handler/mutation"
 dtag "github.com/viant/datly/tag"
)
func TestExplicitMutationFactoryComposition(t *testing.T){
 holder:=reflect.TypeOf(Component{});field,_:=holder.FieldByName("Contract")
 tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatalf("tag=%v error=%v",tag,err)}
 if tag.Handler!="NewOrdersDefinition"{t.Fatalf("factory metadata=%v",tag.Handler)}
 source:=&bootstrap.RouteSource{PackagePath:holder.PkgPath(),PackageName:"orders",HolderType:"Component",FieldName:"Contract",Tag:tag,InputType:"OrdersInput",OutputType:"OrdersOutput"}
 component,err:=source.Resolve(reflect.TypeOf(OrdersInput{}),reflect.TypeOf(OrdersOutput{}));if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(OrdersInput{}),OutputType:reflect.TypeOf(OrdersOutput{})});if err!=nil{t.Fatal(err)}
 // Explicit linking is the existing composition seam, not named-factory discovery.
 handler:=mutation.New[OrdersInput,OrdersOutput](NewOrdersDefinition())
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeOf(OrdersOutput{}),Handler:handler}});if err!=nil{t.Fatal(err)}
 _,err=rt.ExecuteRoute(context.Background(),"POST","/orders",nil)
 if err==nil||!strings.Contains(err.Error(),"artifact capture boundary")||!strings.Contains(err.Error(),"artifact failure finalized"){t.Fatalf("explicit factory did not use mutation adapter: %v",err)}
}
`
	if err = os.WriteFile(filepath.Join(dir, "composition_test.go"), []byte(smoke), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = New(input).Generate(dir); err != nil {
		t.Fatalf("unchanged regeneration: %v", err)
	}
	command := exec.Command("go", "test", "-race", "-mod=mod", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated factory composition: %v\n%s", err, output)
	}
	path := filepath.Join(dir, generated.Plan.MutationHandler.Destination)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(content, []byte("\n// handwritten customization\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = New(input).Generate(dir); err == nil {
		t.Fatal("edited mutation definition overwritten")
	}
}

func TestMutationDefinitionFactoryValidation(t *testing.T) {
	for _, test := range []struct{ name, old, replacement, match string }{
		{"custom contract", "policy.Definition[OrdersInput,OrdersOutput]", "h.Contract[OrdersInput,OrdersOutput]", "does not match"},
		{"other package authority", mutationHandlerPackage, "example.com/other/mutation", "does not match"},
		{"pointer input", "policy.Definition[OrdersInput,OrdersOutput]", "policy.Definition[*OrdersInput,OrdersOutput]", "does not match"},
		{"output mismatch", "policy.Definition[OrdersInput,OrdersOutput]", "policy.Definition[OrdersInput,OrdersInput]", "does not match"},
		{"factory arguments", "NewOrdersDefinition()", "NewOrdersDefinition(value int)", "factory signature"},
		{"factory generic", "NewOrdersDefinition()", "NewOrdersDefinition[T any]()", "type parameters"},
		{"reserved declaration", "type ordersDefinition struct{}", "type OrdersInput struct{}", "package ownership"},
		{"runtime import", mutationHandlerPackage, "github.com/viant/datly/runtime/handler/mutation", "cannot import Datly execution"},
		{"SQL import", mutationHandlerPackage, "github.com/viant/datly/sql/dml", "cannot import Datly SQL execution"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := strings.Replace(mutationDefinitionFixture, test.old, test.replacement, 1)
			_, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: mutationAsset(t, source)}).Plan()
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error=%v want %q", err, test.match)
			}
		})
	}
}

func TestMutationDefinitionAssetOwnershipAndSelection(t *testing.T) {
	asset := mutationAsset(t, mutationDefinitionFixture)
	generator := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset})
	asset.Factory = "Changed"
	asset.File.Decls = nil
	plan, err := generator.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.MutationHandler.Factory != "NewOrdersDefinition" || len(plan.MutationHandler.File.Decls) == 0 {
		t.Fatal("generator borrowed source AST")
	}
	plan.MutationHandler.File.Decls = nil
	second, err := generator.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(second.MutationHandler.File.Decls) == 0 {
		t.Fatal("returned plan mutated generator authority")
	}
	for _, destination := range []string{"../escape.go", "nested/definition.go", "input.go"} {
		t.Run(destination, func(t *testing.T) {
			asset := mutationAsset(t, mutationDefinitionFixture)
			asset.Destination = destination
			_, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: asset}).Plan()
			if err == nil {
				t.Fatal("invalid/colliding destination accepted")
			}
		})
	}
	for _, kind := range []string{"custom", "contract", "velty"} {
		t.Run(kind, func(t *testing.T) {
			input := Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: mutationAsset(t, mutationDefinitionFixture)}
			switch kind {
			case "custom":
				input.GoHandler = &GoHandlerAsset{}
			case "contract":
				input.ContractHandler = &ContractHandlerAsset{}
			case "velty":
				input.VeltyHandler = &VeltyHandlerAsset{}
			}
			if _, err := New(input).Plan(); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestMutationDefinitionFactoryUsesFullContractPackageIdentity(t *testing.T) {
	for _, test := range []struct {
		packagePath string
		fail        bool
	}{{"example.com/private/contracts", false}, {"example.com/other/contracts", true}} {
		t.Run(test.packagePath, func(t *testing.T) {
			plan := &Plan{ComponentName: "Orders", Input: ContractPlan{Type: "model.Input"}, Output: ContractPlan{Type: "model.Output"}, Imports: []spec.ImportSpec{{Alias: "model", Package: "example.com/private/contracts"}}}
			source := `package generated
import (
 policy "github.com/viant/xdatly/handler/mutation"
 other "` + test.packagePath + `"
)
func NewOrdersDefinition() policy.Definition[other.Input,other.Output]{return nil}
`
			err := mutationAsset(t, source).resolve(plan, "example.com/generated")
			if (err != nil) != test.fail {
				t.Fatalf("resolve=%v", err)
			}
		})
	}
}

func TestMutationDefinitionRejectsCrossArtifactNames(t *testing.T) {
	for _, declaration := range []string{"type Extra struct{}", "func Extra(){}"} {
		t.Run(declaration, func(t *testing.T) {
			const target = "example.com/generated/orders"
			catalog := typecatalog.NewCatalog()
			descriptor := generatedTypeDescriptor(target, "Extra")
			if err := catalog.Register(typecatalog.TypeOriginGenerated, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: target})
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(Input{Component: customHandlerComponent(), TargetPackage: target, TypeResolver: resolver, GeneratedTypes: []GeneratedTypeReference{{DescriptorKey: descriptor.Key()}}, MutationHandler: mutationAsset(t, mutationDefinitionFixture+"\n"+declaration)}).Plan()
			if err == nil || !strings.Contains(err.Error(), "shared by generated type Extra") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	entity, err := parser.ParseFile(token.NewFileSet(), "entities.go", "package authored; func Capture(){}; type ordersDefinition struct{}", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: mutationAsset(t, mutationDefinitionFixture), EntitySupport: &EntitySupportAsset{File: entity, CaptureFunction: "Capture"}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "shared by mutation definition and entity support") {
		t.Fatalf("entity collision=%v", err)
	}
}

func TestMutationDefinitionBlankAssertionsDoNotOwnPackageNames(t *testing.T) {
	source := mutationDefinitionFixture + "\nvar _ policy.Definition[OrdersInput,OrdersOutput] = (*ordersDefinition)(nil)\nvar _ policy.Definition[OrdersInput,OrdersOutput] = (*ordersDefinition)(nil)\n"
	if _, err := New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", MutationHandler: mutationAsset(t, source)}).Plan(); err != nil {
		t.Fatal(err)
	}
}

func TestContractHookSupportRejectsEntitySidecarNameCollision(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "entities.go", "package authored; func Capture(){}; type ordersContract struct{}", parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/orders", ContractHandler: parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource()), EntitySupport: &EntitySupportAsset{File: file, CaptureFunction: "Capture"}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "shared by generated contract handler and entity support") {
		t.Fatalf("collision=%v", err)
	}
}
