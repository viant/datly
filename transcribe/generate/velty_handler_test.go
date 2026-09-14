package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func veltyHandlerComponent() *spec.Component {
	return &spec.Component{
		Name:   "Orders",
		Routes: []*spec.Route{{Method: "POST", Path: "/orders"}},
		Parameters: []*spec.Parameter{
			{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string"},
			{Name: "Result", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "string"},
		},
	}
}

func TestGeneratorEmitsExecutableVeltyHandlerArtifact(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "orders")
	asset := &VeltyHandlerAsset{Template: `#set($Output.Result = $Input.Name)`}
	componentSpec := veltyHandlerComponent()
	componentSpec.Routes = append(componentSpec.Routes, &spec.Route{Method: "PUT", Path: "/orders/{id}"})
	result, err := New(Input{
		Component: componentSpec, TargetPackage: "example.com/generated/orders", VeltyHandler: asset,
	}).Generate(dir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if result.Plan.Handler != "NewOrdersHandler" || result.Plan.VeltyHandler == nil {
		t.Fatalf("Velty handler plan = %+v", result.Plan)
	}
	component, err := os.ReadFile(filepath.Join(dir, result.Plan.RouterDest))
	if err != nil || !strings.Contains(string(component), "handler=NewOrdersHandler") {
		t.Fatalf("component source = %q, %v", component, err)
	}
	tags, err := generatedComponentContractTags(component)
	if err != nil || len(tags) != 2 || tags[0].Handler != "NewOrdersHandler" || tags[1].Handler != "NewOrdersHandler" {
		t.Fatalf("component route tags = %+v, %v", tags, err)
	}
	factory, err := os.ReadFile(filepath.Join(dir, "orders_velty.go"))
	if err != nil || !strings.Contains(string(factory), "func NewOrdersHandler()") ||
		!strings.Contains(string(factory), `//go:embed "orders/handler.velty"`) {
		t.Fatalf("factory source = %q, %v", factory, err)
	}
	template, err := os.ReadFile(filepath.Join(dir, "orders", "handler.velty"))
	if err != nil || string(template) != asset.Template {
		t.Fatalf("template = %q, %v", template, err)
	}
	testSource := `package orders

import (
	"context"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
)

func TestGeneratedVeltyFactory(t *testing.T) {
	handler, err := NewOrdersHandler()
	if err != nil { t.Fatal(err) }
	injector, err := bindly.NewInjector()
	if err != nil { t.Fatal(err) }
	scope, err := injector.ForScope()
	if err != nil { t.Fatal(err) }
	input := &OrdersInput{Name: "Ada"}
	actual, err := handler.Execute(context.Background(), rhandler.Invocation{Input: input, Binder: rhandler.NewBinder(scope, input)})
	if err != nil { t.Fatal(err) }
	output, ok := actual.(*OrdersOutput)
	if !ok || output.Result != "Ada" { t.Fatalf("output = %#v", actual) }
}
`
	if err = os.WriteFile(filepath.Join(dir, "velty_factory_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated Velty module does not execute: %v\n%s", runErr, output)
	}
}

func TestGeneratorEmitsExecutableVeltyHandlerWithLinkedContracts(t *testing.T) {
	type linkedInput struct{ Name string }
	type linkedOutput struct{ Result string }

	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	contractsDir := filepath.Join(root, "error")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	contracts := "package contracts\n\ntype Input struct { Name string }\ntype Output struct { Result string }\n"
	if err := os.WriteFile(filepath.Join(contractsDir, "contracts.go"), []byte(contracts), 0o644); err != nil {
		t.Fatal(err)
	}

	catalog := typecatalog.NewCatalog()
	inputDescriptor := x.NewType(reflect.TypeOf(linkedInput{}), x.WithName("Input"), x.WithPkgPath("example.com/generated/error"))
	outputDescriptor := x.NewType(reflect.TypeOf(linkedOutput{}), x.WithName("Output"), x.WithPkgPath("example.com/generated/error"))
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, inputDescriptor, outputDescriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	packageDir := filepath.Join(root, "orders")
	_, err = New(Input{
		Component: veltyHandlerComponent(), TypeResolver: resolver, TargetPackage: "example.com/generated/orders",
		Contracts: ContractReferences{
			Input:  &ContractReference{Expression: "Input", DescriptorKey: inputDescriptor.Key()},
			Output: &ContractReference{Expression: "Output", DescriptorKey: outputDescriptor.Key()},
		},
		VeltyHandler: &VeltyHandlerAsset{
			Template:            `#set($Output.Result = $Input.Name)`,
			ResourceDestination: "templates/order handler.velty",
		},
	}).Generate(packageDir)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := os.ReadFile(filepath.Join(packageDir, "orders_velty.go"))
	if err != nil || !strings.Contains(string(factory), `error2 "example.com/generated/error"`) ||
		!strings.Contains(string(factory), `error2.Input, error2.Output`) {
		t.Fatalf("factory source = %q, %v", factory, err)
	}
	testSource := `package orders

import (
	"context"
	"testing"

	contracts "example.com/generated/error"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
)

func TestLinkedVeltyFactory(t *testing.T) {
	handler, err := NewOrdersHandler()
	if err != nil { t.Fatal(err) }
	injector, err := bindly.NewInjector()
	if err != nil { t.Fatal(err) }
	scope, err := injector.ForScope()
	if err != nil { t.Fatal(err) }
	input := &contracts.Input{Name: "Ada"}
	actual, err := handler.Execute(context.Background(), rhandler.Invocation{Input: input, Binder: rhandler.NewBinder(scope, input)})
	if err != nil { t.Fatal(err) }
	output, ok := actual.(*contracts.Output)
	if !ok || output.Result != "Ada" { t.Fatalf("output = %#v", actual) }
}
`
	if err = os.WriteFile(filepath.Join(packageDir, "velty_linked_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("linked-contract Velty module does not execute: %v\n%s", runErr, output)
	}
}

func TestGeneratorVeltyHandlerRejectsInvalidAssets(t *testing.T) {
	tests := []struct {
		name    string
		asset   *VeltyHandlerAsset
		prepare func(*spec.Component)
		match   string
	}{
		{name: "empty template", asset: &VeltyHandlerAsset{}, match: "template is required"},
		{name: "unexported factory", asset: &VeltyHandlerAsset{Template: "$Input", Factory: "newHandler"}, match: "exported Go identifier"},
		{name: "nested Go destination", asset: &VeltyHandlerAsset{Template: "$Input", GoDestination: "orders/handler.go"}, match: "package-local .go file"},
		{name: "resource extension", asset: &VeltyHandlerAsset{Template: "$Input", ResourceDestination: "orders/handler.txt"}, match: "must use .sql or .velty"},
		{name: "route conflict", asset: &VeltyHandlerAsset{Template: "$Input"}, prepare: func(component *spec.Component) {
			component.Routes[0].Handler = "HandleOrders"
		}, match: "conflicts with Velty handler factory"},
		{name: "factory name collision", asset: &VeltyHandlerAsset{Template: "$Input", Factory: "Component"}, match: `generated type "Component" is shared`},
		{name: "Go destination collision", asset: &VeltyHandlerAsset{Template: "$Input", GoDestination: "orders_router.go"}, match: "shared by component holder and Velty handler factory"},
		{name: "resource ancestor collision", asset: &VeltyHandlerAsset{Template: "$Input", ResourceDestination: "orders_router.go/handler.velty"}, match: "overlap"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := veltyHandlerComponent()
			if test.prepare != nil {
				test.prepare(component)
			}
			_, err := New(Input{
				Component: component, TargetPackage: "example.com/generated/orders", VeltyHandler: test.asset,
			}).Plan()
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("Plan() error = %v, want %q", err, test.match)
			}
		})
	}
}

func TestGeneratorVeltyHandlerRejectsWrappedContractsAndDualAssets(t *testing.T) {
	plan := &Plan{Input: ContractPlan{Type: "*OrdersInput"}, Output: ContractPlan{Type: "OrdersOutput"}}
	if err := resolveVeltyHandler(plan, &VeltyHandlerAsset{Template: "$Input"}); err == nil || !strings.Contains(err.Error(), "direct named struct contracts") {
		t.Fatalf("resolveVeltyHandler() error = %v", err)
	}
	_, err := New(Input{
		Component: veltyHandlerComponent(), TargetPackage: "example.com/generated/orders",
		GoHandler:    parseHandlerAsset(t, "HandleOrders", validHandlerSource("HandleOrders")),
		VeltyHandler: &VeltyHandlerAsset{Template: "$Input"},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorVeltyHandlerRejectsErrorContractImportAlias(t *testing.T) {
	plan := &Plan{
		Input:   ContractPlan{Type: "error.Input", Ownership: ContractLinked},
		Output:  ContractPlan{Type: "Output", Ownership: ContractGenerated},
		Imports: []spec.ImportSpec{{Alias: "error", Package: "example.com/contracts"}},
	}
	err := resolveVeltyHandler(plan, &VeltyHandlerAsset{Template: "$Input"})
	if err == nil || !strings.Contains(err.Error(), "shadows the predeclared error type") {
		t.Fatalf("resolveVeltyHandler() error = %v", err)
	}
}

func TestVeltyHandlerAutomaticErrorAliasReusesOneExplicitImport(t *testing.T) {
	const packagePath = "example.com/contracts/error"
	plan := &Plan{Imports: []spec.ImportSpec{{Package: packagePath}}}
	alias := uniqueImportAlias(plan, packagePath)
	if alias != "error2" {
		t.Fatalf("uniqueImportAlias() = %q, want error2", alias)
	}
	ensureImport(plan, alias, packagePath)
	plan.Input = ContractPlan{Type: alias + ".Input", Ownership: ContractLinked}
	outputAlias := uniqueImportAlias(plan, packagePath)
	if outputAlias != alias {
		t.Fatalf("second uniqueImportAlias() = %q, want reused %q", outputAlias, alias)
	}
	ensureImport(plan, outputAlias, packagePath)
	plan.Output = ContractPlan{Type: outputAlias + ".Output", Ownership: ContractLinked}
	imports := plan.contractImports()
	if len(imports) != 1 || imports[0].Alias != "error2" || imports[0].Package != packagePath {
		t.Fatalf("contractImports() = %+v", imports)
	}
}

func TestGeneratorVeltyHandlerClonesAndRemovesManagedArtifacts(t *testing.T) {
	dir := t.TempDir()
	asset := &VeltyHandlerAsset{Template: `$Output.Result = "original"`}
	generator := New(Input{
		Component: veltyHandlerComponent(), TargetPackage: "example.com/generated/orders", VeltyHandler: asset,
	})
	asset.Template = "changed"
	result, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	templatePath := filepath.Join(dir, "orders", "handler.velty")
	content, err := os.ReadFile(templatePath)
	if err != nil || string(content) != `$Output.Result = "original"` {
		t.Fatalf("isolated template = %q, %v", content, err)
	}
	// Handler cleanup is independent of shape ownership; retain the exact
	// compiled binding tags while removing only the handler artifacts.
	result.Plan.VeltyHandler = nil
	if _, err = EmitScaffold(dir, result.Plan); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "orders_velty.go"), templatePath} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("stale Velty artifact %s remains: %v", path, err)
		}
	}
}

func TestVeltyHandlerFactoryUsesCollisionSafeRuntimeAlias(t *testing.T) {
	plan := &Plan{
		Input:   ContractPlan{Type: "veltyhandler.Input", Ownership: ContractLinked},
		Output:  ContractPlan{Type: "Output", Ownership: ContractGenerated},
		Imports: []spec.ImportSpec{{Alias: "veltyhandler", Package: "example.com/contracts"}},
		VeltyHandler: &VeltyHandlerPlan{
			Factory: "NewHandler", GoDestination: "handler.go", ResourceDestination: "handler.velty",
		},
	}
	content, err := veltyHandlerFileText("generated", plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, `veltyhandler2 "github.com/viant/datly/runtime/handler/velty"`) ||
		!strings.Contains(content, `veltyhandler "example.com/contracts"`) ||
		!strings.Contains(content, `*veltyhandler2.Handler[veltyhandler.Input, Output]`) {
		t.Fatalf("factory source:\n%s", content)
	}
}

func TestVeltyHandlerFactoryReusesRuntimePackageContractImport(t *testing.T) {
	plan := &Plan{
		Input:   ContractPlan{Type: "vh.Context", Ownership: ContractLinked},
		Output:  ContractPlan{Type: "Output", Ownership: ContractGenerated},
		Imports: []spec.ImportSpec{{Alias: "vh", Package: veltyHandlerPackage}},
		VeltyHandler: &VeltyHandlerPlan{
			Factory: "NewHandler", GoDestination: "handler.go", ResourceDestination: "handler.velty",
		},
	}
	content, err := veltyHandlerFileText("generated", plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(content, `"`+veltyHandlerPackage+`"`) != 1 ||
		!strings.Contains(content, `*vh.Handler[vh.Context, Output]`) {
		t.Fatalf("factory source:\n%s", content)
	}
}
