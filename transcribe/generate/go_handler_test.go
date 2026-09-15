package generate

import (
	"github.com/viant/datly/internal/testharness"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func parseHandlerAsset(t *testing.T, entry, source string) *GoHandlerAsset {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "handler.go", source, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse handler: %v", err)
	}
	return &GoHandlerAsset{Entry: entry, File: file}
}

func customHandlerComponent() *spec.Component {
	return &spec.Component{
		Name:   "Orders",
		Routes: []*spec.Route{{Method: "POST", Path: "/orders"}},
	}
}

func validHandlerSource(entry string) string {
	return `package authored

import "context"

func ` + entry + `(ctx context.Context, input *OrdersInput) (*OrdersOutput, error) {
	return &OrdersOutput{}, nil
}
`
}

func TestGeneratorEmitsAcceptedCustomHandler(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "orders")
	componentSpec := customHandlerComponent()
	componentSpec.Routes = append(componentSpec.Routes, &spec.Route{Method: "POST", Path: "/orders/import"})
	result, err := New(Input{
		Component: componentSpec, TargetPackage: "example.com/generated/orders",
		GoHandler: parseHandlerAsset(t, "HandleOrders", validHandlerSource("HandleOrders")),
	}).Generate(dir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if result.Plan.Handler != "HandleOrders" || result.Plan.GoHandler == nil {
		t.Fatalf("handler plan = %+v", result.Plan)
	}
	content, err := os.ReadFile(filepath.Join(dir, "handler.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "package orders") || !strings.Contains(string(content), "func HandleOrders") {
		t.Fatalf("handler source:\n%s", content)
	}
	component, err := os.ReadFile(filepath.Join(dir, result.Plan.RouterDest))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(component), "handler=HandleOrders") {
		t.Fatalf("component source:\n%s", component)
	}
	tags, err := generatedComponentContractTags(component)
	if err != nil || len(tags) != 2 || tags[0].Handler != "HandleOrders" || tags[1].Handler != "HandleOrders" {
		t.Fatalf("component route tags = %+v, %v", tags, err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated module does not compile: %v\n%s", runErr, output)
	}
}

func TestGeneratorCustomHandlerRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name   string
		entry  string
		source string
		match  string
	}{
		{name: "missing", entry: "HandleOrders", source: `package p`, match: "was not found"},
		{name: "unexported", entry: "handleOrders", source: validHandlerSource("handleOrders"), match: "exported Go identifier"},
		{name: "context", entry: "HandleOrders", source: `package p; func HandleOrders(ctx string, input *OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: "context.Context"},
		{name: "input", entry: "HandleOrders", source: `package p; import "context"; func HandleOrders(ctx context.Context, input OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: "input parameter type"},
		{name: "output", entry: "HandleOrders", source: `package p; import "context"; func HandleOrders(ctx context.Context, input *OrdersInput) (OrdersOutput, error) { return OrdersOutput{}, nil }`, match: "first result type"},
		{name: "generic", entry: "HandleOrders", source: `package p; import "context"; func HandleOrders[T any](ctx context.Context, input *OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: "cannot declare type parameters"},
		{name: "method", entry: "HandleOrders", source: `package p; import "context"; type service struct{}; func (service) HandleOrders(ctx context.Context, input *OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: "must be a function"},
		{name: "error import", entry: "HandleOrders", source: `package p; import "context"; import error "errors"; func HandleOrders(ctx context.Context, input *OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: "cannot shadow the predeclared error type"},
		{name: "contract import shadow", entry: "HandleOrders", source: `package p; import "context"; import OrdersInput "fmt"; func HandleOrders(ctx context.Context, input *OrdersInput) (*OrdersOutput, error) { return nil, nil }`, match: `type identifier "OrdersInput" is shadowed by import "fmt"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(Input{
				Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
				GoHandler: parseHandlerAsset(t, test.entry, test.source),
			}).Plan()
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("Plan() error = %v, want %q", err, test.match)
			}
		})
	}
}

func TestGeneratorCustomHandlerRejectsOwnershipCollisions(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*spec.Component, *GoHandlerAsset)
		match   string
	}{
		{name: "route identity", prepare: func(component *spec.Component, _ *GoHandlerAsset) {
			component.Routes[0].Handler = "OtherHandler"
		}, match: "conflicts with custom handler"},
		{name: "destination", prepare: func(_ *spec.Component, asset *GoHandlerAsset) {
			asset.Destination = "router.go"
		}, match: "shared by component holder and custom handler"},
		{name: "generated declaration", prepare: func(_ *spec.Component, asset *GoHandlerAsset) {
			asset.File.Decls = append(asset.File.Decls, &ast.GenDecl{
				Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent("Component"), Type: &ast.StructType{Fields: &ast.FieldList{}}}},
			})
		}, match: "conflicts with generated package ownership"},
		{name: "predeclared error", prepare: func(_ *spec.Component, asset *GoHandlerAsset) {
			asset.File.Decls = append(asset.File.Decls, &ast.GenDecl{
				Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent("error"), Type: &ast.StructType{Fields: &ast.FieldList{}}}},
			})
		}, match: "cannot shadow the predeclared error type"},
		{name: "input marker", prepare: func(component *spec.Component, asset *GoHandlerAsset) {
			component.Parameters = []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "body", Name: "ID"}, TypeExpr: "int"}}
			asset.File.Decls = append(asset.File.Decls, &ast.GenDecl{
				Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent("OrdersInputHas"), Type: &ast.StructType{Fields: &ast.FieldList{}}}},
			})
		}, match: "conflicts with generated package ownership"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			component := customHandlerComponent()
			asset := parseHandlerAsset(t, "HandleOrders", validHandlerSource("HandleOrders"))
			test.prepare(component, asset)
			_, err := New(Input{Component: component, TargetPackage: "example.com/generated/orders", GoHandler: asset}).Plan()
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("Plan() error = %v, want %q", err, test.match)
			}
		})
	}
}

func TestGeneratorCustomHandlerOwnsIsolatedASTAndRemovesStaleFile(t *testing.T) {
	dir := t.TempDir()
	asset := parseHandlerAsset(t, "HandleOrders", validHandlerSource("HandleOrders"))
	generator := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders", GoHandler: asset,
	})
	asset.Entry = "Changed"
	for _, declaration := range asset.File.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			function.Name.Name = "Changed"
		}
	}
	if _, err := generator.Generate(dir); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dir, "handler.go"))
	if err != nil || !strings.Contains(string(content), "HandleOrders") || strings.Contains(string(content), "Changed") {
		t.Fatalf("isolated handler source = %q, %v", content, err)
	}
	if _, err = New(Input{Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders"}).Generate(dir); err != nil {
		t.Fatalf("Generate() without handler error = %v", err)
	}
	if _, err = os.Stat(filepath.Join(dir, "handler.go")); !os.IsNotExist(err) {
		t.Fatalf("stale handler file remains: %v", err)
	}
}

func TestGeneratorRejectsInputMarkerAndPlaceholderCollision(t *testing.T) {
	component := customHandlerComponent()
	component.Parameters = []*spec.Parameter{
		{Name: "ID", Source: spec.BindSource{Kind: "body", Name: "ID"}, TypeExpr: "int"},
		{Name: "Presence", Source: spec.BindSource{Kind: "body", Name: "Presence"}, TypeExpr: "OrdersInputHas"},
	}
	_, err := New(Input{Component: component, TargetPackage: "example.com/generated/orders"}).Plan()
	if err == nil || !strings.Contains(err.Error(), `generated type "OrdersInputHas" is shared by input presence marker and placeholder OrdersInputHas`) {
		t.Fatalf("Plan() error = %v", err)
	}
}
