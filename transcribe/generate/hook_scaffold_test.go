package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testHookScaffoldAsset(t *testing.T) *HookScaffoldAsset {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", `package generated

import "context"

func (input *OrdersInput) Init(ctx context.Context) error { return nil }
func (output *OrdersOutput) Finalize(ctx context.Context, handlerErr error) error { return nil }
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return &HookScaffoldAsset{File: file}
}

func TestGeneratorRejectsHookScaffoldWithoutGeneratedContractHandler(t *testing.T) {
	_, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		HookScaffold: testHookScaffoldAsset(t),
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "requires a generated contract handler") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestResolveHookScaffoldRejectsLinkedContracts(t *testing.T) {
	plan := &Plan{
		ComponentName: "Orders",
		Input:         ContractPlan{Type: "contracts.Input", Ownership: ContractLinked},
		Output:        ContractPlan{Type: "contracts.Output", Ownership: ContractLinked},
		ContractHandler: &ContractHandlerPlan{
			Factory: "NewOrdersHandler",
		},
	}
	err := resolveHookScaffold(plan, testHookScaffoldAsset(t))
	if err == nil || !strings.Contains(err.Error(), "package-local generated input and output contracts") {
		t.Fatalf("resolveHookScaffold() error = %v", err)
	}
}

func TestGeneratorRejectsHookScaffoldDestinationCollision(t *testing.T) {
	handler := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
	hooks := testHookScaffoldAsset(t)
	hooks.Destination = "orders_input.go"
	_, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		ContractHandler: handler, HookScaffold: hooks,
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "shared by input contract and user hook scaffold") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsMalformedHookScaffoldLifecycle(t *testing.T) {
	handler := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
	hooks := testHookScaffoldAsset(t)
	for _, declaration := range hooks.File.Decls {
		method, ok := declaration.(*ast.FuncDecl)
		if ok && method.Name.Name == "Finalize" {
			method.Type.Params.List = method.Type.Params.List[:1]
		}
	}
	_, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		ContractHandler: handler, HookScaffold: hooks,
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "output hook Finalize") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestDirectEmissionRevalidatesHookScaffoldPlan(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Plan)
		want   string
	}{
		{
			name: "missing generated contract handler",
			mutate: func(plan *Plan) {
				plan.ContractHandler = nil
			},
			want: "requires a generated contract handler",
		},
		{
			name: "malformed error finalizer",
			mutate: func(plan *Plan) {
				for _, declaration := range plan.HookScaffold.File.Decls {
					method, ok := declaration.(*ast.FuncDecl)
					if ok && method.Name.Name == "Finalize" {
						method.Type.Params.List = method.Type.Params.List[:1]
					}
				}
			},
			want: "output hook Finalize",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
			plan, err := New(Input{
				Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
				ContractHandler: handler, HookScaffold: testHookScaffoldAsset(t),
			}).Plan()
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			test.mutate(plan)
			dir := t.TempDir()
			if err = plan.ValidateDestination(dir); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateDestination() error = %v", err)
			}
			if _, err = EmitScaffold(dir, plan); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("EmitScaffold() error = %v", err)
			}
		})
	}
}

func TestRegenerationRejectsStaleUserHookSignatureWithoutFileLoss(t *testing.T) {
	handler := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
	input := Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		ContractHandler: handler, HookScaffold: testHookScaffoldAsset(t),
	}
	dir := t.TempDir()
	if _, err := New(input).Generate(dir); err != nil {
		t.Fatalf("initial Generate() error = %v", err)
	}
	hookPath := filepath.Join(dir, "orders_hooks.go")
	hookSource, err := os.ReadFile(hookPath)
	if err != nil {
		t.Fatal(err)
	}
	staleSource := strings.Replace(string(hookSource),
		"Finalize(ctx context.Context, handlerErr error)",
		"Finalize(ctx context.Context)", 1)
	if staleSource == string(hookSource) {
		t.Fatal("expected generated Finalize signature")
	}
	if err = os.WriteFile(hookPath, []byte(staleSource), 0o644); err != nil {
		t.Fatal(err)
	}
	routerPath := filepath.Join(dir, "orders_router.go")
	routerBefore, err := os.ReadFile(routerPath)
	if err != nil {
		t.Fatal(err)
	}

	input.Component = input.Component.Clone()
	input.Component.Routes[0].Path = "/changed"
	plan, err := New(input).Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if err = plan.ValidateDestination(dir); err == nil ||
		!strings.Contains(err.Error(), `user-owned hook scaffold "orders_hooks.go"`) ||
		!strings.Contains(err.Error(), "lifecycle contract changed: output hook Finalize") {
		t.Fatalf("ValidateDestination() error = %v", err)
	}
	if _, err = New(input).Generate(dir); err == nil ||
		!strings.Contains(err.Error(), "lifecycle contract changed: output hook Finalize") {
		t.Fatalf("regenerated stale hook error = %v", err)
	}
	hookAfter, hookErr := os.ReadFile(hookPath)
	routerAfter, routerErr := os.ReadFile(routerPath)
	if hookErr != nil || string(hookAfter) != staleSource {
		t.Fatalf("user hook changed: err=%v\n%s", hookErr, hookAfter)
	}
	if routerErr != nil || string(routerAfter) != string(routerBefore) {
		t.Fatalf("managed package changed after stale hook failure: err=%v\n%s", routerErr, routerAfter)
	}
}
