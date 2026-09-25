package generate

import (
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func parseContractHandlerAsset(t *testing.T, factory, source string) *ContractHandlerAsset {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "handler.go", source, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse generated contract handler: %v", err)
	}
	return &ContractHandlerAsset{Factory: factory, File: file}
}

func validContractHandlerSource() string {
	return `package transcribed

import (
	"context"
	xhandler "github.com/viant/xdatly/handler"
)

type ordersContract struct{}

var _ xhandler.Contract[OrdersInput, OrdersOutput] = (*ordersContract)(nil)

func NewOrdersHandler() xhandler.Contract[OrdersInput, OrdersOutput] {
	return &ordersContract{}
}

func (*ordersContract) Exec(ctx context.Context, session xhandler.Session, input *OrdersInput, output *OrdersOutput) error {
	return nil
}
`
}

func TestGeneratorEmitsGeneratedContractHandler(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dir := filepath.Join(root, "orders")
	asset := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
	result, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders", ContractHandler: asset,
	}).Generate(dir)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if result.Plan.Handler != "NewOrdersHandler" || result.Plan.ContractHandler == nil ||
		result.Plan.ContractHandler.Destination != "handler.go" {
		t.Fatalf("generated contract handler plan = %+v, handler = %q", result.Plan.ContractHandler, result.Plan.Handler)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated contract handler module failed: %v\n%s", runErr, output)
	}
}

func TestGeneratorContractHandlerOwnsClonedAST(t *testing.T) {
	asset := parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource())
	generator := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders", ContractHandler: asset,
	})
	asset.Factory = "Changed"
	asset.File.Decls = nil
	plan, err := generator.Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if plan.ContractHandler == nil || plan.ContractHandler.Factory != "NewOrdersHandler" {
		t.Fatalf("cloned contract handler = %+v", plan.ContractHandler)
	}
}

func TestGeneratorRejectsInvalidContractHandlerFactory(t *testing.T) {
	source := strings.Replace(validContractHandlerSource(),
		"func NewOrdersHandler() xhandler.Contract[OrdersInput, OrdersOutput]",
		"func NewOrdersHandler(input *OrdersInput) xhandler.Contract[OrdersInput, OrdersOutput]", 1)
	_, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		ContractHandler: parseContractHandlerAsset(t, "NewOrdersHandler", source),
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "factory signature") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsCompetingGeneratedContractHandler(t *testing.T) {
	_, err := New(Input{
		Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
		ContractHandler: parseContractHandlerAsset(t, "NewOrdersHandler", validContractHandlerSource()),
		VeltyHandler:    &VeltyHandlerAsset{Template: "$Input"},
	}).Plan()
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestGeneratorRejectsContractHandlerDatlyExecutionImport(t *testing.T) {
	for _, packagePath := range []string{
		"github.com/viant/datly/exec",
		"github.com/viant/datly/runtime/handler/custom",
	} {
		t.Run(packagePath, func(t *testing.T) {
			source := strings.Replace(validContractHandlerSource(), `"context"`,
				`"context"`+"\n\tforbidden "+strconv.Quote(packagePath), 1)
			_, err := New(Input{
				Component: customHandlerComponent(), TargetPackage: "example.com/generated/orders",
				ContractHandler: parseContractHandlerAsset(t, "NewOrdersHandler", source),
			}).Plan()
			if err == nil || !strings.Contains(err.Error(), "cannot import Datly execution package") {
				t.Fatalf("Plan() error = %v", err)
			}
		})
	}
}
