package generate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/viant/datly/spec"
)

const veltyHandlerPackage = "github.com/viant/datly/runtime/handler/velty"

// VeltyHandlerAsset is an explicitly accepted Velty template and its managed
// package destinations. It carries no compiled program or runtime capability.
type VeltyHandlerAsset struct {
	Template            string
	Factory             string
	GoDestination       string
	ResourceDestination string
}

// Clone returns an isolated value copy of the handler asset.
func (a *VeltyHandlerAsset) Clone() *VeltyHandlerAsset {
	if a == nil {
		return nil
	}
	result := *a
	return &result
}

// VeltyHandlerPlan is the validated template/factory pair emitted with a
// generated package.
type VeltyHandlerPlan struct {
	Template            string
	Factory             string
	GoDestination       string
	ResourceDestination string
}

func resolveVeltyHandler(plan *Plan, asset *VeltyHandlerAsset) error {
	if asset == nil {
		return nil
	}
	if strings.TrimSpace(asset.Template) == "" {
		return fmt.Errorf("Velty handler template is required")
	}
	if !isDirectNamedType(plan.Input.Type) || !isDirectNamedType(plan.Output.Type) {
		return fmt.Errorf("Velty handler requires direct named struct contracts, got %s -> %s", plan.Input.Type, plan.Output.Type)
	}
	factory := strings.TrimSpace(asset.Factory)
	if factory == "" {
		factory = "New" + upperCamel(plan.ComponentName) + "Handler"
	}
	if !token.IsIdentifier(factory) || !token.IsExported(factory) {
		return fmt.Errorf("Velty handler factory %q must be an exported Go identifier", factory)
	}
	goDestination := strings.TrimSpace(asset.GoDestination)
	if goDestination == "" {
		goDestination = lowerSnake(plan.ComponentName) + "_velty.go"
	}
	goDestination, err := managedRelativePath(goDestination)
	if err != nil {
		return fmt.Errorf("Velty handler Go destination: %w", err)
	}
	if filepath.Base(goDestination) != goDestination || filepath.Ext(goDestination) != ".go" {
		return fmt.Errorf("Velty handler Go destination %q must be a package-local .go file", goDestination)
	}
	resourceDestination := strings.TrimSpace(asset.ResourceDestination)
	if resourceDestination == "" {
		resourceDestination = filepath.Join(lowerSnake(plan.ComponentName), "handler.velty")
	}
	resourceDestination, err = managedRelativePath(resourceDestination)
	if err != nil {
		return fmt.Errorf("Velty handler resource destination: %w", err)
	}
	switch strings.ToLower(filepath.Ext(resourceDestination)) {
	case ".sql", ".velty":
	default:
		return fmt.Errorf("Velty handler resource destination %q must use .sql or .velty", resourceDestination)
	}
	if filepath.Clean(resourceDestination) == goDestination {
		return fmt.Errorf("Velty handler Go and resource destinations are shared")
	}
	if plan.Handler != "" && plan.Handler != factory {
		return fmt.Errorf("route handler %q conflicts with Velty handler factory %q", plan.Handler, factory)
	}
	variable := lowerInitial(factory) + "Template"
	for _, item := range plan.contractImports() {
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		if alias == "error" {
			return fmt.Errorf("Velty handler contract import alias %q shadows the predeclared error type", alias)
		}
		if alias == factory || alias == variable {
			return fmt.Errorf("Velty handler identifier %q conflicts with contract import alias", alias)
		}
	}
	plan.Handler = factory
	plan.VeltyHandler = &VeltyHandlerPlan{
		Template: asset.Template, Factory: factory, GoDestination: goDestination,
		ResourceDestination: resourceDestination,
	}
	return nil
}

func isDirectNamedType(expression string) bool {
	parsed, err := parser.ParseExpr(strings.TrimSpace(expression))
	if err != nil {
		return false
	}
	switch parsed.(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return true
	default:
		return false
	}
}

func veltyHandlerFileText(packageName string, plan *Plan) (string, error) {
	if plan == nil || plan.VeltyHandler == nil {
		return "", fmt.Errorf("validated Velty handler plan is required")
	}
	handler := plan.VeltyHandler
	imports := plan.contractImports()
	variable := lowerInitial(handler.Factory) + "Template"
	alias := ""
	runtimeImported := false
	for _, item := range imports {
		if strings.TrimSpace(item.Package) != veltyHandlerPackage {
			continue
		}
		alias = strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		runtimeImported = true
		break
	}
	if alias == "" {
		alias = availableImportAlias(imports, "veltyhandler", handler.Factory, variable)
	}
	var builder strings.Builder
	builder.WriteString("package ")
	builder.WriteString(packageName)
	builder.WriteString("\n\nimport (\n\t_ \"embed\"\n")
	if !runtimeImported {
		builder.WriteString("\t")
		builder.WriteString(alias)
		builder.WriteString(" ")
		builder.WriteString(fmt.Sprintf("%q", veltyHandlerPackage))
		builder.WriteString("\n")
	}
	for _, item := range imports {
		builder.WriteString("\t")
		if item.Alias != "" {
			builder.WriteString(item.Alias)
			builder.WriteString(" ")
		}
		builder.WriteString(fmt.Sprintf("%q", item.Package))
		builder.WriteString("\n")
	}
	builder.WriteString(")\n\n")
	builder.WriteString("//go:embed ")
	builder.WriteString(strconv.Quote(filepath.ToSlash(handler.ResourceDestination)))
	builder.WriteString("\nvar ")
	builder.WriteString(variable)
	builder.WriteString(" string\n\n")
	builder.WriteString("// ")
	builder.WriteString(handler.Factory)
	builder.WriteString(" creates the generated typed Velty handler.\n")
	builder.WriteString("func ")
	builder.WriteString(handler.Factory)
	builder.WriteString("() (*")
	builder.WriteString(alias)
	builder.WriteString(".Handler[")
	builder.WriteString(plan.Input.Type)
	builder.WriteString(", ")
	builder.WriteString(plan.Output.Type)
	builder.WriteString("], error) {\n\t")
	if plan.EntitySupport != nil {
		builder.WriteString("instance, err := ")
	} else {
		builder.WriteString("return ")
	}
	builder.WriteString(alias)
	builder.WriteString(".New[")
	builder.WriteString(plan.Input.Type)
	builder.WriteString(", ")
	builder.WriteString(plan.Output.Type)
	builder.WriteString("](")
	builder.WriteString(alias)
	builder.WriteString(".Config{Template: ")
	builder.WriteString(variable)
	builder.WriteString("})\n")
	if plan.EntitySupport != nil {
		builder.WriteString("\tif err != nil { return nil, err }\n\treturn instance.WithInputCapture(" + plan.EntitySupport.CaptureFunction + "), nil\n")
	}
	builder.WriteString("}\n")
	return builder.String(), nil
}

func availableImportAlias(imports []spec.ImportSpec, preferred string, reserved ...string) string {
	used := map[string]bool{}
	for _, item := range reserved {
		used[item] = true
	}
	for _, item := range imports {
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		used[alias] = true
	}
	if !used[preferred] {
		return preferred
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s%d", preferred, suffix)
		if !used[candidate] {
			return candidate
		}
	}
}

func lowerInitial(value string) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) == 0 {
		return "handler"
	}
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}
