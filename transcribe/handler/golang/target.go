// Package golang lowers semantic handler transcription plans into Go AST.
package golang

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

const handlerPackage = "github.com/viant/xdatly/handler"

// Config contains target syntax and package choices only.
type Config struct {
	Package string
	// PackagePath is the canonical target import path when lowering hook type references.
	PackagePath string
	Factory     string
	Handler     string
	InputType   string
	OutputType  string
	Records     []RecordType
	Imports     []spec.ImportSpec
}

// RecordType supplies target-language types after final contract and
// view authority resolution. Semantic plans remain target-neutral.
type RecordType struct {
	Identity string
	Path     plan.FieldPath
	Value    string
	Current  string
	// CurrentValue is the input-bound read carrier; Current names its row collection.
	// Empty means the existing direct-current collection contract.
	CurrentValue string
}

// Asset is the isolated Go AST product of one target lowering pass.
type Asset struct {
	Factory  string
	Handler  string
	File     *ast.File
	Entities *EntityAsset
}

// Source formats the target AST using the standard Go formatter.
func (a *Asset) Source() ([]byte, error) {
	if a == nil || a.File == nil {
		return nil, fmt.Errorf("generated Go handler AST is required")
	}
	source, err := (xshape.SourceParser{}).FormatFile(a.File)
	if err != nil {
		return nil, fmt.Errorf("format generated Go handler: %w", err)
	}
	return source, nil
}

type lowerer struct {
	plan           *plan.Plan
	config         Config
	records        []*recordLowering
	recordByPlan   map[*plan.RecordPlan]*recordLowering
	relationHooks  []*relationHookLowering
	hookByRelation map[*plan.RelationPlan]*relationHookLowering
	factory        string
	handler        string
	dependencies   string
	contextAlias   string
	fmtAlias       string
	handlerAlias   string
	importsByPath  map[string]string
	pathsByAlias   map[string]string
	usedImports    map[string]bool
	reservedNames  map[string]bool
}

// Lower builds one recursive generated write handler as Go AST.
func Lower(value *plan.Plan, config Config) (*Asset, error) {
	for _, record := range config.Records {
		if record.CurrentValue != "" {
			return nil, fmt.Errorf("generated Go write handler requires direct current rows; carrier %s requires the metadata-aware mutation program", record.CurrentValue)
		}
	}
	lowered := &lowerer{plan: value, config: config}
	if err := lowered.prepare(); err != nil {
		return nil, err
	}
	entities, err := lowered.entitySupport()
	if err != nil {
		return nil, err
	}
	file, err := lowered.file()
	if err != nil {
		return nil, err
	}
	if entities != nil {
		file.Decls = append(file.Decls, &ast.FuncDecl{Name: ast.NewIdent("CaptureInput"), Recv: &ast.FieldList{List: []*ast.Field{namedField("h", &ast.StarExpr{X: ast.NewIdent(lowered.handler)})}}, Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{namedField("ctx", selectExpr(ast.NewIdent(lowered.contextAlias), "Context")), namedField("input", &ast.StarExpr{X: parseExpr(lowered.config.InputType)})}}, Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("any")}, {Type: ast.NewIdent("error")}}}}, Body: &ast.BlockStmt{List: []ast.Stmt{returnStmt(callExpr(ast.NewIdent(entities.CaptureFunction), ast.NewIdent("ctx"), ast.NewIdent("input")))}}})
	}
	return &Asset{Factory: lowered.factory, Handler: lowered.handler, File: file, Entities: entities}, nil
}

func (l *lowerer) prepare() error {
	if l.plan == nil || l.plan.Root == nil {
		return fmt.Errorf("Go lowering requires a root write plan")
	}
	switch l.plan.Operation {
	case plan.OperationPost, plan.OperationPut, plan.OperationPatch:
	default:
		return fmt.Errorf("unsupported generated Go write operation %q", l.plan.Operation)
	}
	if l.plan.Operation == plan.OperationPatch && !l.plan.Root.Auxiliary && l.plan.Root.Current == nil {
		return fmt.Errorf("Go PATCH lowering requires a planned current input")
	}
	packageName := strings.TrimSpace(l.config.Package)
	if !token.IsIdentifier(packageName) || token.Lookup(packageName).IsKeyword() {
		return fmt.Errorf("generated Go handler package %q is invalid", l.config.Package)
	}
	l.config.Package = packageName
	l.factory = strings.TrimSpace(l.config.Factory)
	if !token.IsIdentifier(l.factory) || !token.IsExported(l.factory) {
		return fmt.Errorf("generated Go handler factory %q must be an exported identifier", l.config.Factory)
	}
	l.handler = strings.TrimSpace(l.config.Handler)
	if l.handler == "" {
		base := strings.TrimPrefix(l.factory, "New")
		l.handler = lowerInitial(base) + "Contract"
	}
	if !token.IsIdentifier(l.handler) || token.Lookup(l.handler).IsKeyword() {
		return fmt.Errorf("generated Go handler type %q is invalid", l.handler)
	}
	l.dependencies = l.handler + "Dependencies"
	l.reservedNames = map[string]bool{}
	for _, name := range []string{l.factory, l.handler, l.dependencies} {
		if err := l.reserveDeclaration(name); err != nil {
			return err
		}
	}
	if strings.TrimSpace(l.config.InputType) == "" || strings.TrimSpace(l.config.OutputType) == "" {
		return fmt.Errorf("generated Go handler input and output contract types are required")
	}
	if _, err := parser.ParseExpr(l.config.InputType); err != nil {
		return fmt.Errorf("parse generated Go input contract %q: %w", l.config.InputType, err)
	}
	if _, err := parser.ParseExpr(l.config.OutputType); err != nil {
		return fmt.Errorf("parse generated Go output contract %q: %w", l.config.OutputType, err)
	}
	l.importsByPath = map[string]string{}
	l.pathsByAlias = map[string]string{}
	l.usedImports = map[string]bool{}
	if err := l.indexImports(); err != nil {
		return err
	}
	l.contextAlias = l.availableAlias("context")
	l.fmtAlias = l.availableAlias("fmt")
	l.handlerAlias = l.availableAlias("xhandler")
	if err := l.compileRecords(); err != nil {
		return err
	}
	for _, expression := range []string{l.config.InputType, l.config.OutputType} {
		if err := l.markExpressionImports(expression); err != nil {
			return err
		}
	}
	// Direct handlers name current-row types only in PATCH key helpers.
	// Mutation companion files collect imports from their emitted AST instead.
	if l.plan.Operation == plan.OperationPatch {
		for _, record := range l.records {
			if record.plan.Current == nil {
				continue
			}
			for _, expression := range []string{record.value.base, record.current.base} {
				if err := l.markExpressionImports(expression); err != nil {
					return err
				}
			}
		}
	}
	for _, hook := range l.relationHooks {
		if err := l.markExpressionImports(hook.holderType); err != nil {
			return err
		}
	}
	return nil
}

func samePath(left, right plan.FieldPath) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsAction(actions []plan.Action, candidate plan.Action) bool {
	for _, action := range actions {
		if action == candidate {
			return true
		}
	}
	return false
}

func (l *lowerer) indexImports() error {
	for _, item := range l.config.Imports {
		packagePath := strings.TrimSpace(item.Package)
		if packagePath == "" {
			return fmt.Errorf("generated Go handler import path is required")
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(packagePath)
		}
		if !token.IsIdentifier(alias) || token.Lookup(alias).IsKeyword() {
			return fmt.Errorf("generated Go handler import alias %q is invalid", alias)
		}
		if l.reservedNames[alias] {
			return fmt.Errorf("generated Go handler declaration %q conflicts with import %q", alias, packagePath)
		}
		if previous := l.pathsByAlias[alias]; previous != "" && previous != packagePath {
			return fmt.Errorf("generated Go handler import alias %q is ambiguous", alias)
		}
		if previous := l.importsByPath[packagePath]; previous != "" && previous != alias {
			return fmt.Errorf("generated Go handler import %q has conflicting aliases", packagePath)
		}
		l.pathsByAlias[alias] = packagePath
		l.importsByPath[packagePath] = alias
	}
	return nil
}

func (l *lowerer) availableAlias(preferred string) string {
	if l.pathsByAlias[preferred] == "" && !l.reservedNames[preferred] {
		l.pathsByAlias[preferred] = "#generated"
		return preferred
	}
	for suffix := 2; ; suffix++ {
		candidate := preferred + strconv.Itoa(suffix)
		if l.pathsByAlias[candidate] == "" && !l.reservedNames[candidate] {
			l.pathsByAlias[candidate] = "#generated"
			return candidate
		}
	}
}

func (l *lowerer) reserveDeclaration(name string) error {
	if l.reservedNames[name] {
		return fmt.Errorf("generated Go handler declaration %q is duplicated", name)
	}
	if packagePath := l.pathsByAlias[name]; packagePath != "" && packagePath != "#generated" {
		return fmt.Errorf("generated Go handler declaration %q conflicts with import %q", name, packagePath)
	}
	l.reservedNames[name] = true
	return nil
}

func (l *lowerer) markExpressionImports(source string) error {
	expression, err := parser.ParseExpr(strings.TrimSpace(source))
	if err != nil {
		return err
	}
	var missing string
	ast.Inspect(expression, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		alias, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		packagePath := l.pathsByAlias[alias.Name]
		if packagePath == "" || packagePath == "#generated" {
			missing = alias.Name
			return false
		}
		l.usedImports[packagePath] = true
		return true
	})
	if missing != "" {
		return fmt.Errorf("generated Go type expression references unknown import alias %q", missing)
	}
	return nil
}

func lowerInitial(value string) string {
	if value == "" {
		return "handler"
	}
	runes := []rune(value)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func capabilityTag(key xhandler.ValueKey) string {
	return fmt.Sprintf(`parameter:",kind=%s"`, key)
}
