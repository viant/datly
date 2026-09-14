package testharness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/viant/datly"

type packageBoundary struct {
	owner     string
	recursive bool
	forbidden []string
}

func TestFinalPackageArchitecture(t *testing.T) {
	root := repositoryRoot(t)
	assertRemovedPackages(t, root)
	assertRemovedFiles(t, root)
	assertGroupingDirectories(t, root)
	packages := loadProductionPackages(t, root)
	assertPackageComments(t, packages)
	assertPackageBoundaries(t, packages)
	assertHandlerTestBoundaries(t, root)
	assertSpecMetadataOwnership(t, root)
	assertResolvedViewOwnership(t, root)
	assertQuerySelectorNaming(t, root)
	assertHandlerDataCapabilityOwnership(t, root)
	assertCohesionOwnership(t, root)
	assertSQLParameterOwnership(t, root)
}

func assertHandlerDataCapabilityOwnership(t *testing.T, root string) {
	t.Helper()
	forbidden := map[string]bool{"DataScope": true, "CommitOwned": true, "RollbackOwned": true}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && skipArchitectureDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && forbidden[identifier.Name] {
				t.Errorf("%s restores engine transaction-ownership surface %s", path, identifier.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertQuerySelectorNaming(t *testing.T, root string) {
	t.Helper()
	forbidden := map[string]bool{
		"QuerySelectorControl":    true,
		"QuerySelectorFields":     true,
		"QuerySelectorOrderBy":    true,
		"QuerySelectorOffset":     true,
		"QuerySelectorLimit":      true,
		"QuerySelectorPage":       true,
		"QuerySelectorCriteria":   true,
		"SelectorControlPlan":     true,
		"SelectorControls":        true,
		"CompileSelectorControls": true,
		"compileSelectorControls": true,
		"applySelectorControl":    true,
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && skipArchitectureDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && forbidden[identifier.Name] {
				t.Errorf("%s restores obsolete query-selector identifier %s", path, identifier.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertSQLParameterOwnership(t *testing.T, root string) {
	t.Helper()
	forbidden := map[string]bool{
		"InputPlan": true, "InputBinding": true, "SQLInputPlan": true,
		"NewInputPlan": true, "WithBuilderInputPlan": true, "buildSQLInputPlan": true,
		"InputProjection": true,
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && skipArchitectureDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && forbidden[identifier.Name] {
				t.Errorf("%s restores Datly-owned SQL input projection identifier %s", path, identifier.Name)
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			typeSpec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return false
			}
			for _, field := range structType.Fields.List {
				pointer, ok := field.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				selector, ok := pointer.X.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Projection" {
					continue
				}
				owner, ok := selector.X.(*ast.Ident)
				if !ok || owner.Name != "bindly" {
					continue
				}
				canonicalOwner := filepath.Join(root, "runtime", "registry", "input_contract.go")
				if filepath.Clean(path) != canonicalOwner || typeSpec.Name.Name != "InputContract" {
					t.Errorf("%s type %s retains Bindly projection outside canonical InputContract", path, typeSpec.Name.Name)
				}
			}
			return false
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertCohesionOwnership(t *testing.T, root string) {
	t.Helper()
	removed := map[string]bool{
		"bootstrap.descriptorContractIdentity":               true,
		"bootstrap.packageComponentName":                     true,
		"bootstrap.samePackageComponentMetadata":             true,
		"transcribe.packageComponentSourceName":              true,
		"transcribe.mergeProjectManifest":                    true,
		"transcribe.replaceProjectComponent":                 true,
		"transcribe.finalizeProjectManifest":                 true,
		"transcribe.cloneProjectManifest":                    true,
		"transcribe.projectCompiledResult":                   true,
		"transcribe.canonicalProjectComponentKey":            true,
		"transcribe.projectComponentEntry":                   true,
		"transcribe.projectManifestComponents":               true,
		"transcribe/generate.resolvePlan":                    true,
		"transcribe/generate.resolveViewPlans":               true,
		"transcribe/generate.bindIndependentViewFields":      true,
		"transcribe/generate.concretizeFields":               true,
		"transcribe/generate.concretizeArrayAggHelperFields": true,
		"transcribe/generate.concretizeHelperFields":         true,
		"transcribe/generate.applySetMarkerViews":            true,
		"transcribe/generate.validateHelperFieldNames":       true,
		"transcribe/generate.validateConcreteHelperFields":   true,
		"transcribe/generate.validateGeneratedNames":         true,
		"transcribe/generate.validateGeneratedDestinations":  true,
		"report.derivedIdentity":                             true,
		"report.derivedComponent":                            true,
		"report.indexSource":                                 true,
		"report.reportEnabled":                               true,
		"report.groupable":                                   true,
		"report.eligibleRoutes":                              true,
		"report.routeIdentity":                               true,
		"report.componentIdentity":                           true,
		"report.reportPackage":                               true,
		"report.resolutionContext":                           true,
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && skipArchitectureDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil {
				continue
			}
			identity := relative + "." + function.Name.Name
			if removed[identity] {
				t.Errorf("%s restores package-level cohesion helper %s", path, identity)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertSpecMetadataOwnership(t *testing.T, root string) {
	t.Helper()
	types := map[string]*ast.StructType{}
	declarations := map[string]bool{}
	specDir := filepath.Join(root, "spec")
	entries, err := os.ReadDir(specDir)
	if err != nil {
		t.Fatalf("inspect spec package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), filepath.Join(specDir, entry.Name()), nil, 0)
		if parseErr != nil {
			t.Fatalf("parse spec metadata %q: %v", entry.Name(), parseErr)
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			switch generic.Tok {
			case token.TYPE:
				for _, item := range generic.Specs {
					typeSpec, ok := item.(*ast.TypeSpec)
					if !ok {
						continue
					}
					declarations[typeSpec.Name.Name] = true
					if structType, ok := typeSpec.Type.(*ast.StructType); ok {
						types[typeSpec.Name.Name] = structType
					}
				}
			case token.CONST:
				for _, item := range generic.Specs {
					valueSpec, ok := item.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range valueSpec.Names {
						declarations[name.Name] = true
					}
				}
			}
		}
	}
	if structHasField(types["Settings"], "MCP") {
		t.Error("spec.Settings must not own MCP exposure metadata")
	}
	if !structHasField(types["Route"], "MCP") {
		t.Error("spec.Route must own MCP exposure metadata")
	}
	if !structHasField(types["Settings"], "Generation") {
		t.Error("spec.Settings must reference canonical generation settings")
	}
	for _, field := range []string{"TemplateType", "Meta", "Dest", "InputDest", "OutputDest", "RouterDest"} {
		if structHasField(types["Settings"], field) {
			t.Errorf("spec.Settings must not own flat generation field %q", field)
		}
	}
	for _, field := range []string{"Template", "DescriptionResource", "ViewFile", "InputFile", "OutputFile", "RouterFile"} {
		if !structHasField(types["GenerationSettings"], field) {
			t.Errorf("spec.GenerationSettings must own %q", field)
		}
	}
	if !structHasField(types["ReportSettings"], "LinkedInputType") {
		t.Error("spec.ReportSettings must identify its optional linked input type explicitly")
	}
	if !structHasField(types["ReportSettings"], "InputLayout") {
		t.Error("spec.ReportSettings must reference optional linked-input layout metadata")
	}
	for _, field := range []string{"Dimensions", "Measures", "Filters", "OrderBy", "Limit", "Offset"} {
		if structHasField(types["ReportSettings"], field) {
			t.Errorf("spec.ReportSettings must not own flat input-layout field %q", field)
		}
		if !structHasField(types["ReportInputLayout"], field) {
			t.Errorf("spec.ReportInputLayout must own %q", field)
		}
	}
	if structHasField(types["ReportSettings"], "Input") {
		t.Error("spec.ReportSettings must not restore ambiguous Input metadata")
	}
	if structHasField(types["TypeRef"], "Capability") {
		t.Error("spec.TypeRef must not duplicate interface-owned lifecycle capability")
	}
	if !structHasField(types["Snapshot"], "Diagnostics") {
		t.Error("spec.Snapshot must expose diagnostics without an abbreviated field name")
	}
	if structHasField(types["Snapshot"], "Diags") {
		t.Error("spec.Snapshot must not restore the abbreviated Diags field")
	}
	if !declarations["Parameter"] || declarations["Param"] {
		t.Error("spec must expose Parameter without the abbreviated Param type")
	}
	if !structHasField(types["Component"], "Parameters") {
		t.Error("spec.Component must expose the full Parameters field name")
	}
	if structHasField(types["Component"], "Params") {
		t.Error("spec.Component must not restore the abbreviated Params field")
	}
	if !structHasField(types["Predicate"], "ApplyWhenAbsent") {
		t.Error("spec.Predicate must name its absent-input behavior ApplyWhenAbsent")
	}
	if structHasField(types["Predicate"], "Ensure") {
		t.Error("spec.Predicate must not restore the ambiguous Ensure field")
	}
	if !structHasField(types["QuerySelectorBinding"], "Property") {
		t.Error("spec.QuerySelectorBinding must identify the selected view property")
	}
	if structHasField(types["QuerySelectorBinding"], "Control") {
		t.Error("spec.QuerySelectorBinding must not restore ambiguous control vocabulary")
	}
	for _, declaration := range []string{
		"QuerySelectorControl", "QuerySelectorFields", "QuerySelectorOrderBy", "QuerySelectorOffset",
		"QuerySelectorLimit", "QuerySelectorPage", "QuerySelectorCriteria",
	} {
		if declarations[declaration] {
			t.Errorf("spec must not restore obsolete query-selector declaration %q", declaration)
		}
	}
	for _, declaration := range []string{"TypeCapability", "TypeCapabilityHookCapable", "TypeCapabilityDataOnly"} {
		if declarations[declaration] {
			t.Errorf("spec must not restore unused declarative type capability %q", declaration)
		}
	}
	for _, declaration := range []string{
		"KindProject", "KindModule", "KindParam", "KindType", "KindRoute", "KindHandler", "KindConnector",
	} {
		if declarations[declaration] {
			t.Errorf("spec must not restore unused generic key kind %q", declaration)
		}
	}
}

func assertResolvedViewOwnership(t *testing.T, root string) {
	t.Helper()
	types := map[string]*ast.StructType{}
	declarations := map[string]bool{}
	dataDir := filepath.Join(root, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("inspect data package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), filepath.Join(dataDir, entry.Name()), nil, 0)
		if parseErr != nil {
			t.Fatalf("parse data metadata %q: %v", entry.Name(), parseErr)
		}
		for _, declaration := range file.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, item := range generic.Specs {
				switch actual := item.(type) {
				case *ast.TypeSpec:
					declarations[actual.Name.Name] = true
					if structType, ok := actual.Type.(*ast.StructType); ok {
						types[actual.Name.Name] = structType
					}
				case *ast.ValueSpec:
					for _, name := range actual.Names {
						declarations[name.Name] = true
					}
				}
			}
		}
	}
	for _, declaration := range []string{"RelationKind", "RelationKindSubview", "RelationKindDerived", "SelfReference", "CardinalityOne", "CardinalityMany"} {
		if declarations[declaration] {
			t.Errorf("data must reuse spec-owned view declaration %q", declaration)
		}
	}
	for _, field := range []string{"Description", "Key", "Name", "Namespace", "Cardinality", "AllowNulls", "Groupable", "Source", "Selector", "Partitioning", "SelfReference", "BatchSize", "BatchConcurrency", "PublishParent", "RelationalConcurrency"} {
		if structHasField(types["View"], field) {
			t.Errorf("data.View must reuse spec.View metadata instead of redeclaring %s", field)
		}
	}
	if actual := structFieldType(types["View"], "Spec"); actual != "spec.View" {
		t.Errorf("data.View.Spec type = %q, want spec.View snapshot", actual)
	}
	for _, field := range []string{"Target", "OwnerColumn", "TargetColumn", "IncludeColumn"} {
		if structHasField(types["Relation"], field) {
			t.Errorf("data.Relation must not retain write-only field %q", field)
		}
	}
	for structName, fields := range map[string]map[string]string{
		"Relation": {"Kind": "spec.RelationKind", "Cardinality": "spec.Cardinality"},
	} {
		for field, expected := range fields {
			if actual := structFieldType(types[structName], field); actual != expected {
				t.Errorf("data.%s.%s type = %q, want %q", structName, field, actual, expected)
			}
		}
	}
}

func structHasField(structType *ast.StructType, name string) bool {
	if structType == nil || structType.Fields == nil {
		return false
	}
	for _, field := range structType.Fields.List {
		for _, fieldName := range field.Names {
			if fieldName.Name == name {
				return true
			}
		}
	}
	return false
}

func structFieldType(structType *ast.StructType, name string) string {
	if structType == nil || structType.Fields == nil {
		return ""
	}
	for _, field := range structType.Fields.List {
		for _, fieldName := range field.Names {
			if fieldName.Name == name {
				return expressionTypeName(field.Type)
			}
		}
	}
	return ""
}

func expressionTypeName(expression ast.Expr) string {
	switch actual := expression.(type) {
	case *ast.Ident:
		return actual.Name
	case *ast.SelectorExpr:
		owner, _ := actual.X.(*ast.Ident)
		if owner == nil {
			return actual.Sel.Name
		}
		return owner.Name + "." + actual.Sel.Name
	case *ast.StarExpr:
		return "*" + expressionTypeName(actual.X)
	}
	return ""
}

func assertGroupingDirectories(t *testing.T, root string) {
	t.Helper()
	for _, relative := range []string{"transcribe/handler"} {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("inspect grouping directory %q: %v", relative, err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
				t.Errorf("grouping directory %q contains root Go file %q", relative, entry.Name())
			}
		}
	}
}

func assertRemovedPackages(t *testing.T, root string) {
	t.Helper()
	removed := []string{
		"componenttag", "data/columntag", "gen", "goshape", "materialize", "plan",
		"internal/declsql", "internal/pkgpath", "internal/responsebind", "internal/rowbind", "internal/sqlbind", "internal/sqltext",
		"mcp/bind",
		"runtime/legacy", "runtime/mutation", "runtime/patch", "runtime/patchshape",
		"runtime/readflow", "runtime/routeexec",
	}
	for _, relative := range removed {
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(relative))); err == nil && info.IsDir() {
			t.Errorf("removed package %q has reappeared", relative)
		} else if err != nil && !os.IsNotExist(err) {
			t.Errorf("inspect removed package %q: %v", relative, err)
		}
	}
}

func assertRemovedFiles(t *testing.T, root string) {
	t.Helper()
	for _, relative := range []string{
		"sql/builder/input_plan.go",
		"sql/builder/placeholder.go",
		"transcribe/handler/compiler.go",
		"transcribe/handler/current.go",
		"transcribe/handler/model.go",
		"transcribe/handler/relations.go",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(relative))); err == nil {
			t.Errorf("removed file %q has reappeared", relative)
		} else if !os.IsNotExist(err) {
			t.Errorf("inspect removed file %q: %v", relative, err)
		}
	}
}

type productionPackage struct {
	name    string
	hasDoc  bool
	imports map[string]bool
}

func loadProductionPackages(t *testing.T, root string) map[string]*productionPackage {
	t.Helper()
	result := map[string]*productionPackage{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && skipArchitectureDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// Application examples may demonstrate a domain; framework packages must
		// not encode those application types, tables, routes or business rules.
		if relative != "example" && !strings.HasPrefix(relative, "example/") {
			lower := strings.ToLower(string(source))
			for _, applicationTerm := range []string{"ci_audience", "ci_ads", "audiencepatch", "audienceinput", "audienceoutput", "advertiser", "ad_order", "bid_price", "freq_capping"} {
				if strings.Contains(lower, applicationTerm) {
					t.Errorf("%s leaks application-specific term %q into the framework", path, applicationTerm)
				}
			}
		}
		if strings.Contains(string(source), "reflect.StructOf(") {
			t.Errorf("%s constructs a reflected struct outside viant/x", path)
		}
		pkg := result[relative]
		if pkg == nil {
			pkg = &productionPackage{name: file.Name.Name, imports: map[string]bool{}}
			result[relative] = pkg
		}
		if file.Doc != nil && strings.HasPrefix(strings.TrimSpace(file.Doc.Text()), "Package "+pkg.name+" ") {
			pkg.hasDoc = true
		}
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			pkg.imports[value] = true
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if isUntypedMapRowSlice(node) {
				t.Errorf("%s contains forbidden []map[string]any row shape", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func skipArchitectureDirectory(name string) bool {
	return strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor"
}

func assertPackageComments(t *testing.T, packages map[string]*productionPackage) {
	t.Helper()
	for relative, pkg := range packages {
		if !pkg.hasDoc {
			t.Errorf("package %q (%s) has no canonical package comment", relative, pkg.name)
		}
	}
}

func assertPackageBoundaries(t *testing.T, packages map[string]*productionPackage) {
	t.Helper()
	rules := []packageBoundary{
		{owner: "data", forbidden: []string{"database/sql", modulePath + "/runtime", "github.com/viant/sqlx", "github.com/viant/xunsafe", "github.com/viant/xdatly/logger"}},
		{owner: "transcribe/generate", forbidden: []string{modulePath + "/runtime", modulePath + "/transcribe/dql", modulePath + "/transcribe/handler"}},
		{owner: "runtime", recursive: true, forbidden: []string{modulePath + "/sql", modulePath + "/mcp", modulePath + "/report"}},
		{owner: "runtime/registry", forbidden: []string{"database/sql", "net/http", "go/ast", "go/parser", modulePath + "/runtime/handler/compiler", modulePath + "/runtime/predicate", modulePath + "/sql", "github.com/viant/sqlx"}},
		{owner: "runtime/route", forbidden: []string{"database/sql", "net/http", modulePath + "/sql", "github.com/viant/bindly"}},
		{owner: "sql", recursive: true, forbidden: []string{modulePath + "/mcp", modulePath + "/report"}},
		{owner: "sql/fragment", forbidden: []string{"database/sql", modulePath + "/runtime", modulePath + "/sql/reader", modulePath + "/sql/builder", modulePath + "/sql/template", "github.com/viant/sqlx"}},
		{owner: "bootstrap", recursive: true, forbidden: []string{"go/ast", "go/parser", modulePath + "/mcp", modulePath + "/report"}},
		{owner: "spec", recursive: true, forbidden: []string{modulePath + "/mcp", modulePath + "/report"}},
		{owner: "mcp", recursive: true, forbidden: []string{modulePath + "/bootstrap", modulePath + "/gateway", modulePath + "/report", modulePath + "/runtime/handler/engine", modulePath + "/sql"}},
		{owner: "mcp/tool", forbidden: []string{"go/ast", "go/parser"}},
		{owner: "mcp", forbidden: []string{modulePath + "/mcp/server", "github.com/viant/mcp"}},
		{owner: "mcp/server", forbidden: []string{modulePath + "/bootstrap", modulePath + "/gateway", modulePath + "/report", modulePath + "/runtime", modulePath + "/sql"}},
		{owner: "mcp/resource", forbidden: []string{modulePath + "/bootstrap", modulePath + "/gateway", modulePath + "/report", modulePath + "/runtime/handler", modulePath + "/sql"}},
		{owner: "mcp/invocation", forbidden: []string{modulePath + "/bootstrap", modulePath + "/gateway", modulePath + "/report", modulePath + "/runtime", modulePath + "/sql"}},
		{owner: "report", recursive: true, forbidden: []string{"database/sql", "go/ast", "go/parser", modulePath + "/gateway", modulePath + "/mcp", modulePath + "/sql"}},
		{owner: "typecatalog", recursive: true, forbidden: []string{"go/ast", "go/parser"}},
		{owner: "sql/reader", recursive: true, forbidden: []string{"go/ast", "go/parser", "net/http", modulePath + "/gateway", modulePath + "/runtime"}},
		{owner: "transcribe/handler", recursive: true, forbidden: []string{"database/sql", modulePath + "/runtime", modulePath + "/sql", modulePath + "/transcribe/dql", modulePath + "/transcribe/generate", "github.com/viant/bindly", "github.com/viant/sqlx"}},
		{owner: "transcribe/handler/ast", forbidden: []string{modulePath + "/transcribe/handler/compiler", modulePath + "/transcribe/handler/golang", modulePath + "/transcribe/handler/velty"}},
		{owner: "transcribe/handler/compiler", forbidden: []string{modulePath + "/transcribe/handler/golang", modulePath + "/transcribe/handler/velty"}},
		{owner: "transcribe/handler/golang", forbidden: []string{modulePath + "/transcribe/handler/compiler"}},
		{owner: "transcribe/handler/velty", forbidden: []string{modulePath + "/transcribe/handler/compiler"}},
	}
	for relative, pkg := range packages {
		for imported := range pkg.imports {
			// Composite fragments use only native column mapping, value
			// conversion and dialect metadata, never SQL execution services.
			if relative == "sql/fragment" && (imported == "database/sql/driver" || imported == "github.com/viant/sqlx/io" || imported == "github.com/viant/sqlx/option" || imported == "github.com/viant/sqlx/metadata/info") {
				continue
			}
			// Only the pure fragment/argument collector is shared with predicates;
			// runtime retains no dependency on SQL execution or reader owners.
			if relative == "runtime/predicate/velty" && imported == modulePath+"/sql/fragment" {
				continue
			}
			// The release now owns the canonical module path. Keep the original
			// implementation boundary at its obsolete package owners, rather
			// than rejecting every import from this module.
			if imported == modulePath || matchesImport(imported, []string{
				modulePath + "/repository", modulePath + "/service", modulePath + "/view",
				modulePath + "/shared", modulePath + "/logger", modulePath + "/warmup",
				modulePath + "/internal/translator",
			}) {
				t.Errorf("package %q imports obsolete original implementation package %q", relative, imported)
			}
			for _, rule := range rules {
				if ownsPackage(rule, relative) && matchesImport(imported, rule.forbidden) {
					t.Errorf("package %q imports forbidden owner %q", relative, imported)
				}
			}
		}
	}
}

func assertHandlerTestBoundaries(t *testing.T, root string) {
	t.Helper()
	rules := []packageBoundary{
		{owner: "transcribe/handler/ast", forbidden: []string{modulePath + "/transcribe/handler/compiler", modulePath + "/transcribe/handler/golang", modulePath + "/transcribe/handler/velty"}},
		{owner: "transcribe/handler/compiler", forbidden: []string{modulePath + "/transcribe/generate", modulePath + "/transcribe/handler/golang", modulePath + "/transcribe/handler/velty"}},
		{owner: "transcribe/handler/golang", forbidden: []string{modulePath + "/transcribe/handler/compiler"}},
		{owner: "transcribe/handler/velty", forbidden: []string{modulePath + "/transcribe/handler/compiler"}},
	}
	err := filepath.WalkDir(filepath.Join(root, "transcribe", "handler"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		for _, imported := range file.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			for _, rule := range rules {
				if ownsPackage(rule, relative) && matchesImport(value, rule.forbidden) {
					t.Errorf("test package %q imports forbidden owner %q", relative, value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func ownsPackage(rule packageBoundary, relative string) bool {
	return relative == rule.owner || rule.recursive && strings.HasPrefix(relative, rule.owner+"/")
}

func matchesImport(imported string, forbidden []string) bool {
	for _, prefix := range forbidden {
		if imported == prefix || strings.HasPrefix(imported, prefix+"/") {
			return true
		}
	}
	return false
}

func isUntypedMapRowSlice(node ast.Node) bool {
	slice, ok := node.(*ast.ArrayType)
	if !ok || slice.Len != nil {
		return false
	}
	mapping, ok := slice.Elt.(*ast.MapType)
	if !ok {
		return false
	}
	key, ok := mapping.Key.(*ast.Ident)
	if !ok || key.Name != "string" {
		return false
	}
	switch value := mapping.Value.(type) {
	case *ast.Ident:
		return value.Name == "any"
	case *ast.InterfaceType:
		return value.Methods == nil || len(value.Methods.List) == 0
	default:
		return false
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve architecture test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
