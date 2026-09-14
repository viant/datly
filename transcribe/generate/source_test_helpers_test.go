package generate

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
)

func GeneratePackageFromSource(dir string, scope string, name string, source string) (*Result, error) {
	component, err := parseTestComponentSource(scope, name, source)
	if err != nil {
		return nil, err
	}
	var diagnostics []spec.Diagnostic
	if component != nil && component.TypeContext != nil && (len(component.TypeContext.Imports) > 0 || component.TypeContext.DefaultPackage != "") {
		if moduleRoot, ok := findModuleRoot(dir); ok {
			resolver, resolveErr := moduleTypeResolver(moduleRoot, component, &diagnostics)
			if resolveErr == nil {
				generator, err := newTestGenerator(component, resolver)
				if err != nil {
					return nil, err
				}
				result, err := generator.Generate(dir)
				return attachDiagnostics(result, diagnostics), err
			}
			appendDiagnostic(&diagnostics, spec.SeverityWarn, "gen.type_resolver_init_failed", resolveErr.Error())
		} else {
			appendDiagnostic(&diagnostics, spec.SeverityWarn, "gen.module_root_not_found", "type-context lookup requested but no go.mod root was found")
		}
	}
	generator, err := newTestGenerator(component, nil)
	if err != nil {
		return nil, err
	}
	result, err := generator.Generate(dir)
	return attachDiagnostics(result, diagnostics), err
}

func GeneratePackageFromSourceWithTypeResolver(dir string, scope string, name string, source string, lookup func(name string) (reflect.Type, error)) (*Result, error) {
	component, err := parseTestComponentSource(scope, name, source)
	if err != nil {
		return nil, err
	}
	generator, err := newTestGenerator(component, reflectResolver(lookup))
	if err != nil {
		return nil, err
	}
	return generator.Generate(dir)
}

func GeneratePackageFromSourceWithModuleLookup(dir string, moduleRoot string, scope string, name string, source string) (*Result, error) {
	component, err := parseTestComponentSource(scope, name, source)
	if err != nil {
		return nil, err
	}
	var diagnostics []spec.Diagnostic
	resolver, err := moduleTypeResolver(moduleRoot, component, &diagnostics)
	if err != nil {
		return nil, err
	}
	generator, err := newTestGenerator(component, resolver)
	if err != nil {
		return nil, err
	}
	result, err := generator.Generate(dir)
	return attachDiagnostics(result, diagnostics), err
}

func newTestGenerator(component *spec.Component, resolver typeResolver) (*Generator, error) {
	declarations, err := testDeclarations(component)
	if err != nil {
		return nil, err
	}
	generator := New(Input{Component: component, Declarations: declarations})
	if resolver != nil {
		generator.resolver = resolver
	}
	return generator, nil
}

type testTypeResolver func(string) (*x.Type, error)

func (r testTypeResolver) Descriptor(name string) (*x.Type, error) {
	return r(name)
}

func reflectResolver(lookup func(string) (reflect.Type, error)) typeResolver {
	if lookup == nil {
		return nil
	}
	return testTypeResolver(func(name string) (*x.Type, error) {
		typ, err := lookup(unwrapQualifiedTypeName(name))
		if err != nil || typ == nil {
			return nil, err
		}
		return x.NewType(typ), nil
	})
}

func testDeclarations(component *spec.Component) (Declarations, error) {
	if component == nil {
		return nil, nil
	}
	var result Declarations
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || strings.TrimSpace(param.DeclarationSQL) == "" {
			continue
		}
		analysis, err := dql.AnalyzeDeclarationSQL(param.DeclarationSQL)
		if err != nil {
			return nil, err
		}
		if analysis == nil {
			continue
		}
		if result == nil {
			result = Declarations{}
		}
		result[param.Identity()] = Declaration{
			Projection:       testGenerationProjection(analysis.Projection),
			NestedChildField: analysis.NestedChildField(),
			DataType:         analysis.DataType,
		}
	}
	return result, nil
}

func testGenerationProjection(source []dql.DeclarationProjection) []DeclarationProjection {
	result := make([]DeclarationProjection, 0, len(source))
	for _, field := range source {
		result = append(result, DeclarationProjection{Name: field.Name, Source: field.Source, Aggregate: field.Aggregate})
	}
	return result
}

func testPlan(t *testing.T, component *spec.Component) *Plan {
	t.Helper()
	generator, err := newTestGenerator(component, nil)
	if err != nil {
		t.Fatalf("newTestGenerator() error = %v", err)
	}
	plan, err := generator.Plan()
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	return plan
}

func testSyntaxPlan(t *testing.T, component *spec.Component) *Plan {
	t.Helper()
	declarations, err := testDeclarations(component)
	if err != nil {
		t.Fatalf("testDeclarations() error = %v", err)
	}
	plan, err := (&planResolver{input: Input{Component: component, Declarations: declarations}}).resolveBase()
	if err != nil {
		t.Fatalf("planResolver.resolveBase() error = %v", err)
	}
	return plan
}

func testPlanWithTypeResolver(t *testing.T, component *spec.Component, lookup func(name string) (reflect.Type, error)) (*Plan, error) {
	t.Helper()
	generator, err := newTestGenerator(component, reflectResolver(lookup))
	if err != nil {
		return nil, err
	}
	return generator.Plan()
}

func parseTestComponentSource(scope, name, source string) (*spec.Component, error) {
	prepared := dql.PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	return dql.ParsePreparedComponentSource(scope, name, prepared)
}

func moduleTypeResolver(moduleRoot string, component *spec.Component, sink *[]spec.Diagnostic) (typeResolver, error) {
	if component == nil || component.TypeContext == nil || (len(component.TypeContext.Imports) == 0 && component.TypeContext.DefaultPackage == "") {
		return testTypeResolver(func(string) (*x.Type, error) { return nil, nil }), nil
	}
	modulePath, err := readModulePath(moduleRoot)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(modulePath) == "" {
		appendDiagnostic(sink, spec.SeverityWarn, "gen.module_path_empty", "go.mod did not declare a module path")
		return testTypeResolver(func(string) (*x.Type, error) { return nil, nil }), nil
	}
	imports := map[string]string{}
	for _, item := range component.TypeContext.Imports {
		if item.Alias != "" && item.Package != "" {
			imports[item.Alias] = item.Package
		}
	}
	return testTypeResolver(func(name string) (*x.Type, error) {
		base := unwrapQualifiedTypeName(name)
		pkgPath := ""
		typeName := ""
		if parts := strings.Split(base, "."); len(parts) == 2 {
			var ok bool
			pkgPath, ok = imports[parts[0]]
			if !ok {
				return nil, nil
			}
			typeName = parts[1]
		} else if index := strings.LastIndex(base, "."); index != -1 {
			pkgPath = base[:index]
			typeName = base[index+1:]
		} else {
			pkgPath = strings.TrimSpace(component.TypeContext.DefaultPackage)
			typeName = base
		}
		if typeName == "" {
			return nil, nil
		}
		rel := strings.TrimPrefix(pkgPath, modulePath)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			rel = "."
		}
		pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(moduleRoot), rel)
		if err != nil {
			appendDiagnostic(sink, spec.SeverityWarn, "gen.package_load_failed", "failed to load package "+rel+": "+err.Error())
			return nil, nil
		}
		for _, declared := range pkg.Types {
			if declared == nil || declared.Name != typeName {
				continue
			}
			return &x.Type{PkgPath: pkgPath, Name: typeName, SynteticType: declared}, nil
		}
		return nil, nil
	}), nil
}

func readModulePath(moduleRoot string) (string, error) {
	data, err := os.ReadFile(moduleRoot + "/go.mod")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", nil
}

func findModuleRoot(dir string) (string, bool) {
	current := dir
	for {
		if _, err := os.Stat(current + "/go.mod"); err == nil {
			return current, true
		}
		parent := current
		if index := strings.LastIndex(parent, "/"); index != -1 {
			parent = parent[:index]
		}
		if parent == "" || parent == current {
			return "", false
		}
		current = parent
	}
}

func appendDiagnostic(sink *[]spec.Diagnostic, severity spec.Severity, code string, message string) {
	if sink == nil {
		return
	}
	*sink = append(*sink, spec.Diagnostic{
		Severity: severity,
		Code:     code,
		Message:  message,
	})
}

func attachDiagnostics(result *Result, diagnostics []spec.Diagnostic) *Result {
	if result == nil || len(diagnostics) == 0 {
		return result
	}
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	return result
}
