package generate

import (
	"go/ast"
	"go/parser"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

type linkedShapeSnapshot struct{ Id int }

func TestHelperInferenceUsesCurrentGeneratedShape(t *testing.T) {
	type useCase struct {
		description            string
		origin                 typecatalog.TypeOrigin
		shape, field, expected string
		wantError              bool
	}
	for _, test := range []useCase{
		{description: "generated new field", origin: typecatalog.TypeOriginGenerated, shape: "struct { Id int; FreshField *string }", field: "FreshField", expected: "*string"},
		{description: "generated changed field", origin: typecatalog.TypeOriginGenerated, shape: "struct { Id string }", field: "Id", expected: "string"},
		{description: "generated removed field", origin: typecatalog.TypeOriginGenerated, shape: "struct { FreshField *string }", field: "Id", wantError: true},
		{description: "authored linked identity preserved", origin: typecatalog.TypeOriginPackage, shape: "struct { Id string; FreshField *string }", field: "Id", expected: "int"},
		{description: "runtime only generated identity", origin: typecatalog.TypeOriginGenerated, field: "Id", expected: "int"},
	} {
		t.Run(test.description, func(t *testing.T) {
			compiledType := reflect.TypeFor[linkedShapeSnapshot]()
			descriptor := x.NewType(compiledType, x.WithName("Row"), x.WithPkgPath("example.com/model"))
			if test.shape != "" {
				expression, err := parser.ParseExpr(test.shape)
				if err != nil {
					t.Fatal(err)
				}
				descriptor.SynteticType = &smodel.Type{Name: "Row", PkgPath: "example.com/model", ReflectType: compiledType, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Row"), Type: expression}}
			}
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(test.origin, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "model", Package: "example.com/model"}}})
			if err != nil {
				t.Fatal(err)
			}
			source := `#setting($_ = $route('/fresh','GET'))
#import('model','example.com/model')
#define($_ = $Record<*model.Row>(body/record).Optional())
#define($_ = $Projection<?>(param/Record) /* SELECT FIELD AS Value FROM ` + "`/`" + ` LIMIT 1 */)
SELECT 1`
			component, err := parseTestComponentSource("example.com/source", "fresh", strings.Replace(source, "FIELD", test.field, 1))
			if err != nil {
				t.Fatal(err)
			}
			declarations, err := testDeclarations(component)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := New(Input{Component: component, Declarations: declarations, TypeResolver: resolver, TargetPackage: "example.com/output"}).Plan()
			if test.wantError {
				if err == nil {
					t.Fatal("removed source field still inferred from stale runtime type")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				helper := plan.helperType("ProjectionRow")
				if helper == nil || len(helper.Fields) != 1 || helper.Fields[0].Type != test.expected {
					t.Fatalf("helper=%+v, expected %s", helper, test.expected)
				}
			}
			after, err := resolver.Descriptor("model.Row")
			if err != nil {
				t.Fatal(err)
			}
			if after.Type != compiledType {
				t.Fatal("structural inference changed runtime identity")
			}
			if after.SynteticType != nil && after.SynteticType.ReflectType != compiledType {
				t.Fatal("structural inference changed synthetic runtime identity")
			}
			runtimeFields, err := xshape.New(after, resolver.Descriptor).Fields()
			if err != nil {
				t.Fatal(err)
			}
			if len(runtimeFields) != 1 || runtimeFields[0].Name != "Id" || runtimeFields[0].ReflectedType != reflect.TypeFor[int]() {
				t.Fatal("global runtime shape precedence changed")
			}
		})
	}
}

func TestHelperInferenceGeneratedDependencyShapes(t *testing.T) {
	for _, test := range []struct{ description, record, projection, path, field, expected string }{
		{"aggregate", "model.Row", "ARRAY_AGG(FreshField) AS Values", "/", "Values", "[]*string"},
		{"authored source alias", "aliases.RowAlias", "FreshField AS Value", "/", "Value", "*string"},
		{"nested generated row", "model.Parent", "FreshField AS Value", "/Child", "Value", "*string"},
	} {
		t.Run(test.description, func(t *testing.T) {
			compiledType := reflect.TypeFor[linkedShapeSnapshot]()
			catalog := typecatalog.NewCatalog()
			register := func(pkg, name, shape string, origin typecatalog.TypeOrigin, linked bool) {
				expression, err := parser.ParseExpr(shape)
				if err != nil {
					t.Fatal(err)
				}
				descriptor := &x.Type{PkgPath: pkg, Name: name, SynteticType: &smodel.Type{Name: name, PkgPath: pkg, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: expression}}}
				if linked {
					descriptor.Type = compiledType
					descriptor.SynteticType.ReflectType = compiledType
				}
				if err = catalog.Register(origin, descriptor); err != nil {
					t.Fatal(err)
				}
			}
			register("example.com/model", "Row", "struct { Id int; FreshField *string }", typecatalog.TypeOriginGenerated, true)
			register("example.com/model", "Parent", "struct { Child *nested.Child }", typecatalog.TypeOriginGenerated, true)
			register("example.com/nested", "Child", "struct { Id int; FreshField *string }", typecatalog.TypeOriginGenerated, true)
			register("example.com/aliases", "RowAlias", "model.Row", typecatalog.TypeOriginPackage, false)
			imports := []typecatalog.PackageImport{{Alias: "model", Package: "example.com/model"}, {Alias: "nested", Package: "example.com/nested"}, {Alias: "aliases", Package: "example.com/aliases"}}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: imports})
			if err != nil {
				t.Fatal(err)
			}
			source := `#setting($_ = $route('/fresh','GET'))
#import('model','example.com/model')
#import('nested','example.com/nested')
#import('aliases','example.com/aliases')
#define($_ = $Record<*RECORD>(body/record).Optional())
#define($_ = $Projection<?>(param/Record) /* SELECT PROJECTION FROM ` + "`PATH`" + ` LIMIT 1 */)
SELECT 1`
			source = strings.NewReplacer("RECORD", test.record, "PROJECTION", test.projection, "PATH", test.path).Replace(source)
			component, err := parseTestComponentSource("example.com/source", "fresh", source)
			if err != nil {
				t.Fatal(err)
			}
			declarations, err := testDeclarations(component)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := New(Input{Component: component, Declarations: declarations, TypeResolver: resolver, TargetPackage: "example.com/output"}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, helper := range plan.HelperTypes {
				for _, field := range helper.Fields {
					if field.Name == test.field && field.Type == test.expected {
						found = true
					}
				}
			}
			if !found {
				t.Fatalf("missing %s %s in %+v", test.field, test.expected, plan.HelperTypes)
			}
			for _, key := range []string{"model.Row", "model.Parent", "nested.Child"} {
				descriptor, err := resolver.Descriptor(key)
				if err != nil {
					t.Fatal(err)
				}
				if descriptor.Type != compiledType || descriptor.SynteticType.ReflectType != compiledType {
					t.Fatalf("runtime identity changed for %s", key)
				}
			}
			alias, err := resolver.Descriptor("aliases.RowAlias")
			if err != nil {
				t.Fatal(err)
			}
			if alias.Type != nil {
				t.Fatal("source alias acquired runtime identity")
			}
			target, ok := alias.SynteticType.TypeSpec.Type.(*ast.SelectorExpr)
			if !ok || target.Sel.Name != "Row" {
				t.Fatal("authored alias source changed")
			}
		})
	}
}

func TestHelperShapeLookupPreservesLocalAndUnknownDescriptors(t *testing.T) {
	expression, err := parser.ParseExpr("struct { FreshField *string }")
	if err != nil {
		t.Fatal(err)
	}
	emptyResolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		description, key     string
		linked, withResolver bool
	}{
		{"local synthetic descriptor is already authoritative", "local-only[]", false, true},
		{"unknown linked provenance retains identity", "example.com/local.Row", true, true},
		{"no provenance resolver retains identity", "example.com/local.Row", true, false},
	} {
		t.Run(test.description, func(t *testing.T) {
			descriptor := &x.Type{Name: "Row", PkgPath: "example.com/local", SynteticType: &smodel.Type{TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Row"), Type: expression}}}
			if test.linked {
				descriptor.Type = reflect.TypeFor[linkedShapeSnapshot]()
			}
			resolver := &planResolver{types: testTypeResolver(func(string) (*x.Type, error) { return descriptor, nil })}
			if test.withResolver {
				resolver.input.TypeResolver = emptyResolver
			}
			actual, err := resolver.helperShapeLookup()(test.key)
			if err != nil {
				t.Fatal(err)
			}
			if actual != descriptor || actual.SynteticType.TypeSpec.Type != expression {
				t.Fatal("unrelated descriptor changed")
			}
		})
	}
}
