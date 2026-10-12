package generate

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/x/syntetic/model"
)

func namedDiscoveryResolver(t *testing.T, descriptors ...*x.Type) *typecatalog.Resolver {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	for _, descriptor := range descriptors {
		if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}}})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func namedDiscoveryDeclaration(t *testing.T, name, definition string) *x.Type {
	t.Helper()
	expression, err := parser.ParseExpr(definition)
	if err != nil {
		t.Fatal(err)
	}
	return &x.Type{Name: name, PkgPath: "example.com/models", SynteticType: &model.Type{Name: name, PkgPath: "example.com/models", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: expression}}}
}

func TestColumnDiscoveryNamedTypesPreserveRuntimeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, definition, expression string
		want                         reflect.Type
	}{
		{"scalar", "int", "models.Mode", reflect.TypeFor[int]()},
		{"pointer", "int", "*models.Mode", reflect.TypeFor[*int]()},
		{"slice", "[]int", "models.Mode", reflect.TypeFor[[]int]()},
		{"map", "map[string]int", "models.Mode", reflect.TypeFor[map[string]int]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := namedDiscoveryDeclaration(t, "Mode", tc.definition)
			// Richer declaration authority takes precedence over the stale fallback.
			descriptor.Definition = "string"
			resolver := namedDiscoveryResolver(t, descriptor)
			component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}}, Parameters: []*spec.Parameter{{Name: "Mode", TypeExpr: tc.expression, Source: spec.BindSource{Kind: "body", Name: "mode"}}}}
			generator := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"})
			typ, err := generator.ColumnDiscoveryInputType()
			if err != nil {
				t.Fatal(err)
			}
			field, ok := typ.FieldByName("Mode")
			if !ok || field.Type != tc.want {
				t.Fatalf("field=%+v want=%v", field, tc.want)
			}
			if _, err = generator.RuntimeInputType(); err == nil {
				t.Fatal("source-only discovery representation became runtime authority")
			}
			resolved, err := resolver.Descriptor("models.Mode")
			if err != nil || resolved.Type != nil || resolved.Definition != "string" {
				t.Fatalf("descriptor authority changed: %+v %v", resolved, err)
			}
		})
	}
}

func TestColumnDiscoveryNamedTypesUseDeclarationScopeAndRejectCycles(t *testing.T) {
	leaf := namedDiscoveryDeclaration(t, "Leaf", "int")
	for _, tc := range []struct {
		name, definition string
		imported         bool
	}{
		{"local alias", "Leaf", false}, {"local container", "[]Leaf", false}, {"imported container", "map[string]other.Leaf", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := namedDiscoveryDeclaration(t, "Mode", tc.definition)
			descriptors := []*x.Type{leaf, descriptor}
			if tc.imported {
				descriptor.SynteticType.Imports = map[string]*model.ImportRef{"other": {Path: "example.com/other", Alias: "other"}}
				imported := namedDiscoveryDeclaration(t, "Leaf", "string")
				imported.PkgPath = "example.com/other"
				imported.SynteticType.PkgPath = imported.PkgPath
				descriptors = append(descriptors, imported)
			}
			resolver := namedDiscoveryResolver(t, descriptors...)
			m := newRuntimeInputMaterializer(&Plan{}, resolver)
			m.discoveryOnly = true
			typ, err := m.descriptorType("models.Mode")
			if err != nil {
				t.Fatal(err)
			}
			if tc.imported {
				if typ != reflect.TypeFor[map[string]string]() {
					t.Fatal(typ)
				}
			} else if tc.name == "local container" {
				if typ != reflect.TypeFor[[]int]() {
					t.Fatal(typ)
				}
			} else if typ != reflect.TypeFor[int]() {
				t.Fatal(typ)
			}
		})
	}
	for _, definition := range []string{"Mode", "<-1"} {
		t.Run("reject "+definition, func(t *testing.T) {
			descriptor := &x.Type{Name: "Mode", PkgPath: "example.com/models", Definition: definition}
			if definition == "Mode" {
				descriptor = namedDiscoveryDeclaration(t, "Mode", definition)
			}
			m := newRuntimeInputMaterializer(&Plan{}, namedDiscoveryResolver(t, descriptor))
			m.discoveryOnly = true
			if _, err := m.descriptorType("models.Mode"); err == nil {
				t.Fatal("invalid/recursive discovery declaration accepted")
			}
		})
	}
	// A descriptor with only its supported textual declaration also works.
	m := newRuntimeInputMaterializer(&Plan{}, namedDiscoveryResolver(t, &x.Type{Name: "Mode", PkgPath: "example.com/models", Definition: "int"}))
	m.discoveryOnly = true
	if typ, err := m.descriptorType("models.Mode"); err != nil || typ != reflect.TypeFor[int]() {
		t.Fatalf("fallback=%v %v", typ, err)
	}
}

type compiledDiscoveryMode int

func (m *compiledDiscoveryMode) UnmarshalJSON(data []byte) error {
	if string(data) == `"enabled"` {
		*m = 1
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("mode: %w", err)
	}
	*m = compiledDiscoveryMode(n)
	return nil
}
func TestColumnDiscoveryKeepsCompiledNamedIdentityAndJSONMethods(t *testing.T) {
	resolver := namedDiscoveryResolver(t, x.NewType(reflect.TypeFor[compiledDiscoveryMode](), x.WithName("Mode"), x.WithPkgPath("example.com/models")))
	component := &spec.Component{Name: "Records", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}}, Parameters: []*spec.Parameter{{Name: "Mode", TypeExpr: "*models.Mode", Source: spec.BindSource{Kind: "body", Name: "mode"}}}}
	generator := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"})
	for _, build := range []func() (reflect.Type, error){generator.ColumnDiscoveryInputType, generator.RuntimeInputType} {
		typ, err := build()
		if err != nil {
			t.Fatal(err)
		}
		field, _ := typ.FieldByName("Mode")
		if field.Type != reflect.TypeFor[*compiledDiscoveryMode]() {
			t.Fatal(field.Type)
		}
		value := reflect.New(typ)
		if err = json.NewDecoder(strings.NewReader(`{"Mode":"enabled"}`)).Decode(value.Interface()); err != nil {
			t.Fatal(err)
		}
		if mode := value.Elem().FieldByName("Mode").Interface().(*compiledDiscoveryMode); *mode != 1 {
			t.Fatal(*mode)
		}
	}
}
