package generate

import (
	"go/ast"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestGeneratorEmitsCatalogGeneratedTypeAsOwnedPackageArtifact(t *testing.T) {
	t.Parallel()
	const targetPackage = "example.com/generated/reporting"
	catalog := typecatalog.NewCatalog()
	descriptor := generatedTypeDescriptor(targetPackage, "SpendCubeInput")
	if err := catalog.Register(typecatalog.TypeOriginGenerated, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: targetPackage})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	packageDir := filepath.Join(root, "reporting")
	input := Input{
		Component: generatedTypeComponent(), TargetPackage: targetPackage, TypeResolver: resolver,
		GeneratedTypes: []GeneratedTypeReference{{DescriptorKey: descriptor.Key()}},
	}
	result, err := New(input).Generate(packageDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.GeneratedTypes) != 1 || result.Plan.GeneratedTypes[0].Destination != "spend_cube_input.go" {
		t.Fatalf("generated type plan = %+v", result.Plan.GeneratedTypes)
	}
	content, err := os.ReadFile(filepath.Join(packageDir, "spend_cube_input.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	if !strings.Contains(text, `time "time"`) || !strings.Contains(text, "When time.Time") ||
		!strings.Contains(text, `json:"when,omitempty"`) {
		t.Fatalf("generated type content = %s", text)
	}
	command := exec.Command("go", "vet", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated package does not compile: %v\n%s", runErr, output)
	}
	if _, err = New(Input{Component: generatedTypeComponent(), TargetPackage: targetPackage, TypeResolver: resolver}).Generate(packageDir); err != nil {
		t.Fatal(err)
	}
	if retained, err := os.ReadFile(filepath.Join(packageDir, "spend_cube_input.go")); err != nil || string(retained) != text {
		t.Fatalf("unreferenced generated shape was changed or removed: %v", err)
	}
}

func TestGeneratorRejectsInvalidGeneratedTypeAuthorityAndCollisions(t *testing.T) {
	const targetPackage = "example.com/generated/reporting"
	mismatchedSyntheticName := generatedTypeDescriptor(targetPackage, "CubeInput")
	mismatchedSyntheticName.SynteticType.Name = "OtherInput"
	mismatchedSyntheticPackage := generatedTypeDescriptor(targetPackage, "CubeInput")
	mismatchedSyntheticPackage.SynteticType.PkgPath = "example.com/foreign"
	tests := []struct {
		name          string
		descriptor    *x.Type
		descriptorKey string
		context       *typecatalog.ResolutionContext
		reference     GeneratedTypeReference
		want          string
	}{
		{name: "foreign package", descriptor: generatedTypeDescriptor("example.com/foreign", "CubeInput"), want: "not target package"},
		{name: "missing synthetic declaration", descriptor: &x.Type{PkgPath: targetPackage, Name: "CubeInput"}, want: "no synthetic type declaration"},
		{name: "synthetic name mismatch", descriptor: mismatchedSyntheticName, want: "synthetic name"},
		{name: "synthetic package mismatch", descriptor: mismatchedSyntheticPackage, want: "synthetic package"},
		{name: "unqualified descriptor expression", descriptor: generatedTypeDescriptor(targetPackage, "CubeInput"), descriptorKey: "CubeInput", want: "exact catalog key"},
		{
			name: "alias-qualified descriptor expression", descriptor: generatedTypeDescriptor(targetPackage, "CubeInput"),
			descriptorKey: "reporting.CubeInput",
			context:       &typecatalog.ResolutionContext{PackagePath: targetPackage, Imports: []typecatalog.PackageImport{{Alias: "reporting", Package: targetPackage}}},
			want:          "exact catalog key",
		},
		{name: "input name collision", descriptor: generatedTypeDescriptor(targetPackage, "SpendInput"), want: "shared by input contract and generated type"},
		{name: "destination collision", descriptor: generatedTypeDescriptor(targetPackage, "CubeInput"), reference: GeneratedTypeReference{Destination: "input.go"}, want: "shared by input contract and generated type"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginGenerated, testCase.descriptor); err != nil {
				t.Fatal(err)
			}
			context := testCase.context
			if context == nil {
				context = &typecatalog.ResolutionContext{PackagePath: targetPackage}
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, context)
			if err != nil {
				t.Fatal(err)
			}
			reference := testCase.reference
			reference.DescriptorKey = testCase.descriptorKey
			if reference.DescriptorKey == "" {
				reference.DescriptorKey = testCase.descriptor.Key()
			}
			_, err = New(Input{
				Component: generatedTypeComponent(), TargetPackage: targetPackage, TypeResolver: resolver,
				GeneratedTypes: []GeneratedTypeReference{reference},
			}).Plan()
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Plan() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func generatedTypeComponent() *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/source", Name: "Spend"}, Name: "Spend",
		Routes: []*spec.Route{{Method: "GET", Path: "/spend"}},
	}
}

func generatedTypeDescriptor(packagePath, name string) *x.Type {
	typeOf := reflect.TypeOf(struct {
		When time.Time `json:"when,omitempty"`
	}{})
	field := &ast.Field{
		Names: []*ast.Ident{ast.NewIdent("When")},
		Type:  &ast.SelectorExpr{X: ast.NewIdent("time"), Sel: ast.NewIdent("Time")},
		Tag:   &ast.BasicLit{Kind: token.STRING, Value: "`json:\"when,omitempty\"`"},
	}
	return &x.Type{
		Type: typeOf, PkgPath: packagePath, Name: name,
		SynteticType: &smodel.Type{
			Name: name, PkgPath: packagePath, ReflectType: typeOf,
			TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(name), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{field}}}},
			Imports:  map[string]*smodel.ImportRef{"time": {Path: "time", Alias: "time"}},
		},
	}
}

func TestGeneratedTypeFilenamePrefixAndOverride(t *testing.T) {
	const target = "example.com/generated/reporting"
	catalog := typecatalog.NewCatalog()
	descriptor := generatedTypeDescriptor(target, "CubeInput")
	if err := catalog.Register(typecatalog.TypeOriginGenerated, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: target})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, prefix, explicit, override, want string }{
		{"default", "", "", "", "cube_input.go"},
		{"prefix", "orders_", "", "", "orders_cube_input.go"},
		{"explicit API", "orders_", "selected.go", "", "selected.go"},
		{"DQL override", "orders_", "selected.go", "chosen.go", "chosen.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := generatedTypeComponent()
			component.Settings = &spec.Settings{Generation: &spec.GenerationSettings{FilePrefix: tc.prefix}}
			if tc.override != "" {
				if err := component.Settings.Generation.SetSupportFile("type:CubeInput", tc.override); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := New(Input{Component: component, TargetPackage: target, TypeResolver: resolver, GeneratedTypes: []GeneratedTypeReference{{DescriptorKey: descriptor.Key(), Destination: tc.explicit}}}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.GeneratedTypes) != 1 || plan.GeneratedTypes[0].Destination != tc.want {
				t.Fatal(plan.GeneratedTypes)
			}
		})
	}
}
