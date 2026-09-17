package typecatalog

import (
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	model "github.com/viant/x/syntetic/model"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestDescriptorPreservesGenericSpecializationAndOrdinaryIdentity(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", "package hooks\ntype Hook[T any]struct{Value T}\nfunc(*Hook[A])Set(value A){}", 0)
	if err != nil {
		t.Fatal(err)
	}
	definition := file.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec)
	catalog := NewCatalog()
	descriptor := &x.Type{Name: "Hook", PkgPath: "example.com/hooks", SynteticType: &model.Type{Name: "Hook", PkgPath: "example.com/hooks", TypeSpec: definition, PtrMethodsAST: []*ast.FuncDecl{file.Decls[1].(*ast.FuncDecl)}}}
	if err = catalog.RegisterAll(TypeOriginPackage, descriptor, &x.Type{Name: "Item", PkgPath: "example.com/one"}, &x.Type{Name: "Item", PkgPath: "example.com/two"}); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolverWithProvenance(catalog, PackageAuthority, &ResolutionContext{Imports: []PackageImport{{Alias: "h", Package: "example.com/hooks"}, {Alias: "one", Package: "example.com/one"}, {Alias: "two", Package: "example.com/two"}}}, map[string]Provenance{"example.com/one.Item": {Package: "example.com/one", File: "one.go"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"one", "two"} {
		actual, err := resolver.Descriptor("h.Hook[" + alias + ".Item]")
		if err != nil {
			t.Fatal(err)
		}
		methods, err := xshape.New(actual, resolver.Descriptor).Methods(true)
		if err != nil {
			t.Fatal(err)
		}
		if len(methods) != 1 || methods[0].Parameters[0] != "example.com/"+alias+".Item" {
			t.Fatalf("specialized methods = %+v", methods)
		}
		if !strings.Contains(actual.Name, "example.com/"+alias+".Item") {
			t.Fatalf("specialized identity = %s", actual.Name)
		}
		actual.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "Mutation"
		again, err := resolver.Descriptor("h.Hook[" + alias + ".Item]")
		if err != nil || again.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name != "Value" {
			t.Fatalf("specialized descriptor mutation leaked: %v", err)
		}
	}
	ordinary, err := resolver.Descriptor("one.Item")
	if err != nil || ordinary.Key() != "example.com/one.Item" {
		t.Fatalf("ordinary descriptor = %+v, %v", ordinary, err)
	}
	provenance, err := resolver.ResolveWithProvenance("one.Item")
	if err != nil || provenance.Provenance.File != "one.go" {
		t.Fatalf("provenance = %+v, %v", provenance, err)
	}
	original, err := resolver.Descriptor("h.Hook")
	if err != nil || original.SynteticType.TypeSpec.TypeParams == nil {
		t.Fatal("generic descriptor source was mutated")
	}
}
