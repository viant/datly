package typecatalog

import (
	smodel "github.com/viant/x/syntetic/model"
	"go/ast"
	"sync"
	"testing"
)

func TestCatalogSnapshotPublicIsolation(t *testing.T) {
	declaration := &smodel.Type{Name: "Record", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("ID")}, Type: ast.NewIdent("int")}}}}}}
	source := NewCatalog()
	if err := source.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo", Types: []*smodel.Type{declaration}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Clone()
	if err != nil {
		t.Fatal(err)
	}
	const key = "example.com/demo.Record"
	var workers sync.WaitGroup
	for _, catalog := range []*Catalog{source, snapshot} {
		workers.Add(1)
		go func(c *Catalog) {
			defer workers.Done()
			for i := 0; i < 20; i++ {
				registry, e := c.Registry(PackageAuthority)
				if e != nil {
					t.Error(e)
					return
				}
				registry.Lookup(key).SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "Changed"
				got, ok, e := c.Resolve(PackageAuthority, key)
				if e != nil || !ok || syntheticFirstField(got) != "ID" {
					t.Errorf("public mutation reached catalog: %v %v", ok, e)
					return
				}
			}
		}(catalog)
	}
	workers.Wait()
	if err := source.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := source.Resolve(PackageAuthority, key); err != nil || ok {
		t.Fatalf("source replacement: %v %v", ok, err)
	}
	if got, ok, err := snapshot.Resolve(PackageAuthority, key); err != nil || !ok || syntheticFirstField(got) != "ID" {
		t.Fatalf("snapshot changed: %v %v", ok, err)
	}
}

func BenchmarkCatalogSnapshot(b *testing.B) {
	fields := make([]*ast.Field, 1000)
	for i := range fields {
		fields[i] = &ast.Field{Names: []*ast.Ident{ast.NewIdent("Field")}, Type: ast.NewIdent("string")}
	}
	c := NewCatalog()
	if err := c.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo", Types: []*smodel.Type{{Name: "Record", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: fields}}}}}}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Clone(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestResolverSnapshotPublicIsolation(t *testing.T) {
	declaration := &smodel.Type{Name: "Record", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("ID")}, Type: ast.NewIdent("int")}}}}}}
	source := NewCatalog()
	if err := source.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo", Types: []*smodel.Type{declaration}}); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewResolver(source, PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo"}); err != nil {
		t.Fatal(err)
	}
	const key = "example.com/demo.Record"
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 10; j++ {
				got, e := resolver.Descriptor(key)
				if e != nil || got == nil || syntheticFirstField(got) != "ID" {
					t.Errorf("resolver snapshot changed: %v", e)
					return
				}
				got.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "CallerMutation"
				shape, e := resolver.ResolveShape(key)
				if e != nil || shape == nil || syntheticFirstField(shape.Descriptor) != "ID" {
					t.Errorf("shape mutation leaked: %v", e)
					return
				}
				shape.Descriptor.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List[0].Names[0].Name = "ShapeMutation"
			}
		}()
	}
	workers.Wait()
}

func TestCatalogSharedPackageIdentityConcurrentRegistration(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		source := NewCatalog()
		declaration := &smodel.Type{Name: "Record", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Record")}}
		if err := source.RegisterPackage(TypeOriginPackage, &smodel.Package{Name: "demo", PkgPath: "example.com/demo", Types: []*smodel.Type{declaration}}); err != nil {
			t.Fatal(err)
		}
		value, ok, err := source.Resolve(PackageAuthority, "example.com/demo.Record")
		if err != nil || !ok {
			t.Fatalf("resolve %v %v", ok, err)
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		for i := 0; i < 16; i++ {
			snapshot, err := source.Clone()
			if err != nil {
				t.Fatal(err)
			}
			workers.Add(1)
			go func(c *Catalog) {
				defer workers.Done()
				<-start
				if err := c.RegisterAll(TypeOriginPackage, value); err != nil {
					t.Error(err)
				}
			}(snapshot)
		}
		close(start)
		workers.Wait()
	}
}
