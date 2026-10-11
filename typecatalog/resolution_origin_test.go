package typecatalog

import (
	"reflect"
	"sync"
	"testing"

	"github.com/viant/x"
)

func TestResolutionOriginPriorityAndSnapshot(t *testing.T) {
	const pkg = "example.com/contracts"
	opts := []x.Option{x.WithName("Record"), x.WithPkgPath(pkg)}
	catalog := NewCatalog()
	generated := x.NewType(reflect.TypeFor[generatedSample](), opts...)
	if err := catalog.Register(TypeOriginGenerated, generated); err != nil {
		t.Fatal(err)
	}
	context := &ResolutionContext{Imports: []PackageImport{{Alias: "contracts", Package: pkg}}}
	snapshot, err := NewResolver(catalog, PackageAuthority, context)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		origin TypeOrigin
		typ    reflect.Type
	}{
		{TypeOriginPackage, reflect.TypeFor[packageSample]()},
		{TypeOriginDQL, reflect.TypeFor[dqlSample]()},
	} {
		if err := catalog.Register(item.origin, x.NewType(item.typ, opts...)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		authority Authority
		origin    TypeOrigin
		typ       reflect.Type
	}{
		{PackageAuthority, TypeOriginPackage, reflect.TypeFor[packageSample]()},
		{TranscribeAuthority, TypeOriginDQL, reflect.TypeFor[dqlSample]()},
	} {
		resolver, err := NewResolver(catalog, tc.authority, context)
		if err != nil {
			t.Fatal(err)
		}
		for _, expression := range []string{"contracts.Record", "[]*contracts.Record", pkg + ".Record"} {
			resolution, err := resolver.ResolveWithProvenance(expression)
			if err != nil || resolution == nil || resolution.Origin != tc.origin {
				t.Fatalf("%s %s: resolution=%+v err=%v", tc.authority, expression, resolution, err)
			}
			descriptor, err := resolver.Descriptor(expression)
			if err != nil || descriptor == nil || descriptor.Type != tc.typ {
				t.Fatalf("descriptor and origin diverged: %+v %v", descriptor, err)
			}
		}
	}
	resolved, err := snapshot.ResolveWithProvenance("contracts.Record")
	if err != nil || resolved == nil || resolved.Origin != TypeOriginGenerated {
		t.Fatalf("snapshot changed: %+v %v", resolved, err)
	}
	descriptor, err := snapshot.Descriptor("contracts.Record")
	if err != nil || descriptor.Type != generated.Type {
		t.Fatalf("snapshot descriptor changed: %+v %v", descriptor, err)
	}
}

func TestResolutionOriginConcurrentSnapshots(t *testing.T) {
	const key = "example.com/contracts.Record"
	catalog := NewCatalog()
	opts := []x.Option{x.WithName("Record"), x.WithPkgPath("example.com/contracts")}
	if err := catalog.Register(TypeOriginGenerated, x.NewType(reflect.TypeFor[generatedSample](), opts...)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := catalog.Register(TypeOriginPackage, x.NewType(reflect.TypeFor[packageSample](), opts...)); err != nil {
			t.Error(err)
		}
	}()
	for i := 0; i < 30; i++ {
		resolver, err := NewResolver(catalog, PackageAuthority, nil)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 30; n++ {
				resolved, err := resolver.ResolveWithProvenance(key)
				if err != nil || resolved == nil {
					t.Errorf("resolution: %+v %v", resolved, err)
					return
				}
				descriptor, err := resolver.Descriptor(key)
				if err != nil || descriptor == nil {
					t.Errorf("descriptor: %+v %v", descriptor, err)
					return
				}
				wanted := reflect.TypeFor[generatedSample]()
				if resolved.Origin == TypeOriginPackage {
					wanted = reflect.TypeFor[packageSample]()
				} else if resolved.Origin != TypeOriginGenerated {
					t.Errorf("unexpected origin %s", resolved.Origin)
					return
				}
				if descriptor.Type != wanted {
					t.Errorf("mixed catalog snapshot: origin=%s type=%v", resolved.Origin, descriptor.Type)
					return
				}
			}
		}()
	}
	wg.Wait()
}
