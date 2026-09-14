package typecatalog

import (
	"context"
	"reflect"
	"testing"
	"testing/fstest"

	x "github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	"github.com/viant/x/syntetic"
	smodel "github.com/viant/x/syntetic/model"
)

type hookCapableFixture struct{}

func (h *hookCapableFixture) OnFetch(context.Context) error { return nil }

func TestXRegistryCompiledTypeIdentity(t *testing.T) {
	type input struct {
		ID int
	}

	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(input{})))

	got := reg.Lookup("github.com/viant/datly/typecatalog.input")
	if got == nil {
		t.Fatalf("expected compiled type to be registered")
	}
	if got.Type == nil {
		t.Fatalf("expected compiled reflect type")
	}
	if !got.IsNamed() {
		t.Fatalf("expected named type")
	}
}

func TestXSyntheticShapeCanAttachToCompiledType(t *testing.T) {
	reg := x.NewRegistry()

	compiled := x.NewType(
		reflect.TypeOf(hookCapableFixture{}),
		x.WithSyntheticType(&smodel.Type{Name: "hookCapableFixture"}),
	)
	reg.Register(compiled)

	got := reg.Lookup("github.com/viant/datly/typecatalog.hookCapableFixture")
	if got == nil {
		t.Fatalf("expected compiled type with synthetic attachment")
	}
	if got.Type == nil {
		t.Fatalf("expected compiled reflect type to remain present")
	}
	if got.SynteticType == nil {
		t.Fatalf("expected synthetic attachment to remain present")
	}

	ns, err := syntetic.FromRegistry(reg)
	if err != nil {
		t.Fatalf("unexpected bridge error: %v", err)
	}
	if ns == nil {
		t.Fatalf("expected namespace")
	}
	if _, ok := ns.Types["hookCapableFixture"]; !ok {
		t.Fatalf("expected synthetic namespace entry")
	}
}

func TestXAstLoaderLoadsPackageFromFS(t *testing.T) {
	fsys := fstest.MapFS{
		"root/go.mod":     &fstest.MapFile{Data: []byte("module example.com/demo\n\n")},
		"root/p/types.go": &fstest.MapFile{Data: []byte("package p\n\ntype T struct{ ID int }\n")},
	}

	pkg, err := loaderast.LoadPackageFS(context.Background(), fsys, "root/p")
	if err != nil {
		t.Fatalf("LoadPackageFS failed: %v", err)
	}

	if pkg.Name != "p" {
		t.Fatalf("unexpected package name: %s", pkg.Name)
	}
	if pkg.PkgPath != "example.com/demo/p" {
		t.Fatalf("unexpected package path: %s", pkg.PkgPath)
	}
	if !pkg.HasType("T") {
		t.Fatalf("expected loaded type T")
	}
}

func TestXRegistryCurrentPrecedenceUsesSCN(t *testing.T) {
	type sample struct {
		Value int
	}

	reg := x.NewRegistry(x.WithRegistryScn(10))
	first := x.NewType(reflect.TypeOf(sample{}), x.WithName("Sample"), x.WithPkgPath("example.com/demo"))
	first.Scn = 1
	reg.Register(first)

	second := x.NewType(reflect.TypeOf(sample{}), x.WithName("Sample"), x.WithPkgPath("example.com/demo"))
	second.Scn = 2
	reg.Register(second)

	got := reg.Lookup("example.com/demo.Sample")
	if got == nil {
		t.Fatalf("expected registered type")
	}
	if got.Scn != 2 {
		t.Fatalf("expected highest SCN to win in current x behavior, got %d", got.Scn)
	}
}

func TestXDatlySideHookCapableAndDataOnlyCanCoexist(t *testing.T) {
	reg := x.NewRegistry()

	hookType := x.NewType(
		reflect.TypeOf(hookCapableFixture{}),
		x.WithSyntheticType(&smodel.Type{Name: "hookCapableFixture"}),
	)
	reg.Register(hookType)

	dataOnly := x.NewType(
		reflect.StructOf([]reflect.StructField{
			{Name: "ID", Type: reflect.TypeOf(0)},
		}),
		x.WithName("DataOnlyGenerated"),
		x.WithPkgPath("example.com/generated"),
		x.WithSyntheticType(&smodel.Type{Name: "DataOnlyGenerated"}),
	)
	reg.Register(dataOnly)

	hookGot := reg.Lookup("github.com/viant/datly/typecatalog.hookCapableFixture")
	dataGot := reg.Lookup("example.com/generated.DataOnlyGenerated")
	if hookGot == nil || dataGot == nil {
		t.Fatalf("expected both compiled hook-capable and data-only synthetic entries")
	}
	if hookGot.Type == nil {
		t.Fatalf("expected compiled reflect type for hook-capable entry")
	}
	if dataGot.Type == nil {
		t.Fatalf("expected synthetic reflect type carrier for data-only entry")
	}
	if hookGot.SynteticType == nil || dataGot.SynteticType == nil {
		t.Fatalf("expected synthetic attachments on both entries")
	}
}

func TestXAstLoaderLoadsMultiplePackagesFromModuleFS(t *testing.T) {
	fsys := fstest.MapFS{
		"root/go.mod":        &fstest.MapFile{Data: []byte("module example.com/demo\n\n")},
		"root/a/a.go":        &fstest.MapFile{Data: []byte("package a\n\ntype A struct{}\n")},
		"root/b/b.go":        &fstest.MapFile{Data: []byte("package b\n\ntype B struct{}\n")},
		"root/vendor/x/x.go": &fstest.MapFile{Data: []byte("package x\n\ntype X struct{}\n")},
	}

	mod, err := loaderast.LoadModuleFS(context.Background(), fsys, "root")
	if err != nil {
		t.Fatalf("LoadModuleFS failed: %v", err)
	}
	if mod.Path != "example.com/demo" {
		t.Fatalf("unexpected module path: %s", mod.Path)
	}
	if !mod.HasPackage("example.com/demo/a") || !mod.HasPackage("example.com/demo/b") {
		t.Fatalf("expected both packages to load")
	}
	if mod.HasPackage("example.com/demo/vendor/x") {
		t.Fatalf("expected vendor package to be skipped")
	}
}
