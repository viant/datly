package typectx

import "testing"

func TestNormalize_FillsPackageFields(t *testing.T) {
	ctx := Normalize(&Context{
		PackageDir:  "pkg/platform/taxonomy",
		PackagePath: "github.vianttech.com/viant/platform/pkg/platform/taxonomy",
	})
	if ctx == nil {
		t.Fatalf("expected normalized context")
	}
	if ctx.PackageName != "taxonomy" {
		t.Fatalf("expected package name taxonomy, got %q", ctx.PackageName)
	}
	if ctx.DefaultPackage != "github.vianttech.com/viant/platform/pkg/platform/taxonomy" {
		t.Fatalf("expected default package from package path, got %q", ctx.DefaultPackage)
	}
}

func TestValidate_DetectsInvalidPackageName(t *testing.T) {
	issues := Validate(&Context{
		PackageName: "platform/taxonomy",
	})
	if len(issues) == 0 {
		t.Fatalf("expected validation issue")
	}
	if issues[0].Field != "PackageName" {
		t.Fatalf("expected PackageName issue, got %q", issues[0].Field)
	}
}
