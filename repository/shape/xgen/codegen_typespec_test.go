package xgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	shapeload "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
)

func TestComponentCodegen_TypeSpecs_InputOutputAndDest(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/vendors/",
		RootView: "vendor",
		Directives: &dqlshape.Directives{
			Dest:       "all.go",
			RouterDest: "vendor_router.go",
		},
		TypeSpecs: map[string]*shapeload.TypeSpec{
			"input":       {Key: "input", Role: shapeload.TypeRoleInput, TypeName: "VendorReq"},
			"output":      {Key: "output", Role: shapeload.TypeRoleOutput, TypeName: "VendorResp", Dest: "vendor_output.go"},
			"view:vendor": {Key: "view:vendor", Role: shapeload.TypeRoleView, Alias: "vendor", TypeName: "Vendor"},
		},
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name:      "vendor",
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Name: "dev"}}},
				Columns: []*view.Column{
					{Name: "ID", DataType: "int"},
				},
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "vendor",
		PackagePath: "github.com/acme/project/pkg/dev/vendor",
	}
	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}
	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if filepath.Base(result.FilePath) != "vendor_output.go" {
		t.Fatalf("expected destination override vendor_output.go, got %s", filepath.Base(result.FilePath))
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	source := string(data)
	expectContainsTypeSpec(t, source, "type VendorReq struct")
	expectContainsTypeSpec(t, source, "type VendorResp struct")
	expectContainsTypeSpec(t, source, "Data []*Vendor")
	expectContainsTypeSpec(t, source, "reflect.TypeOf(VendorReq{})")
	expectContainsTypeSpec(t, source, "reflect.TypeOf(VendorResp{})")
	routerData, err := os.ReadFile(filepath.Join(packageDir, "vendor_router.go"))
	if err != nil {
		t.Fatalf("read generated router file: %v", err)
	}
	routerSource := string(routerData)
	expectContainsTypeSpec(t, routerSource, "type VendorRouter struct")
	expectContainsTypeSpec(t, routerSource, "Vendor xdatly.Component[VendorReq, VendorResp]")
}

func expectContainsTypeSpec(t *testing.T, source string, fragment string) {
	t.Helper()
	if !strings.Contains(source, fragment) {
		t.Fatalf("expected generated source to contain %q\nsource:\n%s", fragment, source)
	}
}
