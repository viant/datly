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

func TestComponentCodegen_GeneratesDefineComponentParitySnippet(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "wrapper")
	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/dev/vendors/{vendorID}",
		RootView: "Wrapper",
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name:      "Wrapper",
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Name: "dev"}}},
				Columns: []*view.Column{
					{Name: "ID", DataType: "int"},
				},
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "wrapper",
		PackagePath: "github.com/acme/project/pkg/dev/wrapper",
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
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)

	expectContains(t, generated, `var WrapperPathURI = "/v1/api/dev/vendors/{vendorID}"`)
	expectContains(t, generated, `type WrapperRouter struct {`)
	expectContains(t, generated, `Wrapper xdatly.Component[WrapperInput, WrapperOutput] `+"`"+`component:",path=/v1/api/dev/vendors/{vendorID},method=GET,connector=dev,view=WrapperView"`+"`")
	expectContains(t, generated, `func DefineWrapperComponent(ctx context.Context, srv *datly.Service) error {`)
	expectContains(t, generated, `contract.NewPath("GET", WrapperPathURI)`)
	expectContains(t, generated, `repository.WithResource(srv.Resource())`)
	expectContains(t, generated, `repository.WithContract(`)
	expectContains(t, generated, `reflect.TypeOf(WrapperInput{})`)
	expectContains(t, generated, `reflect.TypeOf(WrapperOutput{}), &WrapperFS, view.WithConnectorRef("dev"))`)
}

func expectContains(t *testing.T, actual string, fragment string) {
	t.Helper()
	if !strings.Contains(actual, fragment) {
		t.Fatalf("expected generated source to contain %q\nsource:\n%s", fragment, actual)
	}
}

func TestComponentCodegen_GeneratesSeparateRouterFile(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/dev/vendors",
		RootView: "Vendor",
		Directives: &dqlshape.Directives{
			RouterDest: "vendor_router.go",
		},
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name:      "Vendor",
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Name: "dev"}}},
				Columns:   []*view.Column{{Name: "ID", DataType: "int"}},
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
	if filepath.Base(result.RouterFilePath) != "vendor_router.go" {
		t.Fatalf("expected router file vendor_router.go, got %s", result.RouterFilePath)
	}
	if len(result.GeneratedFiles) != 2 {
		t.Fatalf("expected 2 generated files, got %v", result.GeneratedFiles)
	}
	routerData, err := os.ReadFile(result.RouterFilePath)
	if err != nil {
		t.Fatalf("read router file: %v", err)
	}
	routerSource := string(routerData)
	expectContains(t, routerSource, `type VendorRouter struct {`)
	expectContains(t, routerSource, `Vendor xdatly.Component[VendorInput, VendorOutput] `+"`"+`component:",path=/v1/api/dev/vendors,method=GET,connector=dev,view=VendorView"`+"`")
	outputData, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read primary file: %v", err)
	}
	if strings.Contains(string(outputData), "type VendorRouter struct") {
		t.Fatalf("expected router declaration to be split out of primary output file:\n%s", string(outputData))
	}
}
