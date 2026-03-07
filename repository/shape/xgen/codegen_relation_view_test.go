package xgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	shapeload "github.com/viant/datly/repository/shape/load"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestComponentCodegen_UsesMaterializedViewTypeForRelationHolders(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "details")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/vendors/{vendorID}",
		RootView: "vendor",
		Output: []*shapeplan.State{
			{Parameter: state.Parameter{Name: "Data", In: state.NewOutputLocation("view"), Schema: &state.Schema{Cardinality: state.Many}}},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name:     "vendor",
			Table:    "VENDOR",
			Template: &view.Template{SourceURL: "wrapper/vendor.sql"},
			Schema:   &state.Schema{Name: "VendorView", DataType: "*VendorView", Cardinality: state.Many},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
			},
			With: []*view.Relation{
				{
					Holder:      "Products",
					Cardinality: state.Many,
					On:          view.Links{&view.Link{Field: "Id", Column: "ID"}},
					Of: &view.ReferenceView{
						View: view.View{
							Name:     "products",
							Table:    "PRODUCT",
							Template: &view.Template{SourceURL: "wrapper/products.sql"},
							Schema:   &state.Schema{Name: "ProductsView", DataType: "*ProductsView", Cardinality: state.Many},
						},
						On: view.Links{&view.Link{Field: "VendorId", Column: "VENDOR_ID"}},
					},
				},
			},
		},
		&view.View{
			Name:     "products",
			Table:    "PRODUCT",
			Template: &view.Template{SourceURL: "wrapper/products.sql"},
			Schema:   &state.Schema{Name: "ProductsView", DataType: "*ProductsView", Cardinality: state.Many},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "VENDOR_ID", DataType: "*int", Tag: `internal:"true"`},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "details",
		PackagePath: "github.com/acme/project/shape/dev/vendor/details",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    false,
		WithContract: false,
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
	if !strings.Contains(generated, "Products []*ProductsView `view:\",table=PRODUCT\" on:\"Id:ID=VendorId:VENDOR_ID\" sql:\"uri=wrapper/products.sql\"`") {
		t.Fatalf("expected generated VendorView to use named relation holder field, got:\n%s", generated)
	}
	if strings.Contains(generated, "*struct {") {
		t.Fatalf("expected no anonymous relation structs, got:\n%s", generated)
	}
	if strings.Contains(generated, "table=(SELECT") {
		t.Fatalf("expected no raw subquery text in relation view tag, got:\n%s", generated)
	}
}
