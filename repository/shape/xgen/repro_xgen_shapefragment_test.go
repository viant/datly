package xgen

import (
	shapeload "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReproShapeFragment(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "details")
	component := &shapeload.Component{Method: "GET", URI: "/v1/api/shape/dev/vendors/{vendorID}", RootView: "vendor", Output: []*plan.State{{Parameter: state.Parameter{Name: "Data", In: state.NewOutputLocation("view"), Schema: &state.Schema{Cardinality: state.Many}}}}}
	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{Name: "vendor", Schema: &state.Schema{Name: "VendorView", DataType: "*VendorView", Cardinality: state.Many}})
	resource.Views[0].Schema.SetType(reflect.TypeOf([]struct {
		ID       int
		Products []*struct{ ID int } `view:",table=PRODUCT" json:",omitempty" sqlx:"-"`
	}{}))
	ctx := &typectx.Context{PackageDir: packageDir, PackageName: "details", PackagePath: "github.com/acme/project/shape/dev/vendor/details"}
	codegen := &ComponentCodegen{Component: component, Resource: resource, TypeContext: ctx, ProjectDir: projectDir, WithEmbed: false, WithContract: false}
	frag, err := codegen.generateShapeFragment(projectDir, packageDir, "details", ctx.PackagePath)
	if err != nil {
		t.Fatalf("generateShapeFragment err: %v", err)
	}
	if frag == nil {
		t.Fatalf("nil fragment")
	}
	t.Logf("types=%v", frag.Types)
	t.Logf("decls=%s", frag.TypeDecls)
}
