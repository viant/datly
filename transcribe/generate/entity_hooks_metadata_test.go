package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/compile"
	xshape "github.com/viant/x/shape"
)

func TestGeneratedViewTagPreservesEntityHooks(t *testing.T) {
	for _, reference := range []string{"app.Hooks", "example.com/private/app.Hooks"} {
		t.Run(reference, func(t *testing.T) {
			encoded, err := appendViewTags("", &spec.View{Name: "Rows", EntityHooks: reference, Source: &spec.ViewSource{Table: "rows"}})
			if err != nil {
				t.Fatal(err)
			}
			view, err := tag.ParseView(reflect.StructTag(encoded).Get("view"))
			if err != nil || view.EntityHooks != reference {
				t.Fatalf("tag=%q view=%+v err=%v", encoded, view, err)
			}
		})
	}
}

func TestDQLGeneratedTagBootstrapEntityHooksRoundTrip(t *testing.T) {
	for _, reference := range []string{"app.Hooks", "example.com/private/app.Hooks"} {
		t.Run(reference, func(t *testing.T) {
			view, err := compile.NewReader().Compile(compile.ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{}}, SQL: "SELECT r.id, entity_hooks(r,'" + reference + "') FROM rows r"})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := appendViewTags(`parameter:"Rows,kind=view,in=Rows"`, view)
			if err != nil {
				t.Fatal(err)
			}
			input, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Rows", Type: reflect.TypeOf([]struct{ ID int }{}), Tag: reflect.StructTag(encoded)}})
			if err != nil {
				t.Fatal(err)
			}
			component, err := (&bootstrap.RouteSource{PackagePath: "example.com/project", FieldName: "Contract", Tag: tag.Component{Name: "Rows", Path: "/rows", Method: "GET"}}).Resolve(input, reflect.TypeOf(struct{}{}))
			if err != nil {
				t.Fatal(err)
			}
			if len(component.Views) != 1 || component.Views[0].EntityHooks != reference {
				t.Fatalf("round trip views=%+v", component.Views)
			}
		})
	}
}
