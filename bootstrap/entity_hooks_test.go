package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestGoShapeEntityHooksGenerationMetadata(t *testing.T) {
	type row struct{ ID int }
	type input struct {
		Current []*row `parameter:"Current,kind=view,in=Current" view:"Current,table=rows,entityHooks=example.com/private/app.Hooks"`
	}
	component, err := (ContractResolver{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/model", Name: "Rows"}}, InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Views) != 1 || component.Views[0].EntityHooks != "example.com/private/app.Hooks" {
		t.Fatalf("views=%+v", component.Views)
	}
	if _, found := reflect.TypeOf(data.View{}).FieldByName("EntityHooks"); found {
		t.Fatal("generation hook metadata leaked into runtime data.View")
	}
}
