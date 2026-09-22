package session

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
)

func TestSessionBind_QuerySelectorErrorDoesNotPanicWithoutCustomParameters(t *testing.T) {
	ctx := context.Background()
	resource := view.NewResource(nil)
	aView := &view.View{
		Name: "v",
		Mode: view.ModeQuery,
		Selector: &view.Config{
			Constraints: &view.Constraints{},
		},
	}
	aView.SetResource(resource)
	aView.Template = &view.Template{Schema: vstate.NewSchema(reflect.TypeOf(struct{ Dummy int }{}))}
	if err := aView.Template.Init(ctx, resource, aView); err != nil {
		t.Fatalf("failed to init template: %v", err)
	}
	if err := aView.Selector.Init(ctx, resource, aView); err != nil {
		t.Fatalf("failed to init selector: %v", err)
	}

	component := &repository.Component{View: aView}
	outputType, err := vstate.NewType(
		vstate.WithSchema(vstate.NewSchema(reflect.TypeOf(struct{ X int }{}))),
		vstate.WithResource(aView.Resource()),
	)
	if err != nil {
		t.Fatalf("failed to build component output type: %v", err)
	}
	component.Output.Type = *outputType

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1/?_orderby=id", nil)
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}

	sess := New(aView, WithComponent(component), WithLocatorOptions(locator.WithRequest(req)))
	err = sess.SetViewState(ctx, aView)
	if err == nil {
		t.Fatal("expected query selector error")
	}
}
