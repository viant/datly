package session

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	hstate "github.com/viant/xdatly/handler/state"
)

func TestSessionBind_QuerySelectorOverride_PageComputesOffset(t *testing.T) {
	ctx := context.Background()

	resource := view.NewResource(nil)
	trueValue := true
	aView := &view.View{
		Name: "v",
		Mode: view.ModeQuery,
		Selector: func() *view.Config {
			cfg := view.QueryStateParameters.Clone()
			cfg.Limit = 5
			cfg.Constraints = &view.Constraints{
				Criteria:   true,
				OrderBy:    true,
				Limit:      true,
				Offset:     true,
				Projection: true,
				Page:       &trueValue,
			}
			return cfg
		}(),
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

	sess := New(aView, WithComponent(component))
	var dest struct{}

	// request supplies different selector values; injected selector should take precedence
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1/?_page=1&_limit=1", nil)
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}

	err = sess.Bind(ctx, &dest, hstate.WithQuerySelector(&hstate.NamedQuerySelector{
		Name: "v",
		QuerySelector: hstate.QuerySelector{
			Page: 2,
		},
	}), hstate.WithHttpRequest(req))
	if err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	if err := sess.SetViewState(ctx, aView); err != nil {
		t.Fatalf("SetViewState() error: %v", err)
	}

	selector := sess.State().Lookup(aView)
	if selector.Page != 2 {
		t.Fatalf("expected Page=2, got %d", selector.Page)
	}
	if selector.Limit != 5 {
		t.Fatalf("expected Limit=5, got %d", selector.Limit)
	}
	if selector.Offset != 5 {
		t.Fatalf("expected Offset=5, got %d", selector.Offset)
	}
}
