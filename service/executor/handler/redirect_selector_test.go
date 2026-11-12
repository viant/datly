package handler

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	_ "github.com/viant/datly/repository/locator/component"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	hhttp "github.com/viant/xdatly/handler/http"
	hstate "github.com/viant/xdatly/handler/state"
)

func TestExecutorRedirect_ForwardsQuerySelectors(t *testing.T) {
	ctx := context.Background()
	resource := view.NewResource(nil)

	type childOutput struct {
		Bids  int     `json:"bids"`
		Spend float64 `json:"spend"`
		Other int     `json:"other"`
	}

	childView := &view.View{
		Name:      "ad_cube",
		Mode:      view.ModeQuery,
		Connector: view.NewConnector("test", "sqlite3", ":memory:"),
		Selector: func() *view.Config {
			cfg := view.QueryStateParameters.Clone()
			cfg.Constraints = &view.Constraints{
				Limit:      true,
				Projection: true,
			}
			return cfg
		}(),
		Columns: view.Columns{
			{Name: "Bids", DataType: "int"},
			{Name: "Spend", DataType: "float"},
			{Name: "Other", DataType: "int"},
		},
	}
	childView.SetResource(resource)
	childView.Template = &view.Template{Schema: vstate.NewSchema(reflect.TypeOf(childOutput{}))}
	if err := childView.Template.Init(ctx, resource, childView); err != nil {
		t.Fatalf("failed to init child template: %v", err)
	}
	if err := childView.Selector.Init(ctx, resource, childView); err != nil {
		t.Fatalf("failed to init child selector: %v", err)
	}
	if err := childView.Init(ctx, resource); err != nil {
		t.Fatalf("failed to init child view: %v", err)
	}

	inputType, err := vstate.NewType(
		vstate.WithSchema(vstate.NewSchema(reflect.TypeOf(struct{}{}))),
		vstate.WithResource(childView.Resource()),
	)
	if err != nil {
		t.Fatalf("failed to create input type: %v", err)
	}
	outputType, err := vstate.NewType(
		vstate.WithSchema(vstate.NewSchema(reflect.TypeOf(childOutput{}))),
		vstate.WithResource(childView.Resource()),
	)
	if err != nil {
		t.Fatalf("failed to create output type: %v", err)
	}

	registry := repository.NewRegistry("", nil, func(*repository.Registry, *auth.Service) contract.Dispatcher {
		return testDispatcher{}
	})
	registry.Register(&repository.Component{
		Path: contract.Path{Method: http.MethodGet, URI: "/v1/api/child"},
		View: childView,
		Contract: contract.Contract{
			Input:  contract.Input{Type: *inputType, Body: *inputType},
			Output: contract.Output{Type: *outputType},
		},
	})

	parentView := &view.View{Name: "parent"}
	parentView.SetResource(resource)
	parentSession := session.New(parentView, session.WithRegistry(registry))
	executor := NewExecutor(parentView, parentSession)

	child, err := executor.redirect(ctx, &hhttp.Route{Method: http.MethodGet, URL: "/v1/api/child"},
		hstate.WithQuerySelector(&hstate.NamedQuerySelector{
			Name: "ad_cube",
			QuerySelector: hstate.QuerySelector{
				Fields: []string{"Bids", "Spend"},
				Limit:  10,
			},
		}),
	)
	if err != nil {
		t.Fatalf("redirect() error: %v", err)
	}

	probe := &redirectSelectorProbe{}
	bindCtx := context.WithValue(ctx, redirectSelectorProbeViewKey{}, childView)
	if err = child.Stater().Bind(bindCtx, probe); err != nil {
		t.Fatalf("child Bind() error: %v", err)
	}
	if probe.limit != 10 {
		t.Fatalf("expected forwarded selector limit=10, got %d", probe.limit)
	}
	if got, want := probe.fields, []string{"Bids", "Spend"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected forwarded selector fields=%v, got %v", want, got)
	}
}

type redirectSelectorProbe struct {
	fields []string
	limit  int
}

type redirectSelectorProbeViewKey struct{}

func (p *redirectSelectorProbe) Init(ctx context.Context) error {
	aSession := session.Context(ctx)
	if aSession == nil {
		return fmt.Errorf("session was not attached to context")
	}
	aView, _ := ctx.Value(redirectSelectorProbeViewKey{}).(*view.View)
	if aView == nil {
		return fmt.Errorf("probe view was not attached to context")
	}
	if err := aSession.SetViewState(ctx, aView); err != nil {
		return err
	}
	selector := aSession.State().Lookup(aView)
	if selector == nil {
		return fmt.Errorf("selector state was not initialized")
	}
	p.fields = append([]string(nil), selector.Fields...)
	p.limit = selector.Limit
	return nil
}

type testDispatcher struct{}

func (testDispatcher) Dispatch(context.Context, *contract.Path, ...contract.Option) (interface{}, error) {
	return nil, nil
}
