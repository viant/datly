package session

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
)

func TestSessionQuerySelectorNumericConversions(t *testing.T) {
	ctx := context.Background()
	resource := view.NewResource(nil)
	trueValue := true
	aView := &view.View{
		Name: "audience",
		Mode: view.ModeQuery,
		Selector: func() *view.Config {
			cfg := view.QueryStateParameters.Clone()
			cfg.Limit = 25
			cfg.Constraints = &view.Constraints{
				Limit:  true,
				Offset: true,
				Page:   &trueValue,
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

	sess := New(aView)
	ns := &view.NamespaceView{View: aView}

	if err := sess.setLimitQuerySelector(float64(1), ns); err != nil {
		t.Fatalf("setLimitQuerySelector() error: %v", err)
	}
	if err := sess.setOffsetQuerySelector(float64(1), ns); err != nil {
		t.Fatalf("setOffsetQuerySelector() error: %v", err)
	}
	if err := sess.setPageQuerySelector(float64(2), ns); err != nil {
		t.Fatalf("setPageQuerySelector() error: %v", err)
	}

	selector := sess.State().Lookup(aView)
	if selector.Limit != 1 {
		t.Fatalf("expected Limit=1, got %d", selector.Limit)
	}
	if selector.Offset != 1 {
		t.Fatalf("expected Offset=1, got %d", selector.Offset)
	}
	if selector.Page != 2 {
		t.Fatalf("expected Page=2, got %d", selector.Page)
	}
}
