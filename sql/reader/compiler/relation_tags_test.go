package compiler

import (
	"testing"

	"github.com/viant/datly/data"
	dtag "github.com/viant/datly/tag"
)

func TestApplyFieldViewTagPreservesCanonicalViewOptions(t *testing.T) {
	options, err := dtag.ParseView("children,uri=queries/children.sql,table=child,cache=child-cache,cacheWarmup=startup,orderBy=id DESC,limit=10,offset=2,selectorProjection=true,selectorFilterable={ID}")
	if err != nil {
		t.Fatalf("ParseView() error = %v", err)
	}
	view := &data.View{}
	applyFieldViewTag(view, options)
	if view.Spec.Source == nil || view.Spec.Source.URI != "queries/children.sql" || view.Spec.Source.Table != "child" || view.Spec.Source.Controls == nil || view.Spec.Source.Controls.Limit == nil ||
		*view.Spec.Source.Controls.Limit != 10 || view.Spec.Source.Controls.Offset == nil || *view.Spec.Source.Controls.Offset != 2 ||
		view.Spec.Source.Controls.OrderBy != "id DESC" || view.Spec.Source.Bindings == nil || view.Spec.Source.Bindings.CacheWarmup != "startup" ||
		view.Cache == nil || view.Cache.Name != "child-cache" || view.Spec.Selector == nil ||
		!view.Spec.Selector.AllowFields || len(view.Spec.Selector.Filterable) != 1 || view.Spec.Selector.Filterable[0] != "ID" {
		t.Fatalf("view = %+v", view)
	}
}
