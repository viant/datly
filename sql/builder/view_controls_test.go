package builder

import (
	"testing"

	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
)

func TestNonWindowControls(t *testing.T) {
	limit := 10
	offset := 5
	controls := &spec.ViewControls{
		OrderBy: "name DESC",
		Limit:   &limit,
		Offset:  &offset,
	}
	selector := &xstate.Selector{
		OrderBy: "id ASC",
		Limit:   7,
		Offset:  3,
	}

	actual := NonWindowControls(controls, selector)
	if actual == nil {
		t.Fatalf("expected non-window controls")
	}
	if actual.OrderBy != "id ASC" {
		t.Fatalf("expected selector order by preserved, got %q", actual.OrderBy)
	}
	if actual.Limit != nil {
		t.Fatalf("expected limit to be stripped, got %v", *actual.Limit)
	}
	if actual.Offset != nil {
		t.Fatalf("expected offset to be stripped, got %v", *actual.Offset)
	}
}

func TestNonWindowControls_ReturnsNilWhenOnlyPaginationIsPresent(t *testing.T) {
	limit := 10
	offset := 5
	controls := &spec.ViewControls{Limit: &limit, Offset: &offset}

	actual := NonWindowControls(controls, nil)
	if actual != nil {
		t.Fatalf("expected nil controls when only pagination was present, got %+v", actual)
	}
}

func TestMergeViewControlsWithSelector_PrefersSelector(t *testing.T) {
	limit := 10
	offset := 5
	controls := &spec.ViewControls{
		OrderBy: "name DESC",
		Limit:   &limit,
		Offset:  &offset,
	}
	selector := &xstate.Selector{
		OrderBy: "id ASC",
		Limit:   7,
		Offset:  3,
	}

	actual := MergeViewControlsWithSelector(controls, selector)
	if actual == nil {
		t.Fatalf("expected merged controls")
	}
	if actual.OrderBy != "id ASC" {
		t.Fatalf("expected selector order by, got %q", actual.OrderBy)
	}
	if actual.Limit == nil || *actual.Limit != 7 {
		t.Fatalf("expected selector limit, got %+v", actual.Limit)
	}
	if actual.Offset == nil || *actual.Offset != 3 {
		t.Fatalf("expected selector offset, got %+v", actual.Offset)
	}
}

func TestNonWindowSelector_StripsPaginationKeepsOrder(t *testing.T) {
	selector := &xstate.Selector{
		OrderBy: "name ASC",
		Limit:   10,
		Offset:  5,
		Page:    2,
	}

	actual := NonWindowSelector(selector)
	if actual == nil {
		t.Fatalf("expected cloned selector")
	}
	if actual == selector {
		t.Fatalf("expected clone, got original pointer")
	}
	if actual.OrderBy != "name ASC" {
		t.Fatalf("unexpected order: %s", actual.OrderBy)
	}
	if actual.Limit != 0 || actual.Offset != 0 || actual.Page != 0 {
		t.Fatalf("expected pagination stripped, got %+v", actual)
	}
	if selector.Limit != 10 || selector.Offset != 5 || selector.Page != 2 {
		t.Fatalf("expected original selector unchanged, got %+v", selector)
	}
}

func TestMergeViewControlsWithSelector_ComputesOffsetFromPage(t *testing.T) {
	limit := 5
	actual := MergeViewControlsWithSelector(&spec.ViewControls{Limit: &limit}, &xstate.Selector{
		Page: 3,
	})
	if actual == nil {
		t.Fatalf("expected merged controls")
	}
	if actual.Limit == nil || *actual.Limit != 5 {
		t.Fatalf("expected limit retained, got %+v", actual.Limit)
	}
	if actual.Offset == nil || *actual.Offset != 10 {
		t.Fatalf("expected page-derived offset 10, got %+v", actual.Offset)
	}
}

func TestMergeViewControlsWithSelector_PageUsesSelectorLimitWhenPresent(t *testing.T) {
	limit := 5
	actual := MergeViewControlsWithSelector(&spec.ViewControls{Limit: &limit}, &xstate.Selector{
		Limit: 2,
		Page:  4,
	})
	if actual == nil {
		t.Fatalf("expected merged controls")
	}
	if actual.Limit == nil || *actual.Limit != 2 {
		t.Fatalf("expected selector limit 2, got %+v", actual.Limit)
	}
	if actual.Offset == nil || *actual.Offset != 6 {
		t.Fatalf("expected page-derived offset 6, got %+v", actual.Offset)
	}
}

func TestMergeViewControlsWithSelector_ExplicitOffsetWinsOverPage(t *testing.T) {
	limit := 5
	actual := MergeViewControlsWithSelector(&spec.ViewControls{Limit: &limit}, &xstate.Selector{
		Page:   4,
		Offset: 1,
	})
	if actual == nil {
		t.Fatalf("expected merged controls")
	}
	if actual.Offset == nil || *actual.Offset != 1 {
		t.Fatalf("expected explicit offset 1, got %+v", actual.Offset)
	}
}
