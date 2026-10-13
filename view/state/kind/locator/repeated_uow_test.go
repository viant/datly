package locator

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/view/state"
)

func TestRepeatedExtendsBindingOrderByAuthoredIndex(t *testing.T) {
	items := state.Parameters{{Name: "first"}, {Name: "second"}, {Name: "third"}}
	parameter := &state.Parameter{Repeated: items}
	orders := map[string]string{}
	var mu sync.Mutex
	locator := &Repeated{ParameterLookup: func(ctx context.Context, item *state.Parameter) (interface{}, bool, error) {
		mu.Lock()
		orders[item.Name] = uow.BindingOrder(ctx)
		mu.Unlock()
		return item.Name, true, nil
	}}
	ctx := uow.WithBindingOrder(context.Background(), "parent")
	entries, count := locator.getRepeatedItems(ctx, parameter)
	if count != 3 || len(entries) != 3 {
		t.Fatalf("count=%d entries=%d", count, len(entries))
	}
	want := map[string]string{
		"first":  "parent/00000000000000000001",
		"second": "parent/00000000000000000002",
		"third":  "parent/00000000000000000003",
	}
	if !reflect.DeepEqual(orders, want) {
		t.Fatalf("orders=%v want=%v", orders, want)
	}
}
